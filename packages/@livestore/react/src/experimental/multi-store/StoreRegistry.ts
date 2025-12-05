import { OtelLiveDummy, UnknownError } from '@livestore/common'
import type { LiveStoreSchema } from '@livestore/common/schema'
import { createStore, type Store, type Unsubscribe } from '@livestore/livestore'
import { shouldNeverHappen } from '@livestore/utils'
import {
  Cause,
  Data,
  Effect,
  Exit,
  Fiber,
  Layer,
  ManagedRuntime,
  Option,
  type OtelTracer,
  RcMap,
  Runtime,
  type Scope,
} from '@livestore/utils/effect'
import type { CachedStoreOptions, StoreId } from './types.ts'

/**
 * Default time to keep unused stores in cache.
 *
 * - Browser: 60 seconds (60,000ms)
 * - SSR: Infinity (disables disposal to avoid disposing stores before server render completes)
 *
 * @internal Exported primarily for testing purposes.
 */
export const DEFAULT_UNUSED_CACHE_TIME = typeof window === 'undefined' ? Number.POSITIVE_INFINITY : 60_000

type DefaultStoreOptions = Partial<
  Pick<
    CachedStoreOptions<any>,
    'batchUpdates' | 'disableDevtools' | 'confirmUnsavedChanges' | 'syncPayload' | 'debug' | 'otelOptions'
  >
> & {
  /**
   * The time in milliseconds that unused stores remain in memory.
   * When a store becomes unused (no subscribers), it will be disposed
   * after this duration.
   *
   * Stores transition to the unused state as soon as they have no
   * subscriptions registered, so when all components which use that
   * store have unmounted.
   *
   * @remarks
   * - If set to `Infinity`, will disable disposal
   * - The maximum allowed time is about {@link https://developer.mozilla.org/en-US/docs/Web/API/Window/setTimeout#maximum_delay_value | 24 days}
   *
   * @defaultValue `60_000` (60 seconds) or `Infinity` during SSR to avoid
   * disposing stores before server render completes.
   */
  unusedCacheTime?: number
  // todo jsdoc
  runtime?: Runtime.Runtime<Scope.Scope | OtelTracer.OtelTracer>
}

/**
 * Store Registry coordinating store loading, caching, and subscription
 *
 * @public
 */
export class StoreRegistry {
  /** RcMap tracks store lifetimes keyed by canonical options. */
  #rcMap: RcMap.RcMap<CachedStoreOptions<any>, Store<any>, UnknownError>
  /** Effect runtime to run with current scope and otel tracer. If the scope is closed, all stores will be shut down automatically. */
  #runtime: Runtime.Runtime<Scope.Scope | OtelTracer.OtelTracer>
  /**
   * Settled store cache so already-loaded stores return synchronously.
   * Required to avoid spinning new fibers for repeat callers and to keep
   * identity stable after the first load completes. Combined with the promise
   * cache below, this ensures: (1) one in-flight load per key, (2) subsequent
   * calls after settle return synchronously.
   */
  #storeCache = new Map<CachedStoreOptions<any>, Store<any>>()
  /**
   * In-flight promise cache to keep identity stable during concurrent loads.
   * Without this, concurrent callers would fork separate load fibers and could
   * hang waiting on different adapter bootstraps.
   */
  #promiseCache = new Map<CachedStoreOptions<any>, Promise<Store<any>>>()
  /** Canonicalized options per storeId to provide stable RcMap keys. Used for improved error messages when user passes different options to the same store. */
  #optionsByStoreId = new Map<StoreId, CachedStoreOptions<any>>()

  constructor({ defaultOptions }: { defaultOptions?: DefaultStoreOptions } = {}) {
    this.#runtime =
      defaultOptions?.runtime ??
      ManagedRuntime.make(Layer.mergeAll(Layer.scope, OtelLiveDummy)).runtimeEffect.pipe(Effect.runSync)

    /** We're overriding the idleTimeToLive value with the most recent value passed to the registry. */
    // TODO this is actually not yet working since Effect doesn't yet support dynamic runtime values for RcMap
    // https://github.com/Effect-TS/effect/pull/5859
    const idleTimeToLiveRef = { current: defaultOptions?.unusedCacheTime ?? DEFAULT_UNUSED_CACHE_TIME }

    this.#rcMap = RcMap.make({
      lookup: (options: CachedStoreOptions) =>
        Effect.gen(this, function* () {
          if (options.unusedCacheTime !== undefined) {
            idleTimeToLiveRef.current = options.unusedCacheTime
          }

          const store = yield* createStore(options).pipe(
            Effect.tap((createdStore) => Effect.sync(() => this.#storeCache.set(options, createdStore))),
            Effect.acquireRelease(() =>
              Effect.gen(this, function* () {
                this.#optionsByStoreId.delete(options.storeId)
                this.#storeCache.delete(options)
                this.#promiseCache.delete(options)
              }),
            ),
            Effect.catchAllDefect((cause) => UnknownError.make({ cause })),
          )
          return store
        }).pipe(Effect.withSpan(`StoreRegistry.lookup:${options.storeId}`)),
      idleTimeToLive: idleTimeToLiveRef.current,
    }).pipe(Effect.provide(this.#runtime), Effect.runSync)
  }

  /**
   * Effectful entrypoint to get or load a store via RcMap (one instance per storeId).
   * Keeps lifecycle tied to the current runtime/scope.
   */
  getOrLoad = <TSchema extends LiveStoreSchema>(
    options: CachedStoreOptions<TSchema>,
  ): Effect.Effect<Store<TSchema>, UnknownError, Scope.Scope> =>
    Effect.gen(this, function* () {
      const cacheKey = this.#getCacheKey(options)
      const storeEntry = yield* RcMap.get(this.#rcMap, cacheKey)

      return storeEntry as unknown as Store<TSchema>
    }).pipe(Effect.withSpan(`StoreRegistry.getOrLoad:${options.storeId}`))

  /**
   * Get or load a store (React-friendly): returns a settled store synchronously or the in-flight Promise.
   *
   * @typeParam TSchema - The schema of the store to load
   * @returns The loaded store synchronously if already settled; otherwise the in-flight Promise
   * @throws unknown loading error
   *
   * @remarks
   * - Returns the store instance directly (synchronous) when already settled (fulfilled or rejected)
   * - Returns a stable Promise reference when loading is in progress or needs to be initiated
   * - Applies default options from registry config, with call-site options taking precedence
   */
  getOrLoadStore = <TSchema extends LiveStoreSchema>(
    options: CachedStoreOptions<TSchema>,
  ): Store<TSchema> | Promise<Store<TSchema>> => {
    const cacheKey = this.#getCacheKey(options)
    const cachedStore = this.#storeCache.get(cacheKey) as Store<TSchema> | undefined
    if (cachedStore !== undefined) {
      return cachedStore
    }

    const cachedPromise = this.#promiseCache.get(cacheKey) as Promise<Store<TSchema>> | undefined
    if (cachedPromise !== undefined) {
      return cachedPromise
    }

    const effect = this.getOrLoad<TSchema>(options).pipe(Effect.scoped)
    /**
     * Fork on the registry runtime so the fiber survives the calling fiber and
     * can be polled for a synchronous fast-path. This avoids the
     * AsyncFiberException path and keeps us in control of the fiber handle.
     */
    const fiber = Runtime.runFork(this.#runtime)(effect)
    const polledExit = Fiber.poll(fiber).pipe(Runtime.runSync(this.#runtime))

    if (Option.isSome(polledExit)) {
      const exit = polledExit.value

      if (Exit.isSuccess(exit)) {
        const store = exit.value as Store<TSchema>
        this.#storeCache.set(cacheKey, store)
        return store
      }

      throw Cause.squash(exit.cause)
    }

    const promise = Fiber.join(fiber)
      .pipe(
        Effect.tap((store) => Effect.sync(() => this.#storeCache.set(cacheKey, store as Store<TSchema>))),
        Effect.runPromise,
      )
      .catch((error) => {
        this.#promiseCache.delete(cacheKey)
        throw error
      })

    this.#promiseCache.set(cacheKey, promise)

    promise.then(
      (store) => {
        this.#promiseCache.delete(cacheKey)
        this.#storeCache.set(cacheKey, store)
      },
      () => {
        this.#promiseCache.delete(cacheKey)
      },
    )

    return promise
  }

  /** Retain the store while mounted; caller releases via the returned unsubscribe. */
  retain = (options: CachedStoreOptions<any>): Unsubscribe => {
    const unsubscribe = Effect.gen(this, function* () {
      const cacheKey = this.#getCacheKey(options)
      yield* RcMap.get(this.#rcMap, cacheKey)
      yield* Effect.never
    }).pipe(Effect.provide(this.#runtime), Effect.runCallback)

    return () => unsubscribe()
  }

  /** Fire-and-forget warmup; swallows errors but keeps them cached for later reuse. */
  preload = (options: CachedStoreOptions<any>): Promise<void> =>
    Promise.resolve(this.getOrLoadStore(options)).then(
      () => undefined,
      () => undefined,
    )

  /**
   * Canonicalize caller options for RcMap keying and memoized promise cache.
   * Only used for improved error messages when user passes different options to the same store.
   */
  #getCacheKey = <TSchema extends LiveStoreSchema>(
    options: CachedStoreOptions<TSchema>,
  ): CachedStoreOptions<TSchema> => {
    /**
     * Data.struct keeps a stable structural instance per storeId so RcMap can treat
     * logically identical options as the same cache key even when callers pass fresh
     * object literals.
     */
    const newValue = Data.struct(options) as CachedStoreOptions<TSchema>
    const existing = this.#optionsByStoreId.get(options.storeId) as CachedStoreOptions<TSchema> | undefined

    if (existing) {
      const ignoreKeys = ['boot']
      const warningKeys = ['signal']
      const errorKeys = ['schema', 'adapter']

      for (const key of Object.keys(existing) as (keyof CachedStoreOptions<TSchema>)[]) {
        if (ignoreKeys.includes(key)) continue
        if (warningKeys.includes(key) && existing[key] !== newValue[key]) {
          console.warn(`StoreRegistry received changed options for storeId ${options.storeId}`, {
            existingValue: existing[key],
            newValue: newValue[key],
          })
          continue
        }
        if (errorKeys.includes(key) && existing[key] !== newValue[key]) {
          return shouldNeverHappen(`StoreRegistry received changed options for storeId ${options.storeId}`, {
            existingValue: existing[key],
            newValue: newValue[key],
          })
        }
      }

      return existing
    }

    this.#optionsByStoreId.set(options.storeId, newValue)
    return newValue
  }
}
