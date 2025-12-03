import type { UnknownError } from '@livestore/common'
import { OtelLiveDummy } from '@livestore/common'
import type { LiveStoreSchema } from '@livestore/common/schema'
import { createStore, type Store, type Unsubscribe } from '@livestore/livestore'
import { shouldNeverHappen } from '@livestore/utils'
import {
  Data,
  Effect,
  Equal,
  Layer,
  ManagedRuntime,
  type OtelTracer,
  RcMap,
  type Runtime,
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
  /** Canonicalized options per storeId to provide stable RcMap keys. */
  #optionsByStoreId = new Map<StoreId, CachedStoreOptions<any>>()
  /** Handles promise/result caching per storeId to keep RcMap focused on lifecycle. */
  #promiseCache = new StorePromiseCache()

  constructor({ defaultOptions }: { defaultOptions?: DefaultStoreOptions } = {}) {
    this.#runtime =
      defaultOptions?.runtime ??
      ManagedRuntime.make(Layer.mergeAll(Layer.scope, OtelLiveDummy)).runtimeEffect.pipe(Effect.runSync)

    /** We're overriding the idleTimeToLive value with the most recent value passed to the registry. */
    const idleTimeToLiveRef = { current: defaultOptions?.unusedCacheTime ?? DEFAULT_UNUSED_CACHE_TIME }

    this.#rcMap = RcMap.make({
      lookup: (options: CachedStoreOptions) =>
        Effect.gen(this, function* () {
          if (options.unusedCacheTime !== undefined) {
            idleTimeToLiveRef.current = options.unusedCacheTime
          }

          const store = yield* createStore(options).pipe(
            Effect.acquireRelease(() =>
              Effect.gen(this, function* () {
                this.#promiseCache.delete(options.storeId)
                this.#optionsByStoreId.delete(options.storeId)
              }),
            ),
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
    const cachedResult = this.#promiseCache.getResult<TSchema>(cacheKey.storeId)

    if (cachedResult?.status === 'fulfilled') return cachedResult.value
    if (cachedResult?.status === 'rejected') throw cachedResult.reason

    return this.#promiseCache.getOrLoad(cacheKey, (opts) =>
      this.getOrLoad<TSchema>(opts).pipe(Effect.provide(this.#runtime), Effect.runPromise),
    )
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

  /** Canonicalize caller options for RcMap keying and memoized promise cache. */
  #getCacheKey = <TSchema extends LiveStoreSchema>(
    options: CachedStoreOptions<TSchema>,
  ): CachedStoreOptions<TSchema> => {
    /**
     * Data.struct keeps a stable structural instance per storeId so RcMap can treat
     * logically identical options as the same cache key even when callers pass fresh
     * object literals.
     */
    const canonical = Data.struct(options) as CachedStoreOptions<TSchema>
    const existing = this.#optionsByStoreId.get(options.storeId) as CachedStoreOptions<TSchema> | undefined

    if (existing) {
      if (!Equal.equals(existing, canonical)) {
        return shouldNeverHappen(`StoreRegistry received changed options for storeId ${options.storeId}`)
      }
      return existing
    }

    this.#optionsByStoreId.set(options.storeId, canonical)
    return canonical
  }
}

type StoreResult<TSchema extends LiveStoreSchema> =
  | { status: 'fulfilled'; value: Store<TSchema> }
  | { status: 'rejected'; reason: unknown }

/**
 * Lightweight cache to keep one in-flight/settled promise per storeId and expose
 * settled results for React suspense reuse without entangling StoreRegistry with
 * promise status tagging.
 */
class StorePromiseCache {
  /** In-flight or settled promises per storeId. */
  #storePromises = new Map<StoreId, Promise<Store<any>>>()
  /** Tracks settled results so React can reuse status synchronously in useStore. */
  #storeResults = new Map<StoreId, StoreResult<any>>()

  getOrLoad = <TSchema extends LiveStoreSchema>(
    options: CachedStoreOptions<TSchema>,
    loader: (opts: CachedStoreOptions<TSchema>) => Promise<Store<TSchema>>,
  ): Promise<Store<TSchema>> => {
    /** Only one promise per storeId; reuse across callers to avoid duplicate loads. */
    const existing = this.#storePromises.get(options.storeId) as Promise<Store<TSchema>> | undefined
    if (existing) return existing

    /**
     * Capture settled result so React.use can reuse fulfilled/rejected values
     * without re-awaiting (status tagging happens in useStore).
     */
    const promise = loader(options).then(
      (value) => {
        this.#storeResults.set(options.storeId, { status: 'fulfilled', value })
        return value
      },
      (reason) => {
        this.#storeResults.set(options.storeId, { status: 'rejected', reason })
        throw reason
      },
    )

    this.#storePromises.set(options.storeId, promise)
    return promise
  }

  getResult = <TSchema extends LiveStoreSchema>(storeId: StoreId): StoreResult<TSchema> | undefined =>
    this.#storeResults.get(storeId) as StoreResult<TSchema> | undefined

  delete = (storeId: StoreId): void => {
    this.#storePromises.delete(storeId)
    this.#storeResults.delete(storeId)
  }
}
