import { OtelLiveDummy, UnknownError } from '@livestore/common'
import type { LiveStoreSchema } from '@livestore/common/schema'
import { createStore, type Store, type Unsubscribe } from '@livestore/livestore'
import { shouldNeverHappen } from '@livestore/utils'
import {
  Cause,
  Data,
  Effect,
  Equal,
  Exit,
  Fiber,
  Layer,
  ManagedRuntime,
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
            Effect.acquireRelease(() =>
              Effect.gen(this, function* () {
                this.#optionsByStoreId.delete(options.storeId)
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
    const exit = this.getOrLoad<TSchema>(options).pipe(Effect.scoped, Runtime.runSyncExit(this.#runtime))

    if (Exit.isSuccess(exit)) return exit.value as Store<TSchema>

    // Check if the failure is due to async work
    const defect = Cause.dieOption(exit.cause)
    if (defect._tag === 'Some' && Runtime.isAsyncFiberException(defect.value)) {
      // Use the already-running fiber from the exception
      const fiber = defect.value.fiber
      return Fiber.join(fiber).pipe(Effect.runPromise) as Promise<Store<TSchema>>
    }

    // Handle synchronous failure
    throw Cause.squash(exit.cause)
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
