import type { UnknownError } from '@livestore/common'
import type { LiveStoreSchema } from '@livestore/common/schema'
import { createStore, createStorePromise, type Store, type Unsubscribe } from '@livestore/livestore'
import { Effect, type Fiber, type OtelTracer, Scope, Subscribable } from '@livestore/utils/effect'
import type { CachedStoreOptions, StoreId } from './types.ts'

// TODO: change status to _tag
type StoreEntryState<TSchema extends LiveStoreSchema> =
  | { status: 'idle' }
  | {
      status: 'loading'
      fiber: Fiber.Fiber<Store<TSchema>, UnknownError>
      scope: Scope.CloseableScope
      rc: number
    }
  | { status: 'loaded'; store: Store<TSchema>; scope: Scope.CloseableScope; rc: number }
  | { status: 'error'; error: unknown; scope: Scope.CloseableScope; rc: number }
  | { status: 'shutting_down'; shutdownFiber: Fiber.Fiber<void>; scope: Scope.CloseableScope; rc: number }

type StatusState = {
  _
}

/**
 * Default time to keep unused stores in cache.
 *
 * - Browser: 60 seconds (60,000ms)
 * - SSR: Infinity (disables disposal to avoid disposing stores before server render completes)
 *
 * @internal Exported primarily for testing purposes.
 */
export const DEFAULT_UNUSED_CACHE_TIME = typeof window === 'undefined' ? Number.POSITIVE_INFINITY : 60_000

/**
 * @typeParam TSchema - The schema for this entry's store.
 * @internal
 */
class StoreEntry<TSchema extends LiveStoreSchema = LiveStoreSchema> {
  readonly #storeId: StoreId
  readonly #cache: StoreCache

  #state: StoreEntryState<TSchema> = { status: 'idle' }

  #unusedCacheTime?: number
  #disposalTimeout?: ReturnType<typeof setTimeout> | null

  /**
   * Set of subscriber callbacks to notify on state changes.
   */
  readonly #subscribers = new Set<() => void>()

  constructor(storeId: StoreId, cache: StoreCache) {
    this.#storeId = storeId
    this.#cache = cache
  }

  #scheduleDisposal = (): void => {
    this.#cancelDisposal()

    const effectiveTime = this.#unusedCacheTime === undefined ? DEFAULT_UNUSED_CACHE_TIME : this.#unusedCacheTime

    if (effectiveTime === Number.POSITIVE_INFINITY) return // Infinity disables disposal

    this.#disposalTimeout = setTimeout(() => {
      this.#disposalTimeout = null

      // Re-check to avoid racing with a new subscription
      if (this.#subscribers.size > 0) return

      // Abort any in-progress loading to release resources early
      this.#abortLoading()

      // Transition to shutting_down state BEFORE starting async shutdown.
      // This prevents new subscribers from receiving a store that's about to be disposed.
      const shutdownPromise = this.#shutdown().finally(() => {
        // Reset to idle so fresh loads can proceed, then remove from cache if still inactive
        this.#setIdle()
        if (this.#subscribers.size === 0) this.#cache.delete(this.#storeId)
      })

      this.#setShuttingDown(shutdownPromise)
    }, effectiveTime)
  }

  #cancelDisposal = (): void => {
    if (!this.#disposalTimeout) return
    clearTimeout(this.#disposalTimeout)
    this.#disposalTimeout = null
  }

  /**
   * Transitions to the loading state.
   */
  #setLoading(promise: Promise<Store<TSchema>>, abortController: AbortController): void {
    if (this.#state.status === 'loaded' || this.#state.status === 'loading') return
    this.#state = { status: 'loading', promise, abortController }
    this.#notify()
  }

  /**
   * Transitions to the loaded state.
   */
  #setStore = (store: Store<TSchema>): void => {
    this.#state = { status: 'loaded', store }
    this.#notify()
  }

  /**
   * Transitions to the error state.
   */
  #setError = (error: unknown): void => {
    this.#state = { status: 'error', error }
    this.#notify()
  }

  /**
   * Transitions to the shutting_down state.
   */
  #setShuttingDown = (shutdownPromise: Promise<void>): void => {
    this.#state = { status: 'shutting_down', shutdownFiber: shutdownPromise }
    this.#notify()
  }

  /**
   * Transitions to the idle state.
   */
  #setIdle = (): void => {
    this.#state = { status: 'idle' }
    // No notify needed - getOrLoad will handle the fresh load
  }

  /**
   * Notifies all subscribers of state changes.
   *
   * @remarks
   * This should be called after any meaningful state change.
   */
  #notify = (): void => {
    for (const sub of this.#subscribers) {
      try {
        sub()
      } catch {
        // Swallow to protect other listeners
      }
    }
  }

  /**
   * Subscribes to this entry's updates.
   *
   * @param listener - Callback invoked when the entry changes
   * @returns Unsubscribe function
   */
  subscribe = (listener: () => void): Unsubscribe => {
    this.#cancelDisposal()
    this.#subscribers.add(listener)
    return () => {
      this.#subscribers.delete(listener)
      // If no more subscribers remain, schedule disposal
      if (this.#subscribers.size === 0) this.#scheduleDisposal()
    }
  }

  // subscribeStream = (): Stream.Stream => {}

  /**
   * Gets the loaded store or initiates loading if not already in progress.
   *
   * @param options - Store creation options
   * @returns The loaded store if available, or a Promise that resolves to the loaded store
   *
   * @remarks
   * This method handles the complete lifecycle of loading a store:
   * - Returns the store directly if already loaded (synchronous)
   * - Returns a Promise if loading is in progress or needs to be initiated
   * - Transitions through loading → loaded/error states
   * - Schedules disposal when loading completes without active subscribers
   */
  getOrLoadEffect = (
    options: CachedStoreOptions<TSchema>,
  ): Effect.Effect<Store<TSchema>, UnknownError, Scope.Scope | OtelTracer.OtelTracer> =>
    Effect.gen(this, function* () {
      yield* Effect.addFinalizer(() => Effect.gen(this, function* () {}))
      if (this.#state.status === 'idle') {
        const scope = yield* Scope.make()
        const fiber = yield* createStore(options).pipe(Scope.extend(scope), Effect.forkScoped)
        this.#state = { status: 'loading', scope, fiber, rc: 1 }
        return yield* fiber
      }

      // TODO: use Semaphore for all getOrLoadEffect calls
      return yield* createStore(options)
    })

  status: Effect = Effect.gen(this, function* () {})

  /**
   * Gets the loaded store or initiates loading if not already in progress.
   *
   * @param options - Store creation options
   * @returns The loaded store if available, or a Promise that resolves to the loaded store
   *
   * @remarks
   * This method handles the complete lifecycle of loading a store:
   * - Returns the store directly if already loaded (synchronous)
   * - Returns a Promise if loading is in progress or needs to be initiated
   * - Transitions through loading → loaded/error states
   * - Schedules disposal when loading completes without active subscribers
   */
  getOrLoad = (options: CachedStoreOptions<TSchema>): Store<TSchema> | Promise<Store<TSchema>> => {
    if (options.unusedCacheTime !== undefined)
      this.#unusedCacheTime = Math.max(this.#unusedCacheTime ?? 0, options.unusedCacheTime)

    if (this.#state.status === 'loaded') return this.#state.store
    if (this.#state.status === 'loading') return this.#state.fiber
    if (this.#state.status === 'error') throw this.#state.error

    // Wait for shutdown to complete, then recursively call to load a fresh store
    if (this.#state.status === 'shutting_down') {
      return this.#state.shutdownFiber.then(() => this.getOrLoad(options))
    }

    const abortController = new AbortController()

    const promise = createStorePromise({ ...options, signal: abortController.signal })
      .then((store) => {
        this.#setStore(store)
        return store
      })
      .catch((error) => {
        this.#setError(error)
        throw error
      })
      .finally(() => {
        // The store entry may have become unused (no subscribers) while loading the store
        if (this.#subscribers.size === 0) this.#scheduleDisposal()
      })

    this.#setLoading(promise, abortController)

    return promise
  }

  /**
   * Aborts an in-progress store load.
   *
   * This signals the createStorePromise to cancel, releasing resources like
   * worker threads, SQLite connections, and network requests.
   */
  #abortLoading = (): void => {
    if (this.#state.status !== 'loading') return
    this.#state.abortController.abort()
  }

  #shutdown = async (): Promise<void> => {
    if (this.#state.status !== 'loaded') return
    await this.#state.store.shutdownPromise().catch((reason) => {
      console.warn(`Store ${this.#storeId} failed to shutdown cleanly during disposal:`, reason)
    })
  }
}

/**
 * In-memory map of {@link StoreEntry} instances keyed by {@link StoreId}.
 *
 * @privateRemarks
 * The cache is intentionally small; eviction and disposal timers are coordinated by the client.
 *
 * @internal
 */
class StoreCache {
  readonly #entries = new Map<StoreId, StoreEntry>()

  get = <TSchema extends LiveStoreSchema>(storeId: StoreId): StoreEntry<TSchema> | undefined => {
    return this.#entries.get(storeId) as StoreEntry<TSchema> | undefined
  }

  ensure = <TSchema extends LiveStoreSchema>(storeId: StoreId): StoreEntry<TSchema> => {
    let entry = this.#entries.get(storeId) as StoreEntry<TSchema> | undefined

    if (!entry) {
      entry = new StoreEntry<TSchema>(storeId, this)
      this.#entries.set(storeId, entry as unknown as StoreEntry)
    }

    return entry
  }

  /**
   * Removes an entry from the cache.
   *
   * @param storeId - The ID of the store to remove
   */
  delete = (storeId: StoreId): void => {
    this.#entries.delete(storeId)
  }
}

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
}

/**
 * Store Registry coordinating store loading, caching, and subscription
 *
 * @public
 */
export class StoreRegistry {
  readonly #cache = new StoreCache()

  /**
   * Get or load a store, returning it directly if loaded or a promise if loading.
   *
   * @typeParam TSchema - The schema of the store to load
   * @returns The loaded store if available, or a Promise that resolves to the loaded store
   * @throws unknown loading error
   *
   * @remarks
   * - Returns the store instance directly (synchronous) when already loaded
   * - Returns a stable Promise reference when loading is in progress or needs to be initiated
   * - Applies default options from registry config, with call-site options taking precedence
   */
  getOrLoad = <TSchema extends LiveStoreSchema>(options: CachedStoreOptions<TSchema>): Effect.Effect<Store<TSchema>> =>
    Effect.gen(this, function* () {
      const storeEntry = this.#cache.ensure<TSchema>(options.storeId)

      return storeEntry.getOrLoad(options)
    })

  /**
   * Get or load a store, returning it directly if loaded or a promise if loading.
   *
   * @typeParam TSchema - The schema of the store to load
   * @returns The loaded store if available, or a Promise that resolves to the loaded store
   * @throws unknown loading error
   *
   * @remarks
   * - Returns the store instance directly (synchronous) when already loaded
   * - Returns a stable Promise reference when loading is in progress or needs to be initiated
   * - Applies default options from registry config, with call-site options taking precedence
   */
  getOrLoadPromise = <TSchema extends LiveStoreSchema>(
    options: CachedStoreOptions<TSchema>,
  ): Store<TSchema> | Promise<Store<TSchema>> => {
    const storeEntry = this.#cache.ensure<TSchema>(options.storeId)

    return storeEntry.getOrLoad(options)
  }

  subscribe = <TSchema extends LiveStoreSchema>(storeId: StoreId, listener: () => void): Unsubscribe => {
    const entry = this.#cache.ensure<TSchema>(storeId)

    return entry.subscribe(listener)
  }
}
