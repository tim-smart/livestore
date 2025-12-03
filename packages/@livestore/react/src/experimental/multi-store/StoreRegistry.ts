import type { UnknownError } from '@livestore/common'
import { OtelLiveDummy } from '@livestore/common'
import type { LiveStoreSchema } from '@livestore/common/schema'
import { createStore, createStorePromise, type Store, type Unsubscribe } from '@livestore/livestore'
import {
  Effect,
  type Fiber,
  Layer,
  ManagedRuntime,
  type OtelTracer,
  RcMap,
  RcRef,
  type Runtime,
  type Scope,
  Subscribable,
} from '@livestore/utils/effect'
import type { CachedStoreOptions, StoreId } from './types.ts'

// TODO: change status to _tag
// type StoreEntryState<TSchema extends LiveStoreSchema> =
//   | { status: 'idle' }
//   | {
//       status: 'loading'
//       // fiber: Fiber.Fiber<Store<TSchema>, UnknownError>
//       // scope: Scope.CloseableScope
//       // rc: number
//     }
//   | {
//       status: 'loaded'
//       store: Store<TSchema>
//       scope: Scope.CloseableScope
//       shutdownCallbacks: () => void
//       rc: number
//     }
//   | {
//       status: 'error'
//       error: unknown
//       scope: Scope.CloseableScope
//       rc: number
//     }
//   | {
//       status: 'shutting_down'
//       shutdownFiber: Fiber.Fiber<void>
//       scope: Scope.CloseableScope
//       rc: number
//     }

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
// class StoreEntry<TSchema extends LiveStoreSchema = LiveStoreSchema> {
//   readonly #storeId: StoreId
//   // readonly #cache: StoreCache

//   #state: StoreEntryState<TSchema> = { status: 'idle' }

//   #rcRef: RcRef.RcRef<Store<TSchema>, UnknownError>

//   #unusedCacheTime?: number
//   // #disposalTimeout?: ReturnType<typeof setTimeout> | null

//   /**
//    * Set of subscriber callbacks to notify on state changes.
//    */
//   readonly #subscribers = new Set<() => void>()

//   constructor(
//     storeId: StoreId,
//     options: CachedStoreOptions<TSchema>,
//     runtime: Runtime.Runtime<Scope.Scope | OtelTracer.OtelTracer>,
//   ) {
//     this.#storeId = storeId
//     this.#rcRef = RcRef.make({
//       acquire: Effect.gen(this, function* () {
//         this.#state = { status: 'loading' }
//         return yield* createStore(options).pipe(
//           Effect.acquireRelease(() =>
//             Effect.gen(this, function* () {
//               for (const sub of this.#subscribers) {
//                 sub()
//               }
//             }),
//           ),
//         )
//       }),
//     }).pipe(Effect.provide(runtime), Effect.runSync)
//     // this.#cache = cache
//   }

//   /**
//    * Gets the loaded store or initiates loading if not already in progress.
//    *
//    * @param options - Store creation options
//    * @returns The loaded store if available, or a Promise that resolves to the loaded store
//    *
//    * @remarks
//    * This method handles the complete lifecycle of loading a store:
//    * - Returns the store directly if already loaded (synchronous)
//    * - Returns a Promise if loading is in progress or needs to be initiated
//    * - Transitions through loading → loaded/error states
//    * - Schedules disposal when loading completes without active subscribers
//    */
//   getOrLoad = (
//     options: CachedStoreOptions<TSchema>,
//     // todo: don't use a scope in this fn
//   ): Effect.Effect<Store<TSchema>, UnknownError, Scope.Scope | OtelTracer.OtelTracer> =>
//     Effect.gen(this, function* () {
//       // TODO: use Semaphore for all getOrLoadEffect calls
//       return yield* createStore(options)
//     })

//   use = (
//     options: CachedStoreOptions<TSchema>,
//     onShutdown: () => void,
//   ): Effect.Effect<Store<TSchema>, UnknownError, Scope.Scope> =>
//     Effect.gen(this, function* () {
//       // if (this.#state.status === 'idle') {
//       //   const scope = yield* Scope.make()
//       //   const fiber = yield* createStore(options).pipe(Scope.extend(scope), Effect.forkScoped)
//       //   // todo trigger subscribe here with unused-value timeout to later simulate the cleanup
//       //   this.#state = { status: 'loading', scope, fiber, rc: 1 }
//       //   return yield* fiber
//       // }

//       return yield* this.#rcRef.get
//     })
// }

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
  #rcMap: RcMap.RcMap<StoreId, { store: Store<LiveStoreSchema>; onShutdown: Set<() => void> }, UnknownError>
  #runtime: Runtime.Runtime<Scope.Scope | OtelTracer.OtelTracer>

  constructor(options: CachedStoreOptions) {
    this.#runtime =
      options.runtime ??
      ManagedRuntime.make(Layer.mergeAll(Layer.scope, OtelLiveDummy)).runtimeEffect.pipe(Effect.runSync)

    this.#rcMap = RcMap.make({
      lookup: (_storeId: StoreId) =>
        Effect.gen(this, function* () {
          const onShutdown = new Set<() => void>()
          const store = yield* createStore(options).pipe(
            Effect.acquireRelease(() =>
              Effect.gen(this, function* () {
                for (const sub of onShutdown) {
                  sub()
                }
              }),
            ),
          )
          return { store, onShutdown: new Set<() => void>() }
        }),
      idleTimeToLive: options.unusedCacheTime ?? DEFAULT_UNUSED_CACHE_TIME,
    }).pipe(Effect.provide(this.#runtime), Effect.runSync)
  }

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
  getOrLoad = <TSchema extends LiveStoreSchema>(
    storeId: StoreId,
  ): Effect.Effect<Store<TSchema>, UnknownError, Scope.Scope> =>
    Effect.gen(this, function* () {
      const storeEntry = yield* RcMap.get(this.#rcMap, storeId)

      return storeEntry.store as unknown as Store<TSchema>
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
  getOrLoadPromise = <TSchema extends LiveStoreSchema>(storeId: StoreId): Promise<Store<TSchema>> =>
    this.getOrLoad<TSchema>(storeId).pipe(Effect.provide(this.#runtime), Effect.runPromise)

  subscribe = (storeId: StoreId, listener: () => void): Unsubscribe => {
    const unsubscribe = Effect.gen(this, function* () {
      const entry = yield* RcMap.get(this.#rcMap, storeId)

      entry.onShutdown.add(listener)

      return yield* Effect.never
    }).pipe(Effect.provide(this.#runtime), Effect.runCallback)

    return () => unsubscribe()
  }
}
