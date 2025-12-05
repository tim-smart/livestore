import { makeInMemoryAdapter } from '@livestore/adapter-web'
import { makeShutdownDeferred, StoreInternalsSymbol } from '@livestore/livestore'
import { Deferred, Effect, Exit, type OtelTracer, Scope } from '@livestore/utils/effect'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { schema } from '../../__tests__/fixture.tsx'
import { StoreRegistry } from './StoreRegistry.ts'
import { storeOptions } from './storeOptions.ts'
import type { CachedStoreOptions } from './types.ts'

const testStoreId = 'test-store'

const sharedAdapter = makeInMemoryAdapter()

const testStoreOptions = (overrides: Partial<CachedStoreOptions<typeof schema>> = {}) =>
  storeOptions({
    storeId: testStoreId,
    schema,
    adapter: sharedAdapter,
    disableDevtools: true,
    ...overrides,
  })

const makeRegistry = (options: { unusedCacheTime?: number } = {}) =>
  Effect.gen(function* () {
    const runtime = yield* Effect.runtime<Scope.Scope | OtelTracer.OtelTracer>()
    return new StoreRegistry({ defaultOptions: { unusedCacheTime: 10, ...options, runtime } })
  })

Vitest.describe('StoreRegistry', { timeout: 60_000 }, () => {
  // TODO: replace Vitest fake timers with Effect TestClock for deterministic time control
  const withTest = Vitest.makeWithTestCtx({ timeout: 60_000 })

  Vitest.scopedLive('returns a Promise when the store is loading', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry()
      const shutdownDeferred = yield* makeShutdownDeferred
      const optionsWithShutdown = testStoreOptions({ shutdownDeferred })

      const result = registry.getOrLoadStore(optionsWithShutdown)
      Vitest.expect(result).toBeInstanceOf(Promise)

      const store = yield* Effect.promise(async () => result)
      Vitest.expect(store[StoreInternalsSymbol].clientSession.debugInstanceId).toBeDefined()
      yield* Effect.promise(() => store.shutdownPromise().catch(() => undefined))
      yield* Deferred.await(shutdownDeferred)
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('returns the same store for repeated loads', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry()
      const options = testStoreOptions()

      const store = yield* registry.getOrLoad(options)
      const cached = yield* registry.getOrLoad(options)

      Vitest.expect(cached).toBe(store)
      yield* Effect.promise(() => store.shutdownPromise().catch(() => undefined))
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('reuses the same promise for concurrent loads', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry()
      const options = testStoreOptions()

      // This guards the contract: concurrent callers must see the same in-flight promise
      // (no duplicate fibers/adapter bootstraps), and after resolution both promises yield
      // the same store instance.
      const first = registry.getOrLoadStore(options)
      const second = registry.getOrLoadStore(options)

      Vitest.expect(second).toBe(first)

      const [storeA, storeB] = yield* Effect.all(
        [Effect.promise(async () => first), Effect.promise(async () => second)],
        {
          concurrency: 'unbounded',
        },
      )

      Vitest.expect(storeA).toBe(storeB)
      yield* Effect.promise(() => storeA.shutdownPromise().catch(() => undefined))
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('reuses the same store across concurrent loads', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry()
      const options = testStoreOptions()

      const scope = yield* Scope.make()
      const [storeA, storeB] = yield* Effect.all([registry.getOrLoad(options), registry.getOrLoad(options)], {
        concurrency: 'unbounded',
      }).pipe(Scope.extend(scope))

      Vitest.expect(storeA).toBe(storeB)

      yield* Scope.close(scope, Exit.void)
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('creates a fresh store after the idle cache window closes', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry({ unusedCacheTime: 5 })
      const options = testStoreOptions()

      const scope = yield* Scope.make()
      const initial = yield* registry.getOrLoad(options).pipe(Scope.extend(scope))

      yield* Scope.close(scope, Exit.void)
      yield* Effect.sleep(10)

      const next = yield* registry.getOrLoad(options)

      Vitest.expect(next).not.toBe(initial)
      yield* Effect.promise(() => next.shutdownPromise().catch(() => undefined))
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('does not dispose when unusedCacheTime is Infinity', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry({ unusedCacheTime: Number.POSITIVE_INFINITY })
      const options = testStoreOptions()

      const store = yield* registry.getOrLoad(options)

      yield* Effect.sleep(10)

      const cached = yield* registry.getOrLoad(options)
      Vitest.expect(cached).toBe(store)
      yield* Effect.promise(() => cached.shutdownPromise().catch(() => undefined))
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('applies call-site unusedCacheTime override', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry({ unusedCacheTime: 1_000 })
      const options = testStoreOptions({ unusedCacheTime: 10 })

      const store = yield* registry.getOrLoad(options)
      yield* Effect.sleep(10)

      const next = yield* registry.getOrLoad(options)
      Vitest.expect(next).not.toBe(store)
      yield* Effect.promise(() => next.shutdownPromise().catch(() => undefined))
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('applies constructor defaults when no override is provided', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry({ unusedCacheTime: 100 })
      const options = testStoreOptions()

      const store = yield* registry.getOrLoad(options)
      yield* Effect.sleep(50)

      const cached = yield* registry.getOrLoad(options)
      Vitest.expect(cached).toBe(store)
      yield* Effect.promise(() => cached.shutdownPromise().catch(() => undefined))
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('preload warms the cache', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry({ unusedCacheTime: 50 })
      const options = testStoreOptions()

      yield* Effect.promise(() => registry.preload(options))

      const store = yield* registry.getOrLoad(options)
      const cached = yield* registry.getOrLoad(options)

      Vitest.expect(cached).toBe(store)
      yield* Effect.promise(() => cached.shutdownPromise().catch(() => undefined))
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('disposes after preload when unused', (test) =>
    Effect.gen(function* () {
      const unusedCacheTime = 20
      const registry = yield* makeRegistry({ unusedCacheTime })
      const options = testStoreOptions()

      yield* Effect.promise(() => registry.preload(options))
      const store = yield* registry.getOrLoad(options)

      yield* Effect.sleep(unusedCacheTime + 5)

      const next = yield* registry.getOrLoad(options)
      Vitest.expect(next).not.toBe(store)
      yield* Effect.promise(() => next.shutdownPromise().catch(() => undefined))
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('surfaces cached errors across calls', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry({ unusedCacheTime: 10 })
      const failingAdapter = (): any => Effect.fail(new Error('boom'))
      const badOptions = testStoreOptions({ adapter: failingAdapter as any })

      const first = yield* Effect.either(Effect.promise(async () => registry.getOrLoadStore(badOptions)))
      Vitest.expect(first._tag).toBe('Left')

      const second = yield* Effect.either(Effect.promise(async () => registry.getOrLoadStore(badOptions)))
      Vitest.expect(second._tag).toBe('Left')
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('allows subscribing and unsubscribing without affecting loads', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry({ unusedCacheTime: 50 })
      const options = testStoreOptions()

      const unsubscribe = registry.retain(options)
      const store = yield* registry.getOrLoad(options)

      unsubscribe()

      const cached = yield* registry.getOrLoad(options)
      Vitest.expect(cached).toBe(store)
      yield* Effect.promise(() => cached.shutdownPromise().catch(() => undefined))
    }).pipe(withTest(test)),
  )

  Vitest.scopedLive('manages multiple stores independently', (test) =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry({ unusedCacheTime: 5 })
      const optsA = testStoreOptions({ storeId: 'store-a' })
      const optsB = testStoreOptions({ storeId: 'store-b' })

      const scopeA = yield* Scope.make()
      const scopeB = yield* Scope.make()

      const storeA = yield* registry.getOrLoad(optsA).pipe(Scope.extend(scopeA))
      const storeB = yield* registry.getOrLoad(optsB).pipe(Scope.extend(scopeB))

      Vitest.expect(storeA).not.toBe(storeB)

      yield* Scope.close(scopeA, Exit.void)
      yield* Effect.sleep(10)

      // storeA should be gone after idle window; storeB should still be cached
      const newStoreA = yield* registry.getOrLoad(optsA)
      const cachedStoreB = yield* registry.getOrLoad(optsB)

      Vitest.expect(newStoreA).not.toBe(storeA)
      Vitest.expect(cachedStoreB).toBe(storeB)

      yield* Scope.close(scopeB, Exit.void)
      yield* Effect.promise(() => newStoreA.shutdownPromise().catch(() => undefined))
      yield* Effect.promise(() => cachedStoreB.shutdownPromise().catch(() => undefined))
    }).pipe(withTest(test)),
  )
})
