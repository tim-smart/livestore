import type { LiveStoreSchema } from '@livestore/common/schema'
import type { Store } from '@livestore/livestore'
import * as React from 'react'
import type { ReactApi } from '../../LiveStoreContext.ts'
import { withReactApi } from '../../useStore.ts'
import { useStoreRegistry } from './StoreRegistryContext.tsx'
import type { CachedStoreOptions } from './types.ts'

/**
 * Suspense + Error Boundary friendly hook.
 * - Returns data or throws (Promise|Error).
 * - No loading or error states are returned.
 */
export const useStore = <TSchema extends LiveStoreSchema>(
  options: CachedStoreOptions<TSchema>,
): Store<TSchema> & ReactApi => {
  const storeRegistry = useStoreRegistry()

  /** Keep the store retained while this hook is mounted; shutdown runs when unsubscribed. */
  React.useEffect(() => storeRegistry.retain(options), [storeRegistry, options])

  /** Promise is stable per options and drives Suspense via React.use. */
  const storeThenable = React.useMemo(
    () => tagThenableStatus(storeRegistry.getOrLoadStore(options)),
    [storeRegistry, options],
  )

  /** Suspends on first read; status-tagged promise lets React reuse the settled value or error on rerenders. */
  const store = React.use(storeThenable)

  return withReactApi(store)
}

const tagThenableStatus = <T>(
  resource: Promise<T> | T,
): Promise<T> & {
  status?: 'pending' | 'fulfilled' | 'rejected'
  value?: T
  reason?: unknown
} => {
  /**
   * React caches promise status on the thenable itself. We mirror that shape:
   * - If already tagged, reuse it (preserves React's cached status)
   * - Otherwise tag on settle so React.use can reuse the fulfilled/rejected value
   */
  const thenable = (isPromiseLike(resource) ? resource : Promise.resolve(resource)) as Promise<T> & {
    status?: 'pending' | 'fulfilled' | 'rejected'
    value?: T
    reason?: unknown
  }

  if (thenable.status !== undefined) return thenable

  if (!isPromiseLike(resource)) {
    thenable.status = 'fulfilled'
    thenable.value = resource as T
    return thenable
  }

  thenable.then(
    (value) => {
      thenable.status = 'fulfilled'
      thenable.value = value
    },
    (reason) => {
      thenable.status = 'rejected'
      thenable.reason = reason
    },
  )

  return thenable
}

const isPromiseLike = (value: unknown): value is Promise<unknown> =>
  typeof value === 'object' && value !== null && 'then' in value
