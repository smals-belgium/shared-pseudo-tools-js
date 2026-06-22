import {
  BehaviorSubject,
  defer,
  filter,
  finalize,
  map,
  Observable,
  switchMap,
  take,
  tap,
} from "rxjs";

/**
 * Lightweight in-memory queue service that serializes execution per key.
 *
 * Requests sharing the same key are serialized and only allowed to execute once
 * the previous operation for that key has completed or errored. Requests with
 * different keys can continue independently.
 */
export class QueueService {
  /**
   * Creates a new reactive queue state container.
   *
   * @typeParam T - Type of the initial value (defaults to string array).
   * @param initValue - Initial value of the queue state.
   * @returns A BehaviorSubject holding the queue state.
   */
  create<T = string[]>(initValue: T): BehaviorSubject<T> {
    return new BehaviorSubject<T>(initValue);
  }

  /**
   * Retrieves or creates a per-key execution queue.
   *
   * Each key has a dedicated Subject that serializes execution using `concatMap`,
   * ensuring only one job runs at a time per key.
   *
   * If the key is already being processed, execution waits until it is released.
   * Once the operation completes or errors, the key is automatically removed
   * from the queue.
   *
   * @param queue$ Queue state containing currently processed keys.
   * @param key Unique identifier used for synchronization.
   * @param handle Observable operation to execute.
   * @returns Observable emitting the result of the provided operation.
   */
  queue<T = unknown>(
    queue$: BehaviorSubject<string[]>,
    key: string,
    handle: Observable<T>,
  ): Observable<T> {
    return defer(() =>
      this.waitForKey(queue$, key).pipe(
        tap(() => this.lockKey(queue$, key)),
        switchMap(() =>
          handle.pipe(finalize(() => this.releaseKey(queue$, key))),
        ),
      ),
    );
  }

  /**
   * Waits until a key is no longer present in the queue.
   *
   * @param queue$ Queue state containing currently processed keys.
   * @param key Key to wait for.
   * @returns Observable that emits once the key becomes available.
   */
  private waitForKey(
    queue$: BehaviorSubject<string[]>,
    key: string,
  ): Observable<void> {
    return queue$.pipe(
      filter((keys) => !keys.includes(key)),
      take(1),
      map(() => undefined),
    );
  }

  /**
   * Marks a key as currently being processed.
   *
   * @param queue$ Queue state containing currently processed keys.
   * @param key Key to lock.
   */
  private lockKey(queue$: BehaviorSubject<string[]>, key: string): void {
    const current = queue$.value;

    if (!current.includes(key)) {
      queue$.next([...current, key]);
    }
  }

  /**
   * Releases a key after its operation completes or errors.
   *
   * @param queue$ Queue state containing currently processed keys.
   * @param key Key to release.
   */
  private releaseKey(queue$: BehaviorSubject<string[]>, key: string): void {
    const current = queue$.value;

    if (current.includes(key)) {
      queue$.next(current.filter((item) => item !== key));
    }
  }
}
