import {
  BehaviorSubject,
  Observable,
  Subject,
  of,
  finalize,
  exhaustMap,
  concatMap,
} from "rxjs";

/**
 * Lightweight in-memory queue service that serializes execution per key.
 *
 * Each key gets its own internal mutex queue so that all operations
 * associated with the same key are executed sequentially.
 */
export class QueueService {
  /**
   * Internal map of per-key execution queues.
   * Each queue serializes jobs using `concatMap`.
   */
  private readonly queues = new Map<
    string,
    Subject<() => Observable<unknown>>
  >();

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
   * @param key - Unique identifier for the queue partition.
   * @returns A Subject that accepts execution jobs.
   */
  private getQueue(key: string): Subject<() => Observable<unknown>> {
    if (!this.queues.has(key)) {
      const q = new Subject<() => Observable<unknown>>();

      // Serialize all jobs for this key
      q.pipe(concatMap((job) => job())).subscribe();

      this.queues.set(key, q);
    }

    return this.queues.get(key)!;
  }

  /**
   * Enqueues and executes an observable task in a per-key serialized queue.
   *
   * Execution rules:
   * - Tasks with the same key are executed sequentially.
   * - Tasks with different keys run independently.
   * - The provided BehaviorSubject is updated when a task starts and ends.
   *
   * @typeParam T - Type emitted by the handled observable.
   * @param queue$ - Shared state tracking active keys.
   * @param key - Key used to serialize execution.
   * @param handle - Observable representing the asynchronous task.
   * @returns An Observable that completes when the task completes.
   */
  queue<T = unknown>(
    queue$: BehaviorSubject<string[]>,
    key: string,
    handle: Observable<T>,
  ): Observable<T> {
    return of(null).pipe(
      exhaustMap(() => {
        const q = this.getQueue(key);

        return new Observable<T>((subscriber) => {
          // Enqueue job into per-key mutex queue
          q.next(() => {
            return new Observable<T>((innerSub) => {
              // Mark key as active
              queue$.next([...queue$.value, key]);

              handle
                .pipe(
                  finalize(() => {
                    // Remove key when execution ends
                    queue$.next(queue$.value.filter((k) => k !== key));
                  }),
                )
                .subscribe({
                  next: (value) => innerSub.next(value),
                  error: (err) => innerSub.error(err),
                  complete: () => {
                    innerSub.complete();
                    subscriber.complete();
                  },
                });
            });
          });
        });
      }),
    );
  }
}
