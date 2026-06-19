import {
  BehaviorSubject,
  exhaustMap,
  filter,
  finalize,
  Observable,
  of,
  Subject,
  tap,
} from "rxjs";

/**
 * Provides a lightweight in-memory queue mechanism to prevent concurrent
 * processing of identical keys.
 *
 * Requests sharing the same key are serialized and only allowed to execute
 * once the previous operation for that key has completed.
 */
export class QueueService {
  private readonly unlock$ = new Subject<string>();

  /**
   * Creates a queue state container.
   *
   * @param initValue Initial queue content.
   * @returns A BehaviorSubject holding the queued keys.
   */
  create<T = string[]>(initValue: T) {
    return new BehaviorSubject<T>(initValue);
  }

  /**
   * Executes an observable operation only when the provided key is available.
   *
   * If the key is already being processed, execution waits until it is released.
   * Once the operation completes, the key is automatically removed from the queue.
   *
   * @param queue$ Queue state containing currently processed keys.
   * @param key Unique identifier used for synchronization.
   * @param handle Observable operation to execute.
   * @returns An observable that emits the result of the provided operation.
   */
  queue<T = string[]>(
    queue$: BehaviorSubject<string[]>,
    key: string,
    handle: Observable<T>,
  ) {
    return of(null).pipe(
      exhaustMap(() =>
        this.waitForKey(queue$, key).pipe(
          tap(() => {
            const current = queue$.value;
            if (!current.includes(key)) {
              queue$.next([...current, key]);
            }
          }),
          exhaustMap(() =>
            handle.pipe(
              finalize(() => {
                const current = queue$.value;
                queue$.next(current.filter((i) => i !== key));
                this.unlock$.next(key);
              }),
            ),
          ),
        ),
      ),
    );
  }

  /**
   * Waits until a key is no longer present in the queue.
   *
   * This method is used internally to coordinate concurrent requests
   * targeting the same key.
   *
   * @param queue$ Queue state containing currently processed keys.
   * @param key Key to wait for.
   * @returns An observable that completes once the key becomes available.
   */
  private waitForKey(queue$: BehaviorSubject<string[]>, key: string) {
    return new Observable<void>((subscriber) => {
      const check = () => {
        if (!queue$.value.includes(key)) {
          subscriber.next();
          subscriber.complete();
        }
      };

      const sub = this.unlock$.pipe(filter((k) => k === key)).subscribe(check);
      check();
      return () => sub.unsubscribe();
    });
  }
}
