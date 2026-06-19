import {
  bufferTime,
  catchError,
  filter,
  map,
  mergeMap,
  Observable,
  Subject,
  takeUntil,
  tap,
  EMPTY,
} from "rxjs";
import { Pipeline } from "../interfaces/pipeline.interface";

/**
 * Represents a batch of queued items and their associated response subjects.
 */
type BatchRequest = {
  items: string[];
  resolve: Map<string, Subject<any>>;
};

/**
 * Provides request batching capabilities for pseudonymization and identification
 * operations.
 *
 * Requests received within a short time window are grouped together and
 * processed in a single batch call. Results are then dispatched back to the
 * individual request observables.
 */
export class PseudoBatchService {
  private readonly pipelines = new Map<string, Pipeline>();

  /**
   * Creates and initializes a batching pipeline.
   *
   * Requests are buffered for up to 300 milliseconds or until 50 items are
   * accumulated, whichever comes first.
   *
   * @param name Unique pipeline name.
   * @param handler Function responsible for processing a batch of items.
   * @param destroy$ Observable used to stop and clean up the pipeline.
   * @returns The internal queue used to receive items.
   */
  initBatch<T = any>(
    name: string,
    handler: (items: string[]) => Observable<any[]>,
    destroy$: Subject<void>,
  ) {
    const queue = new Subject<string>();

    const subjects = new Map<string, Subject<T>>();
    const batch$ = new Subject<BatchRequest>();

    batch$
      .pipe(
        mergeMap(({ items, resolve }) =>
          handler(items).pipe(
            tap((results) => {
              this.dispatchBatchResults(items, resolve, results);
            }),
            catchError((err) => {
              // fail whole batch safely
              items.forEach((item) => {
                resolve.get(item)?.error(err);
                resolve.delete(item);
              });
              return EMPTY;
            }),
          ),
        ),
        takeUntil(destroy$),
      )
      .subscribe();

    queue
      .pipe(
        filter(Boolean),
        bufferTime(300, undefined, 50),
        filter((items) => items.length > 0),
        map((items) => Array.from(new Set(items))),
        map((items) => ({
          items,
          resolve: new Map(items.map((i) => [i, subjects.get(i)!])),
        })),
        tap((batch) => batch$.next(batch)),
        takeUntil(destroy$),
      )
      .subscribe();

    this.pipelines.set(name, {
      queue,
      subjects,
      cache: new Map<string, Observable<T>>(),
    });

    return queue;
  }

  /**
   * Dispatches batch results back to the original request subjects.
   *
   * Each result is matched to its corresponding item using the position within
   * the batch response.
   *
   * @param items Original batch items.
   * @param subjects Subjects awaiting results.
   * @param results Results returned by the batch handler.
   */
  private dispatchBatchResults<T>(
    items: string[],
    subjects: Map<string, Subject<T>>,
    results: T[],
  ) {
    items.forEach((item, index) => {
      const subject = subjects.get(item);
      if (!subject) return;

      const result = results?.[index];

      if (result !== undefined) {
        subject.next(result);
      }

      subject.complete();
      subjects.delete(item);
    });
  }

  /**
   * Queues an item for batch processing.
   *
   * Multiple requests for the same item share the same observable result until
   * the batch has been processed.
   *
   * @param item Item to process.
   * @param pipelineName Target pipeline name.
   * @returns Observable emitting the processed result.
   * @throws Error when the specified pipeline does not exist.
   */
  process<T = any>(item: string, pipelineName: string): Observable<T> {
    const pipeline = this.pipelines.get(pipelineName);
    if (!pipeline) throw new Error(`Pipeline not found: ${pipelineName}`);

    const cached = pipeline.cache.get(item);
    if (cached) return cached as Observable<T>;

    let subject = pipeline.subjects.get(item);

    if (!subject) {
      subject = new Subject<T>();
      pipeline.subjects.set(item, subject);
      pipeline.queue.next(item);
    }

    const out$ = subject.asObservable();

    const final$ = out$.pipe(
      filter((v) => v !== undefined && v !== null),
      map((v) => v as T),
    );

    pipeline.cache.set(item, final$);

    return final$;
  }
}
