import {
  bufferTime,
  catchError,
  defer,
  EMPTY,
  filter,
  finalize,
  map,
  mergeMap,
  Observable,
  shareReplay,
  Subject,
  takeLast,
  takeUntil,
  tap,
} from "rxjs";
import { Pipeline } from "../interfaces/pipeline.interface";

/**
 * Provides request batching capabilities for pseudonymization and identification
 * operations.
 *
 * Requests received within a short time window are grouped together and
 * processed in a single batch call. Results are then dispatched back to the
 * individual request observables. Only in-flight requests are shared; once a
 * batch has completed, later calls for the same item trigger a fresh batch.
 */
export class PseudoBatchService {
  /** Registered batching pipelines, indexed by pipeline name. */
  private readonly pipelines = new Map<string, Pipeline>();

  /**
   * Creates and initializes a batching pipeline.
   *
   * Requests are buffered for up to 300 milliseconds or until 10 items are
   * accumulated, whichever comes first. Duplicate items in the same buffer are
   * collapsed so that the handler receives each item only once.
   *
   * @param name Unique pipeline identifier.
   * @param handler Function invoked with the batched items.
   * @param destroy$ Observable used to terminate the pipeline.
   * @returns The internal queue associated with the pipeline.
   */
  initBatch<T = any>(
    name: string,
    handler: (items: string[]) => Observable<any>,
    destroy$: Subject<void>,
  ): Subject<string> {
    const queue = new Subject<string>();
    const subjects = new Map<string, Subject<T>>();

    queue
      .pipe(
        filter((item) => item !== undefined && item !== null),
        bufferTime(300, undefined, 10),
        filter((items) => items.length > 0),
        map((items) => Array.from(new Set(items))),
        mergeMap((items) =>
          defer(() => handler(items)).pipe(
            tap((results) => {
              this.dispatchBatchResults<T>(items, subjects, results as T[]);
            }),
            catchError((err) => {
              this.errorBatch(items, subjects, err);
              return EMPTY;
            }),
          ),
        ),
        takeUntil(destroy$),
      )
      .subscribe({
        error: (err) => {
          this.errorAllPendingSubjects(subjects, err);
        },
      });

    this.pipelines.set(name, {
      queue,
      subjects,
      cache: new Map<string, Observable<T>>(),
    });

    return queue;
  }

  /**
   * Queues an item for batch processing.
   *
   * Multiple requests for the same item share the same in-flight subject until
   * the batch has been processed. Completed subjects are removed during
   * dispatch, so later calls can trigger a fresh batch instead of receiving a
   * completed observable.
   *
   * @param item Item to process.
   * @param pipelineName Target pipeline name.
   * @returns Observable emitting the processed result.
   * @throws Error When the specified pipeline does not exist.
   */
  process<T = any>(item: string, pipelineName: string): Observable<T> {
    const pipeline = this.pipelines.get(pipelineName);

    if (!pipeline) {
      throw new Error(`Pipeline not found: ${pipelineName}`);
    }

    let subject = pipeline.subjects.get(item) as Subject<T> | undefined;

    if (!subject) {
      subject = new Subject<T>();
      pipeline.subjects.set(item, subject);
      pipeline.queue.next(item);
    }

    return subject.asObservable().pipe(
      filter((value) => value !== undefined && value !== null),
      map((value) => value as T),
    );
  }

  /**
   * Dispatches batch results back to the original request subjects.
   *
   * Each result is matched to its corresponding item by index. Missing results
   * produce an error instead of completing silently, which prevents consumers
   * from staying in a loading state indefinitely.
   *
   * Results are matched by index:
   * `results[index]` corresponds to `items[index]`.
   */
  private dispatchBatchResults<T>(
    items: string[],
    subjects: Map<string, Subject<T>>,
    results: T[],
  ): void {
    if (!Array.isArray(results)) {
      this.errorBatch(
        items,
        subjects,
        new Error("Batch handler did not return an array of results"),
      );
      return;
    }

    items.forEach((item, index) => {
      const subject = subjects.get(item);

      if (!subject) {
        return;
      }

      const result = results[index];

      if (result === undefined || result === null) {
        subject.error(
          new Error(
            `No batch result returned for item "${item}" at index ${index}`,
          ),
        );
      } else {
        subject.next(result);
        subject.complete();
      }

      subjects.delete(item);
    });
  }

  /**
   * Errors all pending subjects associated with a specific batch.
   *
   * @param items Items belonging to the failed batch.
   * @param subjects Subjects awaiting results.
   * @param err Error to forward to consumers.
   */
  private errorBatch<T>(
    items: string[],
    subjects: Map<string, Subject<T>>,
    err: unknown,
  ): void {
    items.forEach((item) => {
      const subject = subjects.get(item);

      if (subject) {
        subject.error(err);
        subjects.delete(item);
      }
    });
  }

  /**
   * Errors and clears all pending subjects for a pipeline.
   *
   * @param subjects Subjects awaiting results.
   * @param err Error to forward to consumers.
   */
  private errorAllPendingSubjects<T>(
    subjects: Map<string, Subject<T>>,
    err: unknown,
  ): void {
    subjects.forEach((subject) => subject.error(err));
    subjects.clear();
  }
}
