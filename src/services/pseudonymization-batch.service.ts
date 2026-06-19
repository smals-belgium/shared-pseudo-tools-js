import {
  bufferTime,
  catchError,
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
 * Service used to aggregate multiple single-item requests
 * into a single batch request.
 *
 * Calls to {@link process} made within a 300 ms window
 * (or up to 10 items) are grouped together and passed
 * to the handler registered through {@link initBatch}.
 */
export class PseudoBatchService {
  private readonly pipelines = new Map<string, Pipeline>();

  /**
   * Initializes a batch processing pipeline.
   *
   * @param name Unique pipeline identifier.
   * @param handler Function invoked with the batched items.
   * @param destroy$ Observable used to terminate the pipeline.
   * @returns The internal queue associated with the pipeline.
   */
  initBatch<T = any>(
    name: string,
    handler: (items: string[]) => Observable<T[]>,
    destroy$: Subject<void>,
  ): Subject<string> {
    const queue = new Subject<string>();
    const subjects = new Map<string, Subject<T>>();

    queue
      .pipe(
        filter(Boolean),
        bufferTime(300, undefined, 10),
        filter((items) => items.length > 0),

        // Prevent duplicate items from being sent
        // within the same batch.
        map((items) => [...new Set(items)]),

        mergeMap((items) =>
          handler(items).pipe(
            tap((results) =>
              this.dispatchBatchResults<T>(items, subjects, results),
            ),

            // A handler error should not permanently
            // terminate the pipeline.
            catchError((error) => {
              items.forEach((item) => {
                const subject = subjects.get(item);

                if (subject) {
                  subject.error(error);
                  subjects.delete(item);
                }
              });

              return EMPTY;
            }),
          ),
        ),

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
   * Dispatches batch results to the corresponding subscribers.
   *
   * Results are matched by index:
   * `results[index]` corresponds to `items[index]`.
   */
  private dispatchBatchResults<T>(
    items: string[],
    subjects: Map<string, Subject<T>>,
    results: T[],
  ): void {
    items.forEach((item, index) => {
      const subject = subjects.get(item);

      if (!subject) {
        return;
      }

      const result = results?.[index];

      // Accept falsy values such as 0, false or "".
      if (result !== undefined) {
        subject.next(result);
      }

      subject.complete();
      subjects.delete(item);
    });
  }

  /**
   * Queues an item for processing within the specified pipeline.
   *
   * If multiple requests for the same item are received before
   * the batch is executed, only one request is sent and the
   * resulting value is shared across all subscribers.
   *
   * @param item Item identifier to process.
   * @param pipelineName Target pipeline name.
   * @returns An observable emitting the associated result.
   */
  process<T = any>(item: string, pipelineName: string): Observable<T> {
    const pipeline = this.pipelines.get(pipelineName);

    if (!pipeline) {
      throw new Error(`Pipeline not found: ${pipelineName}`);
    }

    const cached = pipeline.cache.get(item);

    if (cached) {
      return cached;
    }

    let subject = pipeline.subjects.get(item);

    if (!subject) {
      subject = new Subject<T>();
      pipeline.subjects.set(item, subject);
      pipeline.queue.next(item);
    }

    const out$ = subject.asObservable().pipe(
      filter((value): value is T => value !== undefined),

      // The subject emits a single value before completion.
      takeLast(1),

      // Remove the cache entry once processing completes.
      finalize(() => {
        pipeline.cache.delete(item);
      }),

      // Share the result across concurrent subscribers.
      shareReplay({
        bufferSize: 1,
        refCount: true,
      }),
    );

    pipeline.cache.set(item, out$);

    return out$;
  }
}
