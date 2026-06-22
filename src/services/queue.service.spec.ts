import { delay, of, throwError } from "rxjs";
import { QueueService } from "./queue.service";

describe("QueueService (safe mode)", () => {
  let service: QueueService;

  beforeEach(() => {
    jest.useFakeTimers();
    service = new QueueService();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("creates a queue state with the provided initial value", () => {
    const queue$ = service.create<string[]>(["existing"]);

    expect(queue$.value).toEqual(["existing"]);
  });

  it("serializes operations sharing the same key", () => {
    const queue$ = service.create<string[]>([]);
    const results: string[] = [];

    service
      .queue(queue$, "same", of("first").pipe(delay(50)))
      .subscribe((value) => {
        results.push(value);
      });

    service.queue(queue$, "same", of("second")).subscribe((value) => {
      results.push(value);
    });

    expect(queue$.value).toEqual(["same"]);
    expect(results).toEqual([]);

    jest.advanceTimersByTime(49);
    expect(results).toEqual([]);
    expect(queue$.value).toEqual(["same"]);

    jest.advanceTimersByTime(1);
    expect(results).toEqual(["first", "second"]);
    expect(queue$.value).toEqual([]);
  });

  it("allows operations with different keys to run independently", () => {
    const queue$ = service.create<string[]>([]);
    const results: string[] = [];

    service
      .queue(queue$, "slow", of("slow").pipe(delay(50)))
      .subscribe((value) => {
        results.push(value);
      });

    service.queue(queue$, "fast", of("fast")).subscribe((value) => {
      results.push(value);
    });

    expect(results).toEqual(["fast"]);
    expect(queue$.value).toEqual(["slow"]);

    jest.advanceTimersByTime(50);
    expect(results).toEqual(["fast", "slow"]);
    expect(queue$.value).toEqual([]);
  });

  it("releases the key when the operation errors", () => {
    const queue$ = service.create<string[]>([]);
    const errors: unknown[] = [];

    service
      .queue(
        queue$,
        "same",
        throwError(() => new Error("boom")),
      )
      .subscribe({
        error: (error) => errors.push(error),
      });

    expect(errors).toHaveLength(1);
    expect(queue$.value).toEqual([]);
  });

  it("releases the key when the subscription is cancelled", () => {
    const queue$ = service.create<string[]>([]);

    const subscription = service
      .queue(queue$, "same", of("value").pipe(delay(50)))
      .subscribe();

    expect(queue$.value).toEqual(["same"]);

    subscription.unsubscribe();

    expect(queue$.value).toEqual([]);
  });
});
