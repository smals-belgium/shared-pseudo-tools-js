import { Observable, of, Subject, throwError } from "rxjs";
import { PseudoBatchService } from "./pseudonymization-batch.service";

const flushBatch = async (milliseconds = 300): Promise<void> => {
  jest.advanceTimersByTime(milliseconds);
  await Promise.resolve();
  await Promise.resolve();
};

describe("PseudoBatchService", () => {
  let service: PseudoBatchService;
  let destroy$: Subject<void>;

  beforeEach(() => {
    jest.useFakeTimers();
    service = new PseudoBatchService();
    destroy$ = new Subject<void>();
  });

  afterEach(() => {
    destroy$.next();
    destroy$.complete();
    jest.useRealTimers();
  });

  it("batches queued items and keeps input order", async () => {
    let receivedItems: string[] = [];
    const values: string[] = [];

    service.initBatch<string>(
      "test",
      (items) => {
        receivedItems = items;
        return of(items.map((item) => `result:${item}`));
      },
      destroy$,
    );

    service.process<string>("a", "test").subscribe((value) => values.push(value));
    service.process<string>("b", "test").subscribe((value) => values.push(value));

    await flushBatch();

    expect(receivedItems).toEqual(["a", "b"]);
    expect(values).toEqual(["result:a", "result:b"]);
  });

  it("shares one in-flight subject for duplicate items", async () => {
    let handlerCalls = 0;
    let receivedItems: string[] = [];
    const values: string[] = [];

    service.initBatch<string>(
      "test",
      (items) => {
        handlerCalls++;
        receivedItems = items;
        return of(items.map((item) => `result:${item}`));
      },
      destroy$,
    );

    service.process<string>("same", "test").subscribe((value) => {
      values.push(`first:${value}`);
    });
    service.process<string>("same", "test").subscribe((value) => {
      values.push(`second:${value}`);
    });

    await flushBatch();

    expect(handlerCalls).toBe(1);
    expect(receivedItems).toEqual(["same"]);
    expect(values).toEqual(["first:result:same", "second:result:same"]);
  });

  it("triggers a fresh batch for the same item after the previous batch completes", async () => {
    let handlerCalls = 0;
    const values: string[] = [];

    service.initBatch<string>(
      "test",
      (items) => {
        handlerCalls++;
        return of(items.map((item) => `result:${item}:${handlerCalls}`));
      },
      destroy$,
    );

    service.process<string>("same", "test").subscribe((value) => values.push(value));
    await flushBatch();

    service.process<string>("same", "test").subscribe((value) => values.push(value));
    await flushBatch();

    expect(handlerCalls).toBe(2);
    expect(values).toEqual(["result:same:1", "result:same:2"]);
  });

  it("errors consumers when a result is missing", async () => {
    const errors: Error[] = [];

    service.initBatch<string>("test", () => of([]), destroy$);

    service.process<string>("missing", "test").subscribe({
      error: (error) => errors.push(error),
    });

    await flushBatch();

    expect(errors).toHaveLength(1);
    expect(errors[0].message).toContain("No batch result returned");
  });

  it("keeps the pipeline usable after a handler error", async () => {
    let handlerCalls = 0;
    const values: string[] = [];
    const errors: unknown[] = [];

    service.initBatch<string>(
      "test",
      (items): Observable<string[]> => {
        handlerCalls++;

        if (handlerCalls === 1) {
          return throwError(() => new Error("handler failed"));
        }

        return of(items.map((item) => `result:${item}`));
      },
      destroy$,
    );

    service.process<string>("first", "test").subscribe({
      error: (error) => errors.push(error),
    });
    await flushBatch();

    service.process<string>("second", "test").subscribe((value) => values.push(value));
    await flushBatch();

    expect(errors).toHaveLength(1);
    expect(values).toEqual(["result:second"]);
  });

  it("errors consumers when the handler does not return an array", async () => {
    const errors: Error[] = [];

    service.initBatch<string>("test", () => of("not-an-array" as any), destroy$);

    service.process<string>("a", "test").subscribe({
      error: (error) => errors.push(error),
    });

    await flushBatch();

    expect(errors).toHaveLength(1);
    expect(errors[0].message).toContain("Batch handler did not return an array");
  });

  it("throws when processing an unknown pipeline", () => {
    expect(() => service.process("item", "unknown")).toThrow(
      "Pipeline not found: unknown",
    );
  });
});
