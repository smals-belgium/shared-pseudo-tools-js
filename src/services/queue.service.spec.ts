import { QueueService } from "./queue.service";
import { of, firstValueFrom } from "rxjs";

describe("QueueService", () => {
  let service: QueueService;

  beforeEach(() => {
    service = new QueueService();
  });

  it("should create a queue", () => {
    const queue$ = service.create<string[]>([]);
    expect(queue$.value).toEqual([]);
  });

  it("should process a simple queued request", async () => {
    const queue$ = service.create<string[]>([]);

    const result$ = service.queue(queue$, "key1", of("result1"));

    const result = await firstValueFrom(result$);

    expect(result).toBe("result1");
  });

  it("should ensure key is removed after finalize", async () => {
    const queue$ = service.create<string[]>([]);

    await firstValueFrom(service.queue(queue$, "key1", of("value")));

    expect(queue$.value).not.toContain("key1");
  });

  it("should not duplicate execution for same key", async () => {
    const queue$ = service.create<string[]>([]);

    let count = 0;

    const handle = of("done").pipe(() => {
      count++;
      return of("done");
    });

    const obs1 = service.queue(queue$, "key1", handle);
    const obs2 = service.queue(queue$, "key1", handle);

    const results = await Promise.all([
      firstValueFrom(obs1),
      firstValueFrom(obs2),
    ]);

    expect(results).toEqual(["done", "done"]);
    expect(count).toBe(1);
  });

  it("should add and remove key properly", async () => {
    const queue$ = service.create<string[]>([]);

    const obs = service.queue(queue$, "key1", of("done"));

    await firstValueFrom(obs);
    expect(queue$.value).not.toContain("key1");
  });
});
