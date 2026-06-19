import { PseudoBatchService } from "./pseudonymization-batch.service";
import { Subject, of, firstValueFrom } from "rxjs";

describe("PseudoBatchService", () => {
  let service: PseudoBatchService;

  beforeEach(() => {
    service = new PseudoBatchService();
  });

  it("should batch and process items", async () => {
    const destroy$ = new Subject<void>();

    const handler = jest.fn((items: string[]) =>
      of(items.map((i) => `processed-${i}`)),
    );

    const queue$ = service.initBatch("test", handler, destroy$);

    const p1 = firstValueFrom(service.process("a", "test"));
    const p2 = firstValueFrom(service.process("b", "test"));

    // 🔥 FORCE FLUSH (clé magique)
    queue$.complete();

    const results = await Promise.all([p1, p2]);

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(["a", "b"]);
    expect(results).toEqual(["processed-a", "processed-b"]);
  });

  it("should reuse cached observable", () => {
    const destroy$ = new Subject<void>();

    const handler = jest.fn((items: string[]) =>
      of(items.map((i) => `x-${i}`)),
    );

    service.initBatch("test", handler, destroy$);

    const a1 = service.process("a", "test");
    const a2 = service.process("a", "test");

    expect(a1).toBe(a2);
  });

  it("should throw if pipeline not found", () => {
    expect(() => service.process("a", "unknown")).toThrow(
      "Pipeline not found: unknown",
    );
  });

  it("should deduplicate items in batch", async () => {
    const destroy$ = new Subject<void>();

    const handler = jest.fn((items: string[]) =>
      of(items.map((i) => i.toUpperCase())),
    );

    const queue$ = service.initBatch("test", handler, destroy$);

    const p1 = firstValueFrom(service.process("a", "test"));
    const p2 = firstValueFrom(service.process("a", "test"));
    const p3 = firstValueFrom(service.process("a", "test"));

    queue$.complete();

    const results = await Promise.all([p1, p2, p3]);

    expect(handler).toHaveBeenCalledWith(["a"]);
    expect(results).toEqual(["A", "A", "A"]);
  });
});
