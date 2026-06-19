import { QueueService } from "./queue.service";
import { Subject } from "rxjs";

describe("QueueService (safe mode)", () => {
  let service: QueueService;

  beforeEach(() => {
    service = new QueueService();
  });

  it("should create state", () => {
    const queue$ = service.create<string[]>([]);
    expect(queue$.value).toEqual([]);
  });

  it("should emit value from observable", (done) => {
    const queue$ = service.create<string[]>([]);
    const task$ = new Subject<string>();

    service.queue(queue$, "k1", task$).subscribe({
      next: (v) => {
        expect(v).toBe("a");
      },
      complete: () => done(),
      error: done.fail,
    });

    task$.next("a");
    task$.complete();
  });

  it("should update queue state on execution", (done) => {
    const queue$ = service.create<string[]>([]);
    const task$ = new Subject<string>();

    let sawActive = false;

    queue$.subscribe((v) => {
      if (v.includes("k1")) sawActive = true;
    });

    service.queue(queue$, "k1", task$).subscribe({
      complete: () => {
        expect(sawActive).toBe(true);
        done();
      },
    });

    task$.next("ok");
    task$.complete();
  });
});
