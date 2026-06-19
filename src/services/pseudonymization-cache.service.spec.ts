import { PseudoCacheService } from "./pseudonymization-cache.service";

class MockValue {
  constructor(private readonly v: string) {}
  asString() {
    return this.v;
  }
  asBytes() {
    return new Uint8Array();
  }
}

class MockPseudonym {
  constructor(private readonly v: string) {}
  asShortString() {
    return this.v;
  }
}

describe("PseudoCacheService", () => {
  let service: PseudoCacheService;

  beforeEach(() => {
    service = new PseudoCacheService({
      values: { ttl: 1000 },
      pseudonyms: { ttl: 1000 },
    } as any);
  });

  it("should cache and retrieve value", () => {
    const value = new MockValue("v1") as any;

    service.cacheValue("key", value);

    const res = service.getValue("key");

    expect(res).toBeDefined();
    expect(res?.asString()).toBe("v1");
  });

  it("should cache and retrieve pseudonym", () => {
    const pseudo = new MockPseudonym("p1") as any;

    service.cachePseudonym("key", pseudo);

    const res = service.getPseudonym("key");

    expect(res).toBeDefined();
    expect(res?.asShortString()).toBe("p1");
  });

  it("should resolve TTL fallback correctly", () => {
    service.cacheValue("k", new MockValue("x") as any);
    const res = service.getValue("k");

    expect(res).toBeDefined();
  });

  it("should return undefined when not found", () => {
    expect(service.getValue("missing")).toBeUndefined();
    expect(service.getPseudonym("missing")).toBeUndefined();
  });
});
