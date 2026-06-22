type CacheSetOptions = { ttl?: number };

type CacheEntry<K, V> = {
  key: K;
  value: V;
  options?: CacheSetOptions;
};

const mockTTLCacheInstances: MockTTLCache<any, any>[] = [];

class MockTTLCache<K, V> {
  readonly options: Record<string, unknown> | undefined;
  readonly setCalls: Array<CacheEntry<K, V>> = [];

  private readonly store = new Map<K, V>();

  constructor(options?: Record<string, unknown>) {
    this.options = options;
    mockTTLCacheInstances.push(this as MockTTLCache<any, any>);
  }

  set(key: K, value: V, options?: CacheSetOptions): this {
    this.setCalls.push({ key, value, options });

    if ((options?.ttl ?? 1) > 0) {
      this.store.set(key, value);
    }

    return this;
  }

  get(key: K): V | undefined {
    return this.store.get(key);
  }
}

jest.mock("@isaacs/ttlcache", () => ({
  TTLCache: MockTTLCache,
}));

import { PseudoCacheService } from "./pseudonymization-cache.service";

describe("PseudoCacheService", () => {
  const createService = () =>
    new PseudoCacheService({
      values: { ttl: 1_000 },
      pseudonyms: { ttl: 2_000 },
    } as any);

  const value = { asString: () => "identified" } as any;
  const pseudonym = { asShortString: () => "pseudo" } as any;

  beforeEach(() => {
    mockTTLCacheInstances.length = 0;
  });

  const getValueCache = (): MockTTLCache<string, any> =>
    mockTTLCacheInstances[1];
  const getPseudonymCache = (): MockTTLCache<string, any> =>
    mockTTLCacheInstances[0];

  it("configures both caches to check age on get", () => {
    createService();

    expect(getPseudonymCache().options).toMatchObject({
      checkAgeOnGet: true,
      ttl: 2_000,
    });
    expect(getValueCache().options).toMatchObject({
      checkAgeOnGet: true,
      ttl: 1_000,
    });
  });

  it("caches and retrieves identified values", () => {
    const service = createService();

    service.cacheValue("pseudo", value);

    expect(service.getValue("pseudo")).toBe(value);
  });

  it("caches and retrieves pseudonyms", () => {
    const service = createService();

    service.cachePseudonym("clear", pseudonym);

    expect(service.getPseudonym("clear")).toBe(pseudonym);
  });

  it("uses the entry-specific value TTL before the configured value TTL", () => {
    const service = createService();

    service.cacheValue("pseudo", value, 5);

    expect(getValueCache().setCalls).toContainEqual({
      key: "pseudo",
      value,
      options: { ttl: 5 },
    });
  });

  it("uses the entry-specific pseudonym TTL before the configured pseudonym TTL", () => {
    const service = createService();

    service.cachePseudonym("clear", pseudonym, 5);

    expect(getPseudonymCache().setCalls).toContainEqual({
      key: "clear",
      value: pseudonym,
      options: { ttl: 5 },
    });
  });

  it("uses the configured value TTL when no entry-specific value TTL is provided", () => {
    const service = createService();

    service.cacheValue("pseudo", value);

    expect(getValueCache().setCalls).toContainEqual({
      key: "pseudo",
      value,
      options: { ttl: 1_000 },
    });
  });

  it("uses the configured pseudonym TTL when no entry-specific pseudonym TTL is provided", () => {
    const service = createService();

    service.cachePseudonym("clear", pseudonym);

    expect(getPseudonymCache().setCalls).toContainEqual({
      key: "clear",
      value: pseudonym,
      options: { ttl: 2_000 },
    });
  });

  it("does not store values when the effective TTL is zero", () => {
    const service = createService();

    service.cacheValue("pseudo", value, 0);
    service.cachePseudonym("clear", pseudonym, 0);

    expect(getValueCache().setCalls).toEqual([]);
    expect(getPseudonymCache().setCalls).toEqual([]);
    expect(service.getValue("pseudo")).toBeUndefined();
    expect(service.getPseudonym("clear")).toBeUndefined();
  });

  it("falls back to the default TTL when no cache configuration is provided", () => {
    const service = new PseudoCacheService(undefined);

    service.cacheValue("pseudo", value);
    service.cachePseudonym("clear", pseudonym);

    expect(getPseudonymCache().setCalls).toContainEqual({
      key: "clear",
      value: pseudonym,
      options: { ttl: 10_000 },
    });
    expect(getValueCache().setCalls).toContainEqual({
      key: "pseudo",
      value,
      options: { ttl: 10_000 },
    });
  });
});
