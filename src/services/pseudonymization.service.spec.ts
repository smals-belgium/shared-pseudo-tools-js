import { firstValueFrom } from "rxjs";
import { Base64 } from "js-base64";
import { PseudoConfig } from "../interfaces/pseudo-config.interface";
import { PseudoService } from "./pseudonymization.service";

type RawValue = string | Uint8Array;

class MockValue {
  constructor(private readonly raw: RawValue) {}

  asString(): string {
    return typeof this.raw === "string"
      ? this.raw
      : Base64.fromUint8Array(this.raw);
  }

  asBytes(): Uint8Array {
    return this.raw instanceof Uint8Array
      ? this.raw
      : Base64.toUint8Array(this.raw);
  }

  getRaw(): RawValue {
    return this.raw;
  }
}

class MockPseudonymInTransit {
  readonly transitInfo: {
    headers: Map<string, string>;
    asString: () => string;
  };

  constructor(
    private readonly raw: RawValue,
    private readonly shortString: string,
    expiresAtSeconds = Math.floor(Date.now() / 1000) + 3_600,
  ) {
    const encodedHeader = Base64.toBase64(
      JSON.stringify({ exp: expiresAtSeconds }),
    );

    this.transitInfo = {
      headers: new Map([["exp", String(expiresAtSeconds)]]),
      asString: () => `${encodedHeader}.payload`,
    };
  }

  asShortString(): string {
    return this.shortString;
  }

  identify(): Promise<MockValue> {
    return Promise.resolve(new MockValue(this.raw));
  }

  getRaw(): RawValue {
    return this.raw;
  }
}

class MockMultipleValue {
  private readonly points: MockValue[] = [];

  constructor(points: MockValue[] = []) {
    points.forEach((point) => this.pushPoint(point));
  }

  pushPoint(point: MockValue): void {
    this.points.push(point);
  }

  lengthPoints(): number {
    return this.points.length;
  }

  getPoint(index: number): MockValue {
    return this.points[index];
  }

  pseudonymize(): Promise<MockMultiplePseudonymInTransit> {
    return Promise.resolve(
      new MockMultiplePseudonymInTransit(
        this.points.map((point) => pseudonymFromRaw(point.getRaw())),
      ),
    );
  }
}

class MockMultiplePseudonymInTransit {
  private readonly points: MockPseudonymInTransit[] = [];

  constructor(points: MockPseudonymInTransit[] = []) {
    points.forEach((point) => this.pushPoint(point));
  }

  pushPoint(point: MockPseudonymInTransit): void {
    this.points.push(point);
  }

  lengthPoints(): number {
    return this.points.length;
  }

  getPoint(index: number): MockPseudonymInTransit {
    return this.points[index];
  }

  identify(): Promise<MockMultipleValue> {
    return Promise.resolve(
      new MockMultipleValue(
        this.points.map((point) => new MockValue(point.getRaw())),
      ),
    );
  }
}

const pseudonymFromRaw = (raw: RawValue): MockPseudonymInTransit => {
  if (raw instanceof Uint8Array) {
    const encoded = Base64.fromUint8Array(raw);
    return new MockPseudonymInTransit(raw, `psdBytes:${encoded}`);
  }

  return new MockPseudonymInTransit(raw, `psd:${raw}`);
};

const pseudonymFromString = (value: string): MockPseudonymInTransit => {
  if (value.startsWith("psdBytes:")) {
    return new MockPseudonymInTransit(
      Base64.toUint8Array(value.replace("psdBytes:", "")),
      value,
    );
  }

  if (value.startsWith("expired:")) {
    return new MockPseudonymInTransit(
      value.replace("expired:", ""),
      value,
      Math.floor(Date.now() / 1000) - 60,
    );
  }

  if (value.startsWith("psd:")) {
    return new MockPseudonymInTransit(value.replace("psd:", ""), value);
  }

  return new MockPseudonymInTransit(value, value);
};

const createMockDomain = () => ({
  valueFactory: {
    fromString: (value: string) => new MockValue(value),
    fromArray: (value: Uint8Array) => new MockValue(value),
    multiple: () => new MockMultipleValue(),
  },
  pseudonymInTransitFactory: {
    fromSec1AndTransitInfo: (value: string) => pseudonymFromString(value),
    multiple: () => new MockMultiplePseudonymInTransit(),
  },
});

const createConfig = (): PseudoConfig =>
  ({
    domain: "domain",
    curve: "curve",
    audience: "audience",
    bufferSize: 100,
    cache: {
      values: { ttl: 1_000 },
      pseudonyms: { ttl: 1_000 },
    },
  }) as PseudoConfig;

const createService = () => {
  const domain = createMockDomain();
  const helper = {
    createDomain: jest.fn().mockReturnValue(domain),
  };

  return {
    helper,
    service: new PseudoService(helper as any, createConfig()),
  };
};

const flushBatch = async (milliseconds = 300): Promise<void> => {
  jest.advanceTimersByTime(milliseconds);
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
};

describe("PseudoService", () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("creates the pseudonymization domain from the provided configuration", () => {
    const { helper } = createService();

    expect(helper.createDomain).toHaveBeenCalledWith(
      "domain",
      "curve",
      "audience",
      100,
    );
  });

  it("rejects missing helper and invalid configuration", () => {
    expect(() => new PseudoService(undefined as any, createConfig())).toThrow(
      "PseudonymisationHelper must be provided",
    );

    expect(
      () =>
        new PseudoService(
          { createDomain: () => createMockDomain() } as any,
          {},
        ),
    ).toThrow("Invalid pseudo configuration");
  });

  it("pseudonymizes a string", async () => {
    const { service } = createService();

    const result = firstValueFrom(service.toAsn1Compressed("alice"));
    await flushBatch();

    await expect(result).resolves.toBe("psd:alice");
  });

  it("identifies a string pseudonym", async () => {
    const { service } = createService();

    const result = firstValueFrom(service.fromAsn1Compressed("psd:bob"));
    await flushBatch();

    await expect(result).resolves.toBe("bob");
  });

  it("pseudonymizes and identifies byte arrays", async () => {
    const { service } = createService();
    const bytes = new Uint8Array([1, 2, 3]);

    const pseudonymPromise = firstValueFrom(
      service.byteArraytoAsn1Compressed(bytes),
    );
    await flushBatch();
    const pseudonym = await pseudonymPromise;

    expect(pseudonym).toBe(`psdBytes:${Base64.fromUint8Array(bytes)}`);

    const identifiedPromise = firstValueFrom(
      service.byteArrayFromAsn1Compressed(pseudonym),
    );
    await flushBatch();
    const identified = await identifiedPromise;

    expect(Array.from(identified)).toEqual([1, 2, 3]);
  });

  it("handles empty batch inputs", async () => {
    const { service } = createService();

    await expect(
      firstValueFrom(service.toAsn1CompressedMultiple([])),
    ).resolves.toEqual([]);
    await expect(
      firstValueFrom(service.fromAsn1CompressedMultiple([])),
    ).resolves.toEqual([]);
    await expect(
      firstValueFrom(service.byteArraytoAsn1CompressedMultiple([])),
    ).resolves.toEqual([]);
    await expect(
      firstValueFrom(service.byteArrayFromAsn1CompressedMultiple([])),
    ).resolves.toEqual([]);
  });

  it("pseudonymizes and identifies string batches", async () => {
    const { service } = createService();

    await expect(
      firstValueFrom(service.toAsn1CompressedMultiple(["a", "b"])),
    ).resolves.toEqual(["psd:a", "psd:b"]);

    await expect(
      firstValueFrom(service.fromAsn1CompressedMultiple(["psd:a", "psd:b"])),
    ).resolves.toEqual(["a", "b"]);
  });

  it("detects expired and non-expired pseudonyms", async () => {
    const { service } = createService();

    await expect(
      firstValueFrom(service.asn1CompressedHasExpired("psd:active")),
    ).resolves.toBe(false);
    await expect(
      firstValueFrom(service.asn1CompressedHasExpired("expired:old")),
    ).resolves.toBe(true);
  });

  it("uses cache for repeated string pseudonymization", async () => {
    const { service } = createService();

    const first = firstValueFrom(service.toAsn1Compressed("cached"));
    await flushBatch();
    await expect(first).resolves.toBe("psd:cached");

    const second = firstValueFrom(service.toAsn1Compressed("cached"));
    await Promise.resolve();

    await expect(second).resolves.toBe("psd:cached");
  });

  it("completes internal pipelines on destroy", () => {
    const { service } = createService();

    expect(() => service.onDestroy()).not.toThrow();
  });
});
