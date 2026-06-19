import { PseudonymInTransit, Value } from "@smals-belgium-shared/pseudo-helper";
import { TTLCache } from "@isaacs/ttlcache";
import { PseudoConfig } from "../interfaces/pseudo-config.interface";

/**
 * Provides TTL-based caching for pseudonyms and identified values.
 *
 * Two independent caches are maintained:
 * - Value cache: maps pseudonyms to identified values.
 * - Pseudonym cache: maps clear values to pseudonyms.
 *
 * Cache expiration can be configured globally through {@link PseudoConfig}
 * or overridden per entry.
 */
export class PseudoCacheService {
  private readonly config: PseudoConfig["cache"];

  private readonly DEFAULT_TTL_MS = 10000;

  private readonly pseudonymCache: TTLCache<string, PseudonymInTransit>;
  private readonly valueCache: TTLCache<string, Value>;

  /**
   * Creates a new cache service instance.
   *
   * @param configuration Cache configuration options.
   */
  constructor(configuration: PseudoConfig["cache"]) {
    this.config = configuration;

    this.pseudonymCache = new TTLCache<string, PseudonymInTransit>(
      this.config?.pseudonyms ?? {},
    );

    this.valueCache = new TTLCache<string, Value>({
      checkAgeOnGet: true,
      ...this.config?.values,
    });
  }

  /**
   * Resolves the effective TTL for a cache entry.
   *
   * Priority order:
   * 1. Entry-specific TTL
   * 2. Cache configuration TTL
   * 3. Default TTL
   *
   * @param cacheTTL TTL provided for the current entry.
   * @param configTTL TTL configured for the cache.
   * @returns TTL value in milliseconds.
   */
  private resolveTtl(
    cacheTTL: number | undefined,
    configTTL: number | undefined,
  ): number {
    return cacheTTL ?? configTTL ?? this.DEFAULT_TTL_MS;
  }

  /**
   * Stores an identified value associated with a pseudonym.
   *
   * @param pseudonym ASN.1 compressed pseudonym.
   * @param value Identified value.
   * @param cacheTTL Optional entry-specific TTL in milliseconds.
   */
  cacheValue(pseudonym: string, value: Value, cacheTTL?: number): void {
    const ttl = this.resolveTtl(cacheTTL, this.config?.values?.ttl);

    this.valueCache.set(pseudonym, value, { ttl });
  }

  /**
   * Retrieves a cached value from its pseudonym.
   *
   * @param pseudonym ASN.1 compressed pseudonym.
   * @returns The cached value if available and not expired.
   */
  getValue(pseudonym: string): Value | undefined {
    return this.valueCache.get(pseudonym);
  }

  /**
   * Stores a pseudonym associated with a clear value.
   *
   * @param value Original value.
   * @param pseudonym Generated pseudonym.
   * @param cacheTTL Optional entry-specific TTL in milliseconds.
   */
  cachePseudonym(
    value: string,
    pseudonym: PseudonymInTransit,
    cacheTTL?: number,
  ): void {
    const ttl = this.resolveTtl(cacheTTL, this.config?.pseudonyms?.ttl);

    this.pseudonymCache.set(value, pseudonym, { ttl });
  }

  /**
   * Retrieves a cached pseudonym from its original value.
   *
   * @param value Original value.
   * @returns The cached pseudonym if available and not expired.
   */
  getPseudonym(value: string): PseudonymInTransit | undefined {
    return this.pseudonymCache.get(value);
  }
}
