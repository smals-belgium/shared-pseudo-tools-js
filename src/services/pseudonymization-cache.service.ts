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
  private static readonly DEFAULT_TTL_MS = 10_000;

  private readonly config: PseudoConfig["cache"];

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
   * Resolution order:
   * 1. Entry-specific TTL
   * 2. Cache-level configured TTL
   * 3. Default TTL
   *
   * @param entryTTL TTL provided for the current cache entry.
   * @param cacheTTL TTL configured for the cache instance.
   * @returns The effective TTL in milliseconds.
   */
  private resolveTtl(
    entryTTL: number | undefined,
    cacheTTL: number | undefined,
  ): number {
    return entryTTL ?? cacheTTL ?? PseudoCacheService.DEFAULT_TTL_MS;
  }

  /**
   * Stores a value in the specified cache using the resolved TTL.
   *
   * @typeParam K Cache key type.
   * @typeParam V Cache value type.
   * @param cache Target cache instance.
   * @param key Cache key.
   * @param value Value to store.
   * @param entryTTL Optional entry-specific TTL.
   * @param cacheTTL Cache-level TTL configuration.
   */
  private setWithTtl<K, V>(
    cache: TTLCache<K, V>,
    key: K,
    value: V,
    entryTTL: number | undefined,
    cacheTTL: number | undefined,
  ): void {
    cache.set(key, value, {
      ttl: this.resolveTtl(entryTTL, cacheTTL),
    });
  }

  /**
   * Stores an identified value associated with a pseudonym.
   *
   * @param pseudonym ASN.1 compressed pseudonym.
   * @param value Identified value.
   * @param cacheTTL Optional entry-specific TTL in milliseconds.
   */
  cacheValue(pseudonym: string, value: Value, cacheTTL?: number): void {
    this.setWithTtl(
      this.valueCache,
      pseudonym,
      value,
      cacheTTL,
      this.config?.values?.ttl,
    );
  }

  /**
   * Retrieves a cached value from its pseudonym.
   *
   * @param pseudonym ASN.1 compressed pseudonym.
   * @returns The cached value if present and not expired; otherwise undefined.
   */
  getValue(pseudonym: string): Value | undefined {
    return this.valueCache.get(pseudonym);
  }

  /**
   * Stores a pseudonym associated with a clear value.
   *
   * @param value Original clear value.
   * @param pseudonym Generated pseudonym.
   * @param cacheTTL Optional entry-specific TTL in milliseconds.
   */
  cachePseudonym(
    value: string,
    pseudonym: PseudonymInTransit,
    cacheTTL?: number,
  ): void {
    this.setWithTtl(
      this.pseudonymCache,
      value,
      pseudonym,
      cacheTTL,
      this.config?.pseudonyms?.ttl,
    );
  }

  /**
   * Retrieves a cached pseudonym from its original value.
   *
   * @param value Original clear value.
   * @returns The cached pseudonym if present and not expired; otherwise undefined.
   */
  getPseudonym(value: string): PseudonymInTransit | undefined {
    return this.pseudonymCache.get(value);
  }
}
