import {
  Observable,
  Subject,
  combineLatest,
  defer,
  exhaustMap,
  forkJoin,
  from,
  map,
  of,
  switchMap,
  tap,
} from "rxjs";
import {
  PseudonymisationHelper,
  Domain,
  EHealthProblem,
  PseudonymInTransit,
  Value,
  MultiplePseudonymInTransit,
  MultipleValue,
  Curve,
} from "@smals-belgium-shared/pseudo-helper";
import { Base64 } from "js-base64";
import { DateTime } from "luxon";
import { PseudoConfig } from "../interfaces/pseudo-config.interface";
import { PseudoCacheService } from "./pseudonymization-cache.service";
import { QueueService } from "./queue.service";
import { PseudoBatchService } from "./pseudonymization-batch.service";
import { arrayChunks } from "../generators/array-chunks.generator";

/**
 * High-level facade around the Smals pseudonymisation helper.
 *
 * The service exposes single-value and batch helpers for string and byte-array
 * values. Internally, calls are serialized per key, grouped into short-lived
 * batches and cached according to the configured TTL policy.
 */
export class PseudoService {
  /** Handles cache storage for values and pseudonyms. */
  private readonly pseudoCacheService: PseudoCacheService;

  /** Serializes requests for identical keys. */
  private readonly queueService = new QueueService();

  /** Handles request batching. */
  private readonly batchService = new PseudoBatchService();

  /** Underlying pseudonymization helper implementation. */
  private readonly pseudonymisationHelper: PseudonymisationHelper;

  /** Keeps references to initialized batching pipelines. */
  private readonly pipelines = new Map<string, Subject<string>>();

  /** Serializes identification requests by pseudonym key. */
  private readonly identifyQueue$ = this.queueService.create<string[]>([]);

  /** Serializes pseudonymization requests by original value key. */
  private readonly pseudonymizeQueue$ = this.queueService.create<string[]>([]);

  /** Domain created from the provided pseudonymization configuration. */
  private readonly pseudonymisationDomain: Domain;

  /** Emits when all internal pipelines should stop. */
  private readonly destroy$ = new Subject<void>();

  /**
   * Creates a new pseudonymization service instance.
   *
   * @param pseudonymisationHelper Helper used to create the pseudonymization domain.
   * @param configuration Domain, curve, audience, buffer-size and cache settings.
   * @throws Error When the helper or mandatory configuration values are missing.
   */
  constructor(
    pseudonymisationHelper: PseudonymisationHelper,
    configuration: PseudoConfig,
  ) {
    this.validateConstructorParams(pseudonymisationHelper, configuration);

    this.pseudonymisationHelper = pseudonymisationHelper;
    this.pseudoCacheService = new PseudoCacheService(configuration?.cache);

    this.pseudonymisationDomain = this.pseudonymisationHelper.createDomain(
      configuration.domain!,
      configuration.curve! as Curve,
      configuration.audience!,
      configuration.bufferSize!,
    );

    this.pipelines.set(
      "identify",
      this.batchService.initBatch<Value>(
        "identify",
        (items) => this.identifyMultipleValues(items),
        this.destroy$,
      ),
    );

    this.pipelines.set(
      "pseudonymize",
      this.batchService.initBatch<PseudonymInTransit>(
        "pseudonymize",
        (items) => this.toAsn1CompressedMultipleInTransit(items),
        this.destroy$,
      ),
    );

    this.pipelines.set(
      "byteArrayIdentify",
      this.batchService.initBatch<Value>(
        "byteArrayIdentify",
        // byteArrayFromAsn1CompressedMultipleValues was identical to
        // fromAsn1CompressedMultipleValues — reuse the shared implementation.
        (items) => this.identifyMultipleValues(items),
        this.destroy$,
      ),
    );

    this.pipelines.set(
      "byteArrayPseudonymize",
      this.batchService.initBatch<PseudonymInTransit>(
        "byteArrayPseudonymize",
        (items) =>
          this.byteArrayAsStringtoAsn1CompressedMultipleInTransit(items),
        this.destroy$,
      ),
    );
  }

  /**
   * Pseudonymizes a string value and returns the ASN.1 compressed short string.
   *
   * Calls for the same value are serialized and cached. The cache key is scoped
   * to string values to avoid collisions with byte-array values encoded as
   * Base64 strings.
   *
   * @param str Clear string value to pseudonymize.
   * @returns Observable emitting the ASN.1 compressed pseudonym.
   */
  toAsn1Compressed(str: string): Observable<string> {
    const cacheKey = this.stringPseudonymCacheKey(str);

    return this.queueService.queue(
      this.pseudonymizeQueue$,
      cacheKey,
      defer(() =>
        of(this.pseudoCacheService.getPseudonym(cacheKey)).pipe(
          exhaustMap((cached) =>
            cached
              ? of(cached.asShortString())
              : this.toValue(str).pipe(
                  switchMap(() =>
                    this.batchService.process<PseudonymInTransit>(
                      str,
                      "pseudonymize",
                    ),
                  ),
                  tap((p) =>
                    this.pseudoCacheService.cachePseudonym(
                      cacheKey,
                      p,
                      this.expiresIn(p),
                    ),
                  ),
                  map((p) => p.asShortString()),
                ),
          ),
        ),
      ),
    );
  }

  /**
   * Identifies an ASN.1 compressed pseudonym and returns the original string.
   *
   * @param str ASN.1 compressed pseudonym.
   * @returns Observable emitting the identified string value.
   */
  fromAsn1Compressed(str: string): Observable<string> {
    return this.queueService.queue(
      this.identifyQueue$,
      str,
      defer(() =>
        of(this.pseudoCacheService.getValue(str)).pipe(
          exhaustMap((cached) =>
            cached
              ? of(cached.asString())
              : this.toPseudonymInTransit(str).pipe(
                  switchMap((psd) =>
                    combineLatest([
                      this.batchService.process<Value>(str, "identify"),
                      of(this.expiresIn(psd)),
                    ]),
                  ),
                  tap(([p, e]) =>
                    this.pseudoCacheService.cacheValue(str, p, e),
                  ),
                  map(([p]) => p.asString()),
                ),
          ),
        ),
      ),
    );
  }

  /**
   * Pseudonymizes a byte array and returns the ASN.1 compressed short string.
   *
   * @param byteArray Clear byte array to pseudonymize.
   * @returns Observable emitting the ASN.1 compressed pseudonym.
   */
  byteArraytoAsn1Compressed(byteArray: Uint8Array): Observable<string> {
    const byteArrayAsString = Base64.fromUint8Array(byteArray);
    const cacheKey = this.byteArrayPseudonymCacheKey(byteArrayAsString);

    return this.queueService.queue(
      this.pseudonymizeQueue$,
      cacheKey,
      defer(() =>
        of(this.pseudoCacheService.getPseudonym(cacheKey)).pipe(
          exhaustMap((cached) =>
            cached
              ? of(cached.asShortString())
              : this.byteArrayToValue(byteArray).pipe(
                  switchMap(() =>
                    this.batchService.process<PseudonymInTransit>(
                      byteArrayAsString,
                      "byteArrayPseudonymize",
                    ),
                  ),
                  tap((p) =>
                    this.pseudoCacheService.cachePseudonym(
                      cacheKey,
                      p,
                      this.expiresIn(p),
                    ),
                  ),
                  map((p) => p.asShortString()),
                ),
          ),
        ),
      ),
    );
  }

  /**
   * Identifies an ASN.1 compressed byte-array pseudonym.
   *
   * @param str ASN.1 compressed pseudonym.
   * @returns Observable emitting the identified byte array.
   */
  byteArrayFromAsn1Compressed(str: string): Observable<Uint8Array> {
    return this.queueService.queue(
      this.identifyQueue$,
      str,
      defer(() =>
        of(this.pseudoCacheService.getValue(str)).pipe(
          exhaustMap((cached) =>
            cached
              ? of(cached.asBytes())
              : this.toPseudonymInTransit(str).pipe(
                  switchMap((psd) =>
                    combineLatest([
                      this.batchService.process<Value>(
                        str,
                        "byteArrayIdentify",
                      ),
                      of(this.expiresIn(psd)),
                    ]),
                  ),
                  tap(([p, e]) =>
                    this.pseudoCacheService.cacheValue(str, p, e),
                  ),
                  map(([p]) => p.asBytes()),
                ),
          ),
        ),
      ),
    );
  }

  /**
   * Checks whether an ASN.1 compressed pseudonym is expired.
   *
   * Pseudonyms without a readable expiration are treated as expired.
   *
   * @param str ASN.1 compressed pseudonym.
   * @returns Observable emitting true when the pseudonym has expired.
   */
  asn1CompressedHasExpired(str: string): Observable<boolean> {
    return this.toPseudonymInTransit(str).pipe(
      map((p) => {
        const expiresAt = this.getExpirationSeconds(p);

        if (expiresAt == null) {
          return true;
        }

        return expiresAt * 1000 <= DateTime.now().toMillis();
      }),
    );
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Pseudonymizes multiple string values.
   *
   * @param str Clear string values to pseudonymize.
   * @returns Observable emitting ASN.1 compressed pseudonyms in input order.
   */
  toAsn1CompressedMultiple(str: string[]): Observable<string[]> {
    return this.toAsn1CompressedMultipleInTransit(str).pipe(
      map((i) => i.map((v) => v.asShortString())),
    );
  }

  // ---------------------------------------------------------------------------
  // Private helpers — batch pseudonymization
  // ---------------------------------------------------------------------------

  /**
   * Identifies multiple ASN.1 compressed pseudonyms as strings.
   *
   * @param str ASN.1 compressed pseudonyms.
   * @returns Observable emitting identified string values in input order.
   */
  fromAsn1CompressedMultiple(str: string[]): Observable<string[]> {
    return this.fromAsn1CompressedMultipleValues(str).pipe(
      map((i) => i.map((v) => v.asString())),
    );
  }

  /**
   * Pseudonymizes multiple byte arrays.
   *
   * @param byteArrays Clear byte arrays to pseudonymize.
   * @returns Observable emitting ASN.1 compressed pseudonyms in input order.
   */
  byteArraytoAsn1CompressedMultiple(
    byteArrays: Uint8Array[],
  ): Observable<string[]> {
    return this.byteArraytoAsn1CompressedMultipleInTransit(byteArrays).pipe(
      map((i) => i.map((v) => v.asShortString())),
    );
  }

  /**
   * Identifies multiple ASN.1 compressed pseudonyms as byte arrays.
   *
   * @param str ASN.1 compressed pseudonyms.
   * @returns Observable emitting identified byte arrays in input order.
   */
  byteArrayFromAsn1CompressedMultiple(str: string[]): Observable<Uint8Array[]> {
    return this.byteArrayFromAsn1CompressedMultipleValues(str).pipe(
      map((i) => i.map((v) => v.asBytes())),
    );
  }

  /**
   * Stops all internal batching pipelines.
   *
   * Call this from the Angular wrapper's ngOnDestroy when the wrapper is
   * destroyed.
   */
  onDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  /**
   * Validates constructor dependencies and configuration.
   *
   * @param pseudonymisationHelper Pseudonymization helper instance.
   * @param configuration Service configuration.
   * @throws Error When mandatory parameters are missing.
   */
  private validateConstructorParams(
    pseudonymisationHelper: PseudonymisationHelper,
    configuration: PseudoConfig,
  ): void {
    if (!pseudonymisationHelper) {
      throw new Error("PseudonymisationHelper must be provided");
    }

    if (
      configuration?.domain == null ||
      configuration?.curve == null ||
      configuration?.audience == null ||
      configuration?.bufferSize == null
    ) {
      throw new Error("Invalid pseudo configuration");
    }
  }

  /**
   * Creates transit pseudonyms for a string batch.
   *
   * @param str Clear string values.
   * @returns Observable emitting pseudonyms in input order.
   */
  private toAsn1CompressedMultipleInTransit(
    str: string[],
  ): Observable<PseudonymInTransit[]> {
    const slices = [...arrayChunks(str, 10)];

    return this.forkJoinChunks(
      slices.map((slice) =>
        this.toValues(slice).pipe(
          switchMap((val) => this.pseudonymizeMultiple(val)),
          map((p) => this.getValues(p)),
          map((p) =>
            p.map((i) => this.throwIfEHealthProblem<PseudonymInTransit>(i)),
          ),
        ),
      ),
    );
  }

  /**
   * Identifies a batch of ASN.1 compressed pseudonyms as Value instances.
   *
   * @param str ASN.1 compressed pseudonyms.
   * @returns Observable emitting identified values in input order.
   */
  private fromAsn1CompressedMultipleValues(str: string[]): Observable<Value[]> {
    const slices = [...arrayChunks(str, 10)];

    return this.forkJoinChunks(
      slices.map((slice) =>
        this.toPseudonymsInTransit(slice).pipe(
          switchMap((psd) => this.identifyMultiple(psd)),
          map((p) => this.getPoints(p)),
          map((p) => p.map((i) => this.throwIfEHealthProblem<Value>(i))),
        ),
      ),
    );
  }

  /**
   * Creates transit pseudonyms for a byte-array batch.
   *
   * @param byteArrays Clear byte arrays.
   * @returns Observable emitting pseudonyms in input order.
   */
  private byteArraytoAsn1CompressedMultipleInTransit(
    byteArrays: Uint8Array[],
  ): Observable<PseudonymInTransit[]> {
    const slices = [...arrayChunks(byteArrays, 10)];

    return this.forkJoinChunks(
      slices.map((slice) =>
        this.byteArraysToValues(slice).pipe(
          switchMap((val) => this.pseudonymizeMultiple(val)),
          map((p) => this.getValues(p)),
          map((p) =>
            p.map((i) => this.throwIfEHealthProblem<PseudonymInTransit>(i)),
          ),
        ),
      ),
    );
  }

  /**
   * Converts Base64-encoded byte-array values back to byte arrays before batch
   * pseudonymization.
   *
   * @param strings Base64-encoded byte arrays.
   * @returns Observable emitting pseudonyms in input order.
   */
  private byteArrayAsStringtoAsn1CompressedMultipleInTransit(
    strings: string[],
  ): Observable<PseudonymInTransit[]> {
    const byteArrays = strings.map((str) => Base64.toUint8Array(str));
    return this.byteArraytoAsn1CompressedMultipleInTransit(byteArrays);
  }

  /**
   * Identifies a batch of ASN.1 compressed byte-array pseudonyms as Value instances.
   *
   * @param str ASN.1 compressed pseudonyms.
   * @returns Observable emitting identified byte-array values in input order.
   */
  private byteArrayFromAsn1CompressedMultipleValues(
    str: string[],
  ): Observable<Value[]> {
    const slices = [...arrayChunks(str, 10)];

    return this.forkJoinChunks(
      slices.map((slice) =>
        this.toPseudonymsInTransit(slice).pipe(
          switchMap((psd) => this.identifyMultiple(psd)),
          map((p) => this.getPoints(p)),
          map((p) => p.map((i) => this.throwIfEHealthProblem<Value>(i))),
        ),
      ),
    );
  }

  /**
   * Extracts all points from a multiple pseudonym response.
   *
   * @param p Multiple pseudonym response.
   * @returns Extracted points.
   */
  private getValues(
    p: MultiplePseudonymInTransit,
  ): Array<PseudonymInTransit | EHealthProblem> {
    const points: Array<PseudonymInTransit | EHealthProblem> = [];

    for (let index = 0; index < p.lengthPoints(); index++) {
      points.push(p.getPoint(index));
    }

    return points;
  }

  /**
   * Extracts all points from a multiple value response.
   *
   * @param p Multiple value response.
   * @returns Extracted points.
   */
  private getPoints(p: MultipleValue): Array<Value | EHealthProblem> {
    const points: Array<Value | EHealthProblem> = [];

    for (let index = 0; index < p.lengthPoints(); index++) {
      points.push(p.getPoint(index));
    }

    return points;
  }

  /**
   * Pseudonymizes a multiple value object.
   *
   * @param values Multiple value object.
   * @returns Observable emitting the multiple pseudonym response.
   */
  private pseudonymizeMultiple(
    values: MultipleValue,
  ): Observable<MultiplePseudonymInTransit> {
    return from(values.pseudonymize()).pipe(
      map((r) => this.throwIfEHealthProblem<MultiplePseudonymInTransit>(r)),
    );
  }

  /**
   * Identifies a multiple pseudonym object.
   *
   * @param pseudonyms Multiple pseudonym object.
   * @returns Observable emitting the multiple value response.
   */
  private identifyMultiple(
    pseudonyms: MultiplePseudonymInTransit,
  ): Observable<MultipleValue> {
    return from(pseudonyms.identify()).pipe(
      map((r) => this.throwIfEHealthProblem<MultipleValue>(r)),
    );
  }

  /**
   * Converts a string into a Value instance.
   *
   * @param str Clear string value.
   * @returns Observable emitting the Value instance.
   */
  private toValue(str: string): Observable<Value> {
    return of(this.pseudonymisationDomain.valueFactory.fromString(str));
  }

  /**
   * Converts string values into a MultipleValue instance.
   *
   * @param str Clear string values.
   * @returns Observable emitting the MultipleValue instance.
   */
  private toValues(str: string[]): Observable<MultipleValue> {
    const multipleValue = this.pseudonymisationDomain.valueFactory.multiple();
    str.forEach((i) =>
      multipleValue.pushPoint(
        this.pseudonymisationDomain.valueFactory.fromString(i),
      ),
    );

    return of(multipleValue);
  }

  /**
   * Converts a byte array into a Value instance.
   *
   * @param str Clear byte array.
   * @returns Observable emitting the Value instance.
   */
  private byteArrayToValue(str: Uint8Array): Observable<Value> {
    return of(this.pseudonymisationDomain.valueFactory.fromArray(str));
  }

  /**
   * Converts byte arrays into a MultipleValue instance.
   *
   * @param str Clear byte arrays.
   * @returns Observable emitting the MultipleValue instance.
   */
  private byteArraysToValues(str: Uint8Array[]): Observable<MultipleValue> {
    const multipleValue = this.pseudonymisationDomain.valueFactory.multiple();
    str.forEach((i) =>
      multipleValue.pushPoint(
        this.pseudonymisationDomain.valueFactory.fromArray(i),
      ),
    );
    return of(multipleValue);
  }

  /**
   * Converts an ASN.1 compressed string into a PseudonymInTransit instance.
   *
   * @param asn1Compressed ASN.1 compressed pseudonym.
   * @returns Observable emitting the PseudonymInTransit instance.
   * @throws Error When the provided pseudonym is empty.
   */
  private toPseudonymInTransit(
    asn1Compressed: string,
  ): Observable<PseudonymInTransit> {
    if (!asn1Compressed) {
      throw new Error("asn1 compressed is null !");
    }

    return of(
      this.pseudonymisationDomain.pseudonymInTransitFactory.fromSec1AndTransitInfo(
        asn1Compressed,
      ),
    );
  }

  /**
   * Converts ASN.1 compressed strings into a MultiplePseudonymInTransit instance.
   *
   * @param asn1Compressed ASN.1 compressed pseudonyms.
   * @returns Observable emitting the MultiplePseudonymInTransit instance.
   */
  private toPseudonymsInTransit(
    asn1Compressed: string[],
  ): Observable<MultiplePseudonymInTransit> {
    const multiplePseudonymInTransit =
      this.pseudonymisationDomain.pseudonymInTransitFactory.multiple();

    asn1Compressed.forEach((i) =>
      multiplePseudonymInTransit.pushPoint(
        this.pseudonymisationDomain.pseudonymInTransitFactory.fromSec1AndTransitInfo(
          i,
        ),
      ),
    );

    return of(multiplePseudonymInTransit);
  }

  // ---------------------------------------------------------------------------
  // Private helpers — TTL computation
  // ---------------------------------------------------------------------------

  /**
   * Computes the safe cache TTL from a pseudonym expiration.
   *
   * A 30-second safety margin is subtracted to avoid reusing almost-expired
   * pseudonyms.
   *
   * @param pseudonym Pseudonym carrying transit expiration metadata.
   * @returns TTL in milliseconds, or undefined when no expiration is available.
   */
  private expiresIn(pseudonym: PseudonymInTransit): number | undefined {
    const expiresAt = this.getExpirationSeconds(pseudonym);

    return expiresAt
      ? Math.max(0, expiresAt * 1000 - DateTime.now().toMillis() - 30 * 1000)
      : undefined;
  }

  /**
   * Reads the expiration timestamp from transit headers or encoded transit info.
   *
   * @param pseudonym Pseudonym carrying transit metadata.
   * @returns Expiration timestamp in seconds since epoch, or undefined.
   */
  private getExpirationSeconds(
    pseudonym: PseudonymInTransit,
  ): number | undefined {
    const headerExp = pseudonym?.transitInfo?.headers?.get("exp");

    if (headerExp != null) {
      const exp = Number(headerExp);

      if (Number.isFinite(exp)) {
        return exp;
      }
    }

    const transitInfo = pseudonym?.transitInfo?.asString();
    const encodedHeader = transitInfo?.split(".")[0];

    if (!encodedHeader) {
      return undefined;
    }

    try {
      const decoded = JSON.parse(Base64.fromBase64(encodedHeader));
      const exp = Number(decoded?.exp);

      return Number.isFinite(exp) ? exp : undefined;
    } catch {
      return undefined;
    }
  }

  /**
   * Joins chunk observables and safely handles an empty chunk list.
   *
   * @param chunks Chunk observables returning arrays.
   * @returns Observable emitting one flattened result array.
   */
  private forkJoinChunks<T>(chunks: Array<Observable<T[]>>): Observable<T[]> {
    if (!chunks.length) {
      return of([]);
    }

    return forkJoin(chunks).pipe(map((results) => results.flat()));
  }

  /**
   * Throws a JavaScript Error when the helper returns an EHealthProblem.
   *
   * A structural fallback is kept because libraries consumed from an npm
   * package can sometimes carry another copy of the pseudo-helper dependency,
   * making instanceof checks unreliable.
   *
   * @param value Value or problem returned by the helper.
   * @returns The original value when it is not a problem.
   * @throws Error When value represents an EHealthProblem.
   */
  private throwIfEHealthProblem<T>(value: T | EHealthProblem): T {
    if (this.isEHealthProblem(value)) {
      throw new Error(value.title, { cause: value.detail });
    }

    return value;
  }

  /**
   * Checks whether a value behaves like an EHealthProblem.
   *
   * @param value Unknown helper result.
   * @returns True when the value is an EHealthProblem or a compatible object.
   */
  private isEHealthProblem(value: unknown): value is EHealthProblem {
    return (
      value instanceof EHealthProblem ||
      (typeof value === "object" &&
        value !== null &&
        "title" in value &&
        "detail" in value)
    );
  }

  /**
   * Builds the pseudonym cache key for string values.
   *
   * @param value Clear string value.
   * @returns Namespaced cache key.
   */
  private stringPseudonymCacheKey(value: string): string {
    return `string:${value}`;
  }

  /**
   * Builds the pseudonym cache key for byte-array values.
   *
   * @param value Base64-encoded byte array.
   * @returns Namespaced cache key.
   */
  private byteArrayPseudonymCacheKey(value: string): string {
    return `bytes:${value}`;
  }
}
