import {
  Observable,
  Subject,
  combineLatest,
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

import { PseudoCacheService } from "./pseudonymization-cache.service";
import { Base64 } from "js-base64";
import { DateTime } from "luxon";
import { arrayChunks } from "../generators/array-chunks.generator";
import { PseudoBatchService } from "./pseudonymization-batch.service";
import { QueueService } from "./queue.service";
import { PseudoConfig } from "../interfaces/pseudo-config.interface";

/**
 * High-level pseudonymization service built on top of
 * PseudonymisationHelper.
 *
 * Features:
 * - Single value pseudonymization and identification
 * - Batch processing with automatic grouping
 * - In-memory caching with configurable TTL
 * - Byte array support
 * - Request deduplication through queues
 *
 * All operations return RxJS observables.
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

  private readonly pipelines = new Map<string, Subject<string>>();

  private readonly identifyQueue$ = this.queueService.create<string[]>([]);
  private readonly pseudonymizeQueue$ = this.queueService.create<string[]>([]);

  private readonly pseudonymisationDomain: Domain;

  private readonly destroy$ = new Subject<void>();

  /**
   * Creates a new pseudonymization service instance.
   *
   * @param pseudonymisationHelper Helper used to communicate with the
   * pseudonymization infrastructure.
   * @param configuration Service configuration.
   *
   * @throws Error When required configuration values are missing.
   */
  constructor(
    pseudonymisationHelper: PseudonymisationHelper,
    configuration: PseudoConfig,
  ) {
    this.validateConstructorParams(pseudonymisationHelper, configuration);

    this.pseudonymisationHelper = pseudonymisationHelper;
    this.pseudoCacheService = new PseudoCacheService(configuration?.cache);

    this.pseudonymisationDomain = this.pseudonymisationHelper.createDomain(
      configuration?.domain!,
      <Curve>configuration?.curve!,
      configuration?.audience!,
      configuration?.bufferSize!,
    );

    this.pipelines.set(
      "identify",
      this.batchService.initBatch<Value>(
        "identify",
        (items) => this.fromAsn1CompressedMultipleValues(items),
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
        (items) => this.byteArrayFromAsn1CompressedMultipleValues(items),
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
   * Validates constructor dependencies and configuration.
   *
   * @param pseudonymisationHelper Pseudonymization helper instance.
   * @param configuration Service configuration.
   *
   * @throws Error When mandatory parameters are missing.
   */
  private validateConstructorParams(
    pseudonymisationHelper: PseudonymisationHelper,
    configuration: PseudoConfig,
  ) {
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
   * Pseudonymizes a string value.
   *
   * Results may be returned from cache when available.
   *
   * @param str Plain value to pseudonymize.
   * @returns Observable emitting the ASN.1 compressed pseudonym.
   */
  toAsn1Compressed(str: string): Observable<string> {
    return this.queueService.queue(
      this.pseudonymizeQueue$,
      str,
      of(this.pseudoCacheService.getPseudonym(str)).pipe(
        switchMap((cached) =>
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
                    str,
                    p,
                    this.expiresIn(p),
                  ),
                ),
                map((p) => p.asShortString()),
              ),
        ),
      ),
    );
  }

  /**
   * Identifies an ASN.1 compressed pseudonym and returns its original value.
   *
   * Results may be returned from cache when available.
   *
   * @param str ASN.1 compressed pseudonym.
   * @returns Observable emitting the original value.
   */
  fromAsn1Compressed(str: string): Observable<string> {
    return this.queueService.queue(
      this.identifyQueue$,
      str,
      of(this.pseudoCacheService.getValue(str)).pipe(
        switchMap((cached) =>
          cached
            ? of(cached.asString())
            : this.toPseudonymInTransit(str).pipe(
                switchMap((psd) =>
                  this.batchService
                    .process<Value>(str, "identify")
                    .pipe(map((p) => [p, this.expiresIn(psd)] as const)),
                ),
                tap(([p, e]) =>
                  this.pseudoCacheService.cacheValue(str, p, e ?? undefined),
                ),
                map(([p, e]) => p.asString()),
              ),
        ),
      ),
    );
  }

  /**
   * Pseudonymizes a byte array.
   *
   * @param byteArray Binary value to pseudonymize.
   * @returns Observable emitting the ASN.1 compressed pseudonym.
   */
  byteArraytoAsn1Compressed(byteArray: Uint8Array): Observable<string> {
    const byteArrayAsString = Base64.fromUint8Array(byteArray);

    return this.queueService.queue(
      this.pseudonymizeQueue$,
      byteArrayAsString,
      of(this.pseudoCacheService.getPseudonym(byteArrayAsString)).pipe(
        switchMap((cached) =>
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
                    byteArrayAsString,
                    p,
                    this.expiresIn(p),
                  ),
                ),
                map((p) => p.asShortString()),
              ),
        ),
      ),
    );
  }

  /**
   * Identifies an ASN.1 compressed pseudonym and returns its original
   * byte array representation.
   *
   * @param str ASN.1 compressed pseudonym.
   * @returns Observable emitting the decoded byte array.
   */
  byteArrayFromAsn1Compressed(str: string): Observable<Uint8Array> {
    return this.queueService.queue(
      this.identifyQueue$,
      str,
      of(this.pseudoCacheService.getValue(str)).pipe(
        switchMap((cached) =>
          cached
            ? of(cached.asBytes())
            : this.toPseudonymInTransit(str).pipe(
                switchMap((psd) =>
                  combineLatest([
                    this.batchService.process<Value>(str, "byteArrayIdentify"),
                    of(this.expiresIn(psd)),
                  ]),
                ),
                tap(([p, e]) => this.pseudoCacheService.cacheValue(str, p, e)),
                map(([p, e]) => p.asBytes()),
              ),
        ),
      ),
    );
  }

  /**
   * Checks whether a pseudonym has expired according to its transit
   * information metadata.
   *
   * @param str ASN.1 compressed pseudonym.
   * @returns Observable emitting true when expired.
   */
  asn1CompressedHasExpired(str: string) {
    return this.toPseudonymInTransit(str).pipe(
      map((p) => {
        const expiresAt = p.transitInfo?.headers?.get("exp")
          ? Number(p.transitInfo?.headers?.get("exp")) * 1000
          : 0;
        return expiresAt <= DateTime.now().toMillis();
      }),
    );
  }

  /**
   * Pseudonymizes multiple values in batches.
   *
   * Values are automatically chunked before being sent to the
   * underlying helper.
   *
   * @param str Values to pseudonymize.
   * @returns Observable emitting pseudonymized values.
   */
  toAsn1CompressedMultiple(str: string[]): Observable<string[]> {
    if (str.length === 0) {
      return of([]);
    }
    return this.toAsn1CompressedMultipleInTransit(str).pipe(
      map((i) => i.map((v) => v.asShortString())),
    );
  }

  /**
   * Converts a collection of values into pseudonyms using batch processing.
   *
   * Values are automatically chunked to limit request size.
   *
   * @param str Values to pseudonymize.
   * @returns Observable emitting pseudonymized transit objects.
   */
  private toAsn1CompressedMultipleInTransit(
    str: string[],
  ): Observable<PseudonymInTransit[]> {
    if (str.length === 0) {
      return of([]);
    }
    const slices = [...arrayChunks(str, 10)];
    return forkJoin([
      ...slices.map((slice) =>
        this.toValues(slice).pipe(
          switchMap((val) => this.pseudonymizeMultiple(val)),
          map((p) => this.getValues(p)),
          map((p) =>
            p.map((i) => {
              if (i instanceof EHealthProblem) {
                throw new TypeError(i.title, { cause: i.detail });
              }
              return i;
            }),
          ),
        ),
      ),
    ]).pipe(map((r) => r?.flat() || []));
  }

  /**
   * Identifies multiple pseudonyms in batches.
   *
   * @param str ASN.1 compressed pseudonyms.
   * @returns Observable emitting original values.
   */
  fromAsn1CompressedMultiple(str: string[]): Observable<string[]> {
    if (str.length === 0) {
      return of([]);
    }
    return this.fromAsn1CompressedMultipleValues(str).pipe(
      map((i) => i.map((v) => v.asString())),
    );
  }

  /**
   * Converts a collection of pseudonyms back to values using batch
   * processing.
   *
   * Pseudonyms are automatically chunked to limit request size.
   *
   * @param str ASN.1 compressed pseudonyms.
   * @returns Observable emitting resolved values.
   */
  private fromAsn1CompressedMultipleValues(str: string[]): Observable<Value[]> {
    if (str.length === 0) {
      return of([]);
    }
    const slices = [...arrayChunks(str, 10)];
    return forkJoin([
      ...slices.map((slice) =>
        this.toPseudonymsInTransit(slice).pipe(
          switchMap((psd) => this.identifyMultiple(psd)),
          map((p) => this.getPoints(p)),
          map((p) =>
            p.map((i) => {
              if (i instanceof EHealthProblem) {
                throw new TypeError(i.title, { cause: i.detail });
              }
              return i;
            }),
          ),
        ),
      ),
    ]).pipe(map((r) => r?.flat() || []));
  }

  /**
   * Pseudonymizes multiple byte arrays in batches.
   *
   * @param byteArrays Values to pseudonymize.
   * @returns Observable emitting pseudonymized values.
   */
  byteArraytoAsn1CompressedMultiple(
    byteArrays: Uint8Array[],
  ): Observable<string[]> {
    return this.byteArraytoAsn1CompressedMultipleInTransit(byteArrays).pipe(
      map((i) => i.map((v) => v.asShortString())),
    );
  }

  /**
   * Pseudonymizes multiple byte arrays using transit objects.
   *
   * Internal batch implementation used by public byte array APIs.
   *
   * @param byteArrays Values to pseudonymize.
   * @returns Observable emitting transit pseudonyms.
   */
  private byteArraytoAsn1CompressedMultipleInTransit(
    byteArrays: Uint8Array[],
  ): Observable<PseudonymInTransit[]> {
    if (byteArrays.length === 0) {
      return of([]);
    }
    const slices = [...arrayChunks(byteArrays, 10)];
    return forkJoin([
      ...slices.map((slice) =>
        this.byteArraysToValues(slice).pipe(
          switchMap((val) => this.pseudonymizeMultiple(val)),
          map((p) => this.getValues(p)),
          map((p) =>
            p.map((i) => {
              if (i instanceof EHealthProblem) {
                throw new TypeError(i.title, { cause: i.detail });
              }
              return i;
            }),
          ),
        ),
      ),
    ]).pipe(map((r) => r?.flat() || []));
  }

  /**
   * Converts Base64 encoded byte arrays back to transit pseudonyms.
   *
   * @param strings Base64 encoded values.
   * @returns Observable emitting transit pseudonyms.
   */
  private byteArrayAsStringtoAsn1CompressedMultipleInTransit(
    strings: string[],
  ): Observable<PseudonymInTransit[]> {
    const byteArrays = strings.map((str) => Base64.toUint8Array(str));
    return this.byteArraytoAsn1CompressedMultipleInTransit(byteArrays);
  }

  /**
   * Identifies multiple pseudonyms and returns their original
   * byte array representations.
   *
   * @param str ASN.1 compressed pseudonyms.
   * @returns Observable emitting decoded byte arrays.
   */
  byteArrayFromAsn1CompressedMultiple(str: string[]): Observable<Uint8Array[]> {
    return this.byteArrayFromAsn1CompressedMultipleValues(str).pipe(
      map((i) => i.map((v) => v.asBytes())),
    );
  }

  private byteArrayFromAsn1CompressedMultipleValues(
    str: string[],
  ): Observable<Value[]> {
    if (str.length === 0) {
      return of([]);
    }
    const slices = [...arrayChunks(str, 10)];
    return forkJoin([
      ...slices.map((slice) =>
        this.toPseudonymsInTransit(slice).pipe(
          switchMap((psd) => this.identifyMultiple(psd)),
          map((p) => this.getPoints(p)),
          map((p) =>
            p.map((i) => {
              if (i instanceof EHealthProblem) {
                throw new TypeError(i.title, { cause: i.detail });
              }
              return i;
            }),
          ),
        ),
      ),
    ]).pipe(map((r) => r?.flat() || []));
  }

  /**
   * Extracts all pseudonym points from a batch response.
   *
   * @param p Batch pseudonym response.
   * @returns Flat array of pseudonym points.
   */
  private getValues(p: MultiplePseudonymInTransit) {
    const points: Array<PseudonymInTransit | EHealthProblem> = [];
    for (let index = 0; index < p.lengthPoints(); index++) {
      points.push(p.getPoint(index));
    }
    return points;
  }

  /**
   * Extracts all value points from a batch response.
   *
   * @param p Batch value response.
   * @returns Flat array of value points.
   */
  private getPoints(p: MultipleValue) {
    const points: Array<Value | EHealthProblem> = [];
    for (let index = 0; index < p.lengthPoints(); index++) {
      points.push(p.getPoint(index));
    }
    return points;
  }

  /**
   * Pseudonymizes a single value.
   *
   * @param val Value to pseudonymize.
   * @returns Observable emitting a pseudonym.
   */
  private pseudonymize(val: Value): Observable<PseudonymInTransit> {
    return from(val.pseudonymize()).pipe(
      map((r) => {
        if (r instanceof EHealthProblem) {
          throw new TypeError(r.title, { cause: r.detail });
        }
        return r;
      }),
    );
  }

  /**
   * Identifies a single pseudonym.
   *
   * @param pseudonym Pseudonym to identify.
   * @returns Observable emitting the original value.
   */
  private identify(pseudonym: PseudonymInTransit): Observable<Value> {
    return from(pseudonym.identify()).pipe(
      map((r) => {
        if (r instanceof EHealthProblem) {
          throw new TypeError(r.title, { cause: r.detail });
        }
        return r;
      }),
    );
  }

  /**
   * Pseudonymizes multiple values in a single helper call.
   *
   * @param values Values to pseudonymize.
   * @returns Observable emitting pseudonymized values.
   */
  private pseudonymizeMultiple(
    values: MultipleValue,
  ): Observable<MultiplePseudonymInTransit> {
    return from(values.pseudonymize()).pipe(
      map((r) => {
        if (r instanceof EHealthProblem) {
          throw new TypeError(r.title, { cause: r.detail });
        }
        return r;
      }),
    );
  }

  /**
   * Identifies multiple pseudonyms in a single helper call.
   *
   * @param pseudonyms Pseudonyms to identify.
   * @returns Observable emitting resolved values.
   */
  private identifyMultiple(
    pseudonyms: MultiplePseudonymInTransit,
  ): Observable<MultipleValue> {
    return from(pseudonyms.identify()).pipe(
      map((r) => {
        if (r instanceof EHealthProblem) {
          throw new TypeError(r.title, { cause: r.detail });
        }
        return r;
      }),
    );
  }

  /**
   * Creates a Value instance from a string.
   *
   * @param str Input value.
   * @returns Observable emitting the created Value.
   */
  private toValue(str: string): Observable<Value> {
    return of(this.pseudonymisationDomain.valueFactory.fromString(str));
  }

  /**
   * Creates a batch Value collection from strings.
   *
   * @param str Input values.
   * @returns Observable emitting a MultipleValue.
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
   * Creates a Value instance from a byte array.
   *
   * @param str Binary value.
   * @returns Observable emitting the created Value.
   */
  private byteArrayToValue(str: Uint8Array): Observable<Value> {
    return of(this.pseudonymisationDomain.valueFactory.fromArray(str));
  }
  /**
   * Creates a batch Value collection from byte arrays.
   *
   * @param str Binary values.
   * @returns Observable emitting a MultipleValue.
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
   * Converts an ASN.1 compressed value into a transit pseudonym instance.
   *
   * @param asn1Compressed ASN.1 compressed pseudonym.
   * @returns Observable emitting a transit pseudonym.
   *
   * @throws Error When the provided value is empty.
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
   * Converts multiple ASN.1 compressed values into a batch pseudonym object.
   *
   * @param asn1Compressed ASN.1 compressed pseudonyms.
   * @returns Observable emitting a MultiplePseudonymInTransit.
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

  /**
   * Computes the cache TTL from pseudonym expiration information.
   *
   * Expiration data is extracted from the transit headers when available,
   * otherwise from the token payload.
   *
   * @param pseudonym Pseudonym to inspect.
   * @returns Remaining cache duration in milliseconds or undefined.
   */
  private expiresIn(pseudonym: PseudonymInTransit): number | undefined {
    try {
      let expiresAt: number | undefined;

      const expHeader = pseudonym?.transitInfo?.headers?.get("exp");

      if (expHeader == null) {
        const token = pseudonym.transitInfo.asString();
        const payload = token.split(".")[0];

        if (!payload) {
          return undefined;
        }

        expiresAt = JSON.parse(Base64.fromBase64(payload))?.exp;
      } else {
        expiresAt = Number(expHeader);
      }

      if (!expiresAt || Number.isNaN(expiresAt)) {
        return undefined;
      }

      return Math.max(0, expiresAt * 1000 - DateTime.now().toMillis() - 30000);
    } catch {
      return undefined;
    }
  }

  /**
   * Stops all internal processing pipelines and releases resources.
   *
   * Should be called when the service is no longer needed.
   */
  onDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
    this.pipelines.clear();
  }
}
