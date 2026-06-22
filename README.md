# @smals-belgium/shared-pseudo-tools-js

![npm](https://img.shields.io/npm/v/%40smals-belgium/shared-pseudo-tools-js)
![license](https://img.shields.io/npm/l/%40smals-belgium/shared-pseudo-tools-js)
![build](https://img.shields.io/github/actions/workflow/status/smals-belgium/shared-pseudo-tools-js/nodejs.yml?branch=master)

Framework-agnostic reactive pseudonymization toolkit built on top of `@smals-belgium-shared/pseudo-helper`.

The library exposes a small RxJS-based API for pseudonymization and identification, with built-in batching, in-flight request deduplication, TTL caching and `Uint8Array` support.

---

## Features

- String pseudonymization and identification
- Batch pseudonymization and batch identification
- Binary data support through `Uint8Array`
- Automatic batching of concurrent requests
- In-flight deduplication for identical keys
- TTL-based in-memory caching
- Expiration check for ASN.1 compressed pseudonyms
- Framework-agnostic implementation, usable from Angular, Node-compatible build tools, or any TypeScript project using RxJS

---

## Installation

```bash
npm install @smals-belgium/shared-pseudo-tools-js
```

Install the underlying pseudonymization helper expected by the library:

```bash
npm install @smals-belgium-shared/pseudo-helper
```

The package uses RxJS observables. Make sure your consuming project has a compatible RxJS setup.

---

## Quick start

```ts
import { PseudoService } from "@smals-belgium/shared-pseudo-tools-js";
import { PseudonymisationHelper } from "@smals-belgium-shared/pseudo-helper";

const helper = new PseudonymisationHelper("https://pseudo.example.be");

const pseudoService = new PseudoService(helper, {
  domain: "patient",
  curve: "secp256k1",
  audience: "consumer",
  bufferSize: 100,
  cache: {
    values: { ttl: 300_000 },
    pseudonyms: { ttl: 300_000 },
  },
});

pseudoService.toAsn1Compressed("12345678901").subscribe((pseudonym) => {
  console.log(pseudonym);
});
```

---

## Configuration

```ts
import type { TTLCacheOptions } from "@isaacs/ttlcache";
import type {
  PseudonymInTransit,
  Value,
} from "@smals-belgium-shared/pseudo-helper";

export interface PseudoConfig {
  endpoint?: string;
  domain?: string;
  curve?: string;
  audience?: string;
  bufferSize?: number;
  cache?: {
    values?: TTLCacheOptions<string, Value>;
    pseudonyms?: TTLCacheOptions<string, PseudonymInTransit>;
  };
}
```

### Required values

The constructor validates that the following values are present:

- `domain`
- `curve`
- `audience`
- `bufferSize`

`endpoint` is kept in the configuration interface for compatibility, but the current service expects the `PseudonymisationHelper` instance to be provided by the caller.

### Cache TTL

The cache uses milliseconds.

```ts
const config = {
  domain: "patient",
  curve: "secp256k1",
  audience: "consumer",
  bufferSize: 100,
  cache: {
    values: { ttl: 300_000 },
    pseudonyms: { ttl: 300_000 },
  },
};
```

If no TTL is configured, the internal default is `10_000 ms`.

When a generated pseudonym contains an expiration timestamp, the service uses that expiration to derive a safer per-entry TTL and subtracts a small safety margin before storing it.

---

## API reference

All public methods return RxJS `Observable`s.

### String API

#### `toAsn1Compressed(value: string): Observable<string>`

Pseudonymizes a string value and emits its ASN.1 compressed representation.

```ts
pseudoService.toAsn1Compressed("12345678901").subscribe(console.log);
```

#### `fromAsn1Compressed(pseudonym: string): Observable<string>`

Identifies a pseudonym and emits the original string value.

```ts
pseudoService.fromAsn1Compressed(pseudonym).subscribe(console.log);
```

#### `toAsn1CompressedMultiple(values: string[]): Observable<string[]>`

Pseudonymizes multiple string values.

```ts
pseudoService
  .toAsn1CompressedMultiple(["123", "456", "789"])
  .subscribe(console.log);
```

#### `fromAsn1CompressedMultiple(pseudonyms: string[]): Observable<string[]>`

Identifies multiple pseudonyms.

```ts
pseudoService.fromAsn1CompressedMultiple([p1, p2]).subscribe(console.log);
```

Empty arrays are supported and emit an empty array.

---

### Binary API

#### `byteArraytoAsn1Compressed(value: Uint8Array): Observable<string>`

Pseudonymizes binary content.

```ts
const bytes = new Uint8Array([1, 2, 3]);

pseudoService.byteArraytoAsn1Compressed(bytes).subscribe(console.log);
```

#### `byteArrayFromAsn1Compressed(pseudonym: string): Observable<Uint8Array>`

Identifies a pseudonym and emits the original binary content.

```ts
pseudoService.byteArrayFromAsn1Compressed(pseudonym).subscribe(console.log);
```

#### `byteArraytoAsn1CompressedMultiple(values: Uint8Array[]): Observable<string[]>`

Pseudonymizes multiple byte arrays.

#### `byteArrayFromAsn1CompressedMultiple(pseudonyms: string[]): Observable<Uint8Array[]>`

Identifies multiple binary pseudonyms and emits the original byte arrays.

---

### Utilities

#### `asn1CompressedHasExpired(pseudonym: string): Observable<boolean>`

Checks whether a pseudonym has expired.

```ts
pseudoService.asn1CompressedHasExpired(pseudonym).subscribe(console.log);
```

#### `onDestroy(): void`

Stops the internal batch pipelines.

```ts
pseudoService.onDestroy();
```

Do not reuse a `PseudoService` instance after calling `onDestroy()`.

---

## Angular integration

The library itself has no Angular dependency. In Angular applications, wrap it in an injectable service.

```ts
import { inject, Injectable, OnDestroy } from "@angular/core";
import { PseudonymisationHelper } from "@smals-belgium-shared/pseudo-helper";
import { ConfigurationService } from "@smals/ngx-configuration-service";
import { PseudoService as BasePseudoService } from "@smals-belgium/shared-pseudo-tools-js";

@Injectable({
  providedIn: "root",
})
export class PseudoService extends BasePseudoService implements OnDestroy {
  constructor() {
    super(
      inject(PseudonymisationHelper),
      inject(ConfigurationService).getEnvironmentVariable("pseudo"),
    );
  }

  ngOnDestroy(): void {
    super.onDestroy();
  }
}
```

### Local library testing in Angular

When testing a locally packed version of the library in an Angular application, clear Angular's cache after reinstalling the `.tgz` package:

```bash
npx ng cache clean
```

Then restart `ng serve` completely.

---

## Error handling

The underlying helper can return `EHealthProblem` objects. The library converts these values into standard JavaScript `Error` instances with the original detail stored as `cause` when available.

Batch calls also error explicitly when a batch handler does not return a result for every requested item. This prevents consumers from receiving observables that complete silently without a value.

---

## Internal behavior

### Batching

The internal batch service buffers requests for up to `300 ms` or until `10` unique items are collected, whichever comes first.

```ts
bufferTime(300, undefined, 10);
```

### Deduplication

Identical in-flight requests share the same pending subject. Once the batch is resolved, the pending subject is removed so a later call can start a fresh operation if the value is no longer available from the TTL cache.

### Caching

The service keeps two TTL caches:

- `value -> pseudonym`
- `pseudonym -> value`

String values and byte-array values use separate internal cache namespaces to avoid collisions between plain strings and Base64-encoded binary data.

---

## Development

```bash
npm install
npm test
npm run test:coverage
npm run build
```

The test suite uses Jest and is framework-agnostic. Keep fake timers local to tests that need them, such as batching or queue timing tests. Avoid enabling fake timers globally for TTL cache tests.

---

## Additional documentation

- [Architecture](./ARCHITECTURE.MD)
- [Changelog](./CHANGELOG.md)
- [NPM guide](./NPM.md)

---

## License

[Smals Web Components and Libraries (SWCL)](./LICENSE.md)

Copyright © Smals
