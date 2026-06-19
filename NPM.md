# @smals-belgium/shared-pseudo-tools-js

Reactive pseudonymization toolkit built on top of `@smals-belgium-shared/pseudo-helper`.

It provides a high-level API for pseudonymization with built-in optimizations:

- request batching
- automatic deduplication
- in-memory TTL caching
- RxJS-based reactive API
- binary (Uint8Array) support

---

## Installation

```bash
npm install @smals-belgium/shared-pseudo-tools-js
```

### Peer dependency

```bash
npm install @smals-belgium-shared/pseudo-helper
```

---

## Quick start

### Create helper

```ts
import { PseudonymisationHelper } from "@smals-belgium-shared/pseudo-helper";
import { PseudoService } from "@smals-belgium/shared-pseudo-tools-js";

const helper = new PseudonymisationHelper("https://your-endpoint");
```

### Create service

```ts
const service = new PseudoService(helper, {
  domain: "patient",
  curve: "secp256k1",
  audience: "consumer",
  bufferSize: 100,
});
```

---

## Basic usage

### Pseudonymize a value

```ts
service.toAsn1Compressed("12345678901").subscribe(console.log);
```

### Restore original value

```ts
service.fromAsn1Compressed(pseudonym).subscribe(console.log);
```

---

## Batch operations

```ts
service.toAsn1CompressedMultiple(["123", "456", "789"]).subscribe(console.log);

service.fromAsn1CompressedMultiple(["p1", "p2"]).subscribe(console.log);
```

---

## Binary support

```ts
const bytes = new Uint8Array([1, 2, 3]);

service.byteArraytoAsn1Compressed(bytes).subscribe(console.log);

service.byteArrayFromAsn1Compressed(pseudonym).subscribe(console.log);
```

---

## Configuration

```ts
interface PseudoConfig {
  domain?: string;
  curve?: string;
  audience?: string;
  bufferSize?: number;
  cache?: {
    values?: { ttl: number };
    pseudonyms?: { ttl: number };
  };
}
```

### Example

```ts
const config = {
  domain: "patient",
  curve: "secp256k1",
  audience: "consumer",
  bufferSize: 100,
  cache: {
    values: { ttl: 300000 },
    pseudonyms: { ttl: 300000 },
  },
};
```

---

## Angular integration

### Class extension

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

### Standalone provider

```ts
import { Provider, inject } from "@angular/core";
import { PseudonymisationHelper } from "@smals-belgium-shared/pseudo-helper";
import { ConfigurationService } from "@smals/ngx-configuration-service";
import { PseudoService } from "@smals-belgium/shared-pseudo-tools-js";

export const PSEUDO_SERVICE_PROVIDER: Provider = {
  provide: PseudoService,
  useFactory: () =>
    new PseudoService(
      inject(PseudonymisationHelper),
      inject(ConfigurationService).getEnvironmentVariable("pseudo"),
    ),
};
```

---

## API overview

All methods return `Observable`.

### String API

- `toAsn1Compressed(value: string): Observable<string>`
- `fromAsn1Compressed(pseudonym: string): Observable<string>`
- `toAsn1CompressedMultiple(values: string[]): Observable<string[]>`
- `fromAsn1CompressedMultiple(values: string[]): Observable<string[]>`

### Binary API

- `byteArraytoAsn1Compressed(value: Uint8Array): Observable<string>`
- `byteArrayFromAsn1Compressed(pseudonym: string): Observable<Uint8Array>`
- `byteArraytoAsn1CompressedMultiple(values: Uint8Array[]): Observable<string[]>`
- `byteArrayFromAsn1CompressedMultiple(values: Uint8Array[]): Observable<Uint8Array[]>`

### Utilities

- `asn1CompressedHasExpired(pseudonym: string): Observable<boolean>`
- `onDestroy(): void`

---

## Features

- batching of requests
- automatic deduplication
- TTL-based caching
- RxJS reactive streams
- binary support
- optimized backend usage

---

## License

Smals Web Components and Libraries (SWCL)
