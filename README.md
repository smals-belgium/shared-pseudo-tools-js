# @smals-belgium/shared-pseudo-tools-js

![npm](https://img.shields.io/npm/v/%40smals-belgium/shared-pseudo-tools-js)
![license](https://img.shields.io/npm/l/%40smals-belgium/shared-pseudo-tools-js)
![build](https://img.shields.io/github/actions/workflow/status/smals-belgium/shared-pseudo-tools-js/nodejs.yml?branch=master)

Reactive pseudonymization toolkit built on top of `@smals-belgium-shared/pseudo-helper`.

This library provides:

- Simple pseudonymization / identification APIs
- Batch processing
- Automatic request aggregation
- In-memory caching with TTL
- Queue management to avoid duplicate concurrent requests
- RxJS-based reactive API

---

# Features

## Pseudonymization

Convert a value into a pseudonym.

```ts
pseudoService.toAsn1Compressed("12345678901");
```

## Identification

Convert a pseudonym back into its original value.

```ts
pseudoService.fromAsn1Compressed(pseudonym);
```

## Batch Processing

Multiple requests are automatically grouped into batches.

```ts
pseudoService.toAsn1CompressedMultiple(["123", "456", "789"]);
```

## Byte Array Support

Pseudonymize binary content.

```ts
pseudoService.byteArraytoAsn1Compressed(bytes);
```

Restore original binary content.

```ts
pseudoService.byteArrayFromAsn1Compressed(pseudonym);
```

## Smart Caching

The library automatically caches:

- Value → Pseudonym
- Pseudonym → Value

using TTL-based storage.

## Request Deduplication

Multiple simultaneous requests for the same value only trigger a single underlying pseudonymization operation.

## Automatic Batching

Requests received within a configurable time window are automatically aggregated into a single batch request.

---

# Installation

```bash
npm install @smals-belgium/shared-pseudo-tools-js
```

---

# Peer Dependency

This package relies on:

```bash
npm install @smals-belgium-shared/pseudo-helper
```

---

# Quick Start

## Import

```ts
import { PseudoService } from "@smals-belgium/shared-pseudo-tools-js";

import { PseudonymisationHelper } from "@smals-belgium-shared/pseudo-helper";
```

## Create Helper

```ts
const helper = new PseudonymisationHelper("https://my-pseudonymization-server");
```

## Create Service

```ts
const service = new PseudoService(helper, {
  domain: "patient",
  curve: "secp256k1",
  audience: "consumer",
  bufferSize: 100,
});
```

---

# Configuration

```ts
interface PseudoConfig {
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

## Configuration Example

```ts
const config = {
  domain: "patient",

  curve: "secp256k1",

  audience: "consumer",

  bufferSize: 100,

  cache: {
    values: {
      ttl: 300000,
    },

    pseudonyms: {
      ttl: 300000,
    },
  },
};
```

---

# API Reference

## PseudoService

### toAsn1Compressed

Pseudonymizes a string value.

```ts
toAsn1Compressed(
  value: string
): Observable<string>
```

Example:

```ts
service.toAsn1Compressed("12345678901").subscribe((pseudonym) => {
  console.log(pseudonym);
});
```

---

### fromAsn1Compressed

Restores the original value.

```ts
fromAsn1Compressed(
  pseudonym: string
): Observable<string>
```

Example:

```ts
service.fromAsn1Compressed(pseudonym).subscribe((value) => {
  console.log(value);
});
```

---

### toAsn1CompressedMultiple

Pseudonymizes multiple values.

```ts
toAsn1CompressedMultiple(
  values: string[]
): Observable<string[]>
```

Example:

```ts
service.toAsn1CompressedMultiple(["123", "456", "789"]).subscribe(console.log);
```

---

### fromAsn1CompressedMultiple

Restores multiple values.

```ts
fromAsn1CompressedMultiple(
  pseudonyms: string[]
): Observable<string[]>
```

---

### byteArraytoAsn1Compressed

Pseudonymizes binary data.

```ts
byteArraytoAsn1Compressed(
  value: Uint8Array
): Observable<string>
```

Example:

```ts
const bytes = new Uint8Array([1, 2, 3]);

service.byteArraytoAsn1Compressed(bytes).subscribe(console.log);
```

---

### byteArrayFromAsn1Compressed

Restores binary data.

```ts
byteArrayFromAsn1Compressed(
  pseudonym: string
): Observable<Uint8Array>
```

---

### byteArraytoAsn1CompressedMultiple

Pseudonymizes multiple byte arrays.

```ts
byteArraytoAsn1CompressedMultiple(
  values: Uint8Array[]
): Observable<string[]>
```

---

### byteArrayFromAsn1CompressedMultiple

Restores multiple byte arrays.

```ts
byteArrayFromAsn1CompressedMultiple(
  pseudonyms: string[]
): Observable<Uint8Array[]>
```

---

### asn1CompressedHasExpired

Checks whether a pseudonym has expired.

```ts
asn1CompressedHasExpired(
  pseudonym: string
): Observable<boolean>
```

Example:

```ts
service.asn1CompressedHasExpired(token).subscribe(console.log);
```

---

### onDestroy

Stops all internal streams and pipelines.

```ts
service.onDestroy();
```

---

## Complete Example

```ts
import { PseudoService } from "@smals-belgium/shared-pseudo-tools-js";

import { PseudonymisationHelper } from "@smals-belgium-shared/pseudo-helper";

const helper = new PseudonymisationHelper("https://pseudo.example.be");

const service = new PseudoService(helper, {
  domain: "patient",
  curve: "secp256k1",
  audience: "consumer",
  bufferSize: 100,
});

service.toAsn1Compressed("12345678901").subscribe((pseudonym) => {
  console.log(pseudonym);
});
```

---

## Additional Documentation

This library includes detailed architecture documentation describing:

- Request batching
- Request deduplication
- Queue management
- Cache management
- Internal processing flows
- Sequence diagrams

See:

- [ARCHITECTURE.md](./ARCHITECTURE.md)

---

# Internal Architecture

The library is built around three optimization layers.

## QueueService

Avoids duplicate concurrent requests.

Example:

```
Request A -> patient123
Request B -> patient123
Request C -> patient123
```

Only one processing flow is executed.

All subscribers receive the same result.

---

## PseudoBatchService

Aggregates incoming requests.

Internally:

```ts
bufferTime(300);
```

is used to group requests.

Example:

```
patient1
patient2
patient3
```

becomes:

```
[
  "patient1",
  "patient2",
  "patient3"
]
```

before sending a batch request.

Maximum batch size:

```ts
50;
```

---

## PseudoCacheService

Provides two caches.

### Pseudonym Cache

Stores:

```
value -> pseudonym
```

### Value Cache

Stores:

```
pseudonym -> value
```

Implementation:

```ts
@isaacs/ttlcache
```

Default TTL:

```ts
10000 ms
```

---

# Request Lifecycle

## Pseudonymization Flow

```text
Client
  |
  v
PseudoService
  |
  +--> Cache Lookup
  |
  +--> Queue Service
  |
  +--> Batch Service
  |
  +--> PseudonymisationHelper
  |
  +--> Cache Result
  |
  v
Observable Result
```

---

## Identification Flow

```text
Client
  |
  v
PseudoService
  |
  +--> Cache Lookup
  |
  +--> Queue Service
  |
  +--> Batch Service
  |
  +--> PseudonymisationHelper
  |
  +--> Cache Result
  |
  v
Observable Result
```

---

# Error Handling

Errors returned by the underlying pseudonymization infrastructure are transformed into JavaScript errors.

Example:

```ts
TypeError;
```

containing:

```ts
title;
detail;
```

from the underlying `EHealthProblem`.

---

# Performance Characteristics

Built-in optimizations:

- Request deduplication
- Automatic batching
- TTL caching
- Reactive processing with RxJS
- Reduced network usage
- Lower server load
- Improved throughput

---

# Development

## Build

```bash
npm run build
```

## Test

```bash
npm test
```

## Coverage

```bash
npm run test:coverage
```

---

## Documentation

- 📖 [Architecture](./ARCHITECTURE.md)
- 📝 [Changelog](./CHANGELOG.md)

---

# License

[Smals Web Components and Libraries (SWCL)](./LICENSE.md)

Copyright © Smals
