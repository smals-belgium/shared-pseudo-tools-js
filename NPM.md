# NPM package guide

This document describes how to build, test, package and consume `@smals-belgium/shared-pseudo-tools-js`.

---

## Package purpose

`@smals-belgium/shared-pseudo-tools-js` is a framework-agnostic TypeScript/RxJS wrapper around `@smals-belgium-shared/pseudo-helper`.

It provides:

- pseudonymization and identification APIs
- automatic batching
- in-flight request deduplication
- TTL caching
- `Uint8Array` support
- Jest-tested framework-agnostic services

---

## Install from the registry

```bash
npm install @smals-belgium/shared-pseudo-tools-js
```

Install the underlying helper expected by the package:

```bash
npm install @smals-belgium-shared/pseudo-helper
```

---

## Local development workflow

### Install dependencies

```bash
npm install
```

### Run tests

```bash
npm test
```

### Run coverage

```bash
npm run test:coverage
```

### Build

```bash
npm run build
```

---

## Local package testing with `npm pack`

Use `npm pack` to test the exact package contents that would be published.

```bash
npm run build
npm pack
```

This creates an archive such as:

```text
smals-belgium-shared-pseudo-tools-js-0.0.1.tgz
```

Install it in a consuming project:

```bash
npm install ../path/to/smals-belgium-shared-pseudo-tools-js-0.0.1.tgz
```

### Angular consuming applications

When testing a local `.tgz` package in Angular, stop the dev server, reinstall the package, clear Angular's cache and restart the dev server:

```bash
npm install ../path/to/smals-belgium-shared-pseudo-tools-js-0.0.1.tgz
npx ng cache clean
npm start
```

If a change appears to have no effect, verify that the compiled file inside `node_modules/@smals-belgium/shared-pseudo-tools-js` contains the expected change, then clear Angular's cache again.

---

## Pre-publish checklist

Before publishing a new version:

```bash
npm test
npm run test:coverage
npm run build
npm pack --dry-run
```

Check that the package contains only the expected build artifacts and documentation.

Recommended checks:

- no debug `console.warn` / `console.error`
- no framework-specific imports in the library services
- no Angular dependency in framework-agnostic code
- tests pass with Jest
- README examples still match the public API
- `CHANGELOG.md` contains the release notes
- package version has been bumped if publishing a new version

---

## Publishing

Use your configured Smals/npm registry and access policy.

```bash
npm publish
```

For scoped packages, use the appropriate access level required by your registry setup.

```bash
npm publish --access restricted
```

or, if the package is intentionally public:

```bash
npm publish --access public
```

---

## Recommended dependency model

The package is framework-agnostic. Angular should not be a dependency of this library.

The consuming application must provide the `PseudonymisationHelper` instance passed to `PseudoService`.

Recommended package expectations:

- `@smals-belgium-shared/pseudo-helper` must be available to the consumer
- RxJS must be compatible with the library build
- Angular-specific integration should live in the consuming application, not inside this package

---

## Minimal usage

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

service.toAsn1Compressed("12345678901").subscribe(console.log);
```

---

## Troubleshooting

### Changes are visible in `node_modules` but not at runtime

In Angular consuming projects, the dependency optimizer/cache can serve a previous build. Stop the dev server and run:

```bash
npx ng cache clean
```

Then restart the application.

### TTL cache tests are flaky

Do not enable Jest fake timers globally. Keep fake timers local to specs that test RxJS timing. TTL cache tests should either mock `@isaacs/ttlcache` or avoid testing the internals of the external dependency.

### Observables stay pending in a consuming application

Check that the package version actually loaded by the app is the rebuilt version. Then verify that the batch service does not keep completed observables in a persistent internal cache. The stable implementation deduplicates only in-flight subjects and removes them after dispatch.
