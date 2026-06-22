# Changelog

All notable changes to this package should be documented in this file.

The format follows the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project uses semantic versioning where applicable.

---

## [Unreleased]

### Added

- Added English JSDoc documentation to public and internal services.
- Added framework-agnostic Jest tests for cache, queue, batch and pseudonymization services.
- Added documentation for Angular wrapper integration while keeping the library itself framework-agnostic.
- Added troubleshooting notes for Angular cache issues when testing locally packed builds.

### Changed

- Updated README, NPM and architecture documentation to match the current implementation.
- Documented the current batch buffering strategy: `bufferTime(300, undefined, 10)`.
- Clarified that in-flight batch deduplication is based on pending subjects, not a persistent observable cache.
- Clarified cache behavior, default TTL and byte-array cache key separation.

---

## [0.0.1] - 2026-06-22

### Added

- Initial `PseudoService` public API.
- String pseudonymization and identification.
- Batch string pseudonymization and identification.
- `Uint8Array` pseudonymization and identification.
- `asn1CompressedHasExpired()` utility.
- `QueueService` for per-key request serialization.
- `PseudoBatchService` for automatic batching.
- `PseudoCacheService` for TTL-based in-memory caching.
- Initial README, NPM and architecture documentation.

### Fixed

- Avoided persistent reuse of already-completed batch observables.
- Ensured missing batch results surface as explicit errors instead of silent completions.
- Ensured empty batch inputs emit `[]` instead of completing without a value.
- Made cache TTL resolution safer for entry-specific and configured TTL values.
- Avoided collisions between string values and Base64-encoded byte-array cache keys.
