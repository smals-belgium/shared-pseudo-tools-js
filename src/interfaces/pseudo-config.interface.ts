import { TTLCacheOptions } from "@isaacs/ttlcache";
import { PseudonymInTransit, Value } from "@smals-belgium-shared/pseudo-helper";

export interface PseudoConfig {
  endpoint?: string;
  domain?: string;
  curve?: string;
  audience?: string;
  bufferSize?: number;
  cache?: {
    values?: TTLCacheOptions<string, Value>;
    pseudonyms: TTLCacheOptions<string, PseudonymInTransit>;
  };
}
