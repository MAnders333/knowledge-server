/**
 * Database module entry point.
 *
 * Re-exports interfaces and implementations.
 * Use StoreRegistry (src/db/store-registry.ts) to create DB instances —
 * it reads store configuration from config.jsonc and initializes all stores.
 */

export type { IServerStateDB, IKnowledgeStore } from "./interface.js";
