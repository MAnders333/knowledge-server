import {
	DEFAULT_CONFIG,
	defaultConfigPath,
	loadConfigFile,
} from "./config-file.js";

/**
 * The user_id that writers (daemon, process-inbox) stamp on episodes and that
 * scoped reads (reinitialize --reset-state) match. Resolution is the same chain
 * StoreRegistry applies to config.userId: KNOWLEDGE_USER_ID → config.jsonc
 * userId → hostname → "default".
 */
export function currentUserId(configPath = defaultConfigPath()): string {
	return (loadConfigFile(configPath) ?? DEFAULT_CONFIG).userId;
}
