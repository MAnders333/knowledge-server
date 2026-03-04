# Changelog

## [1.4.1](https://github.com/MAnders333/knowledge-server/compare/v1.4.0...v1.4.1) (2026-03-04)


### Bug Fixes

* update release workflow asset paths for commands/ rename ([82eefb2](https://github.com/MAnders333/knowledge-server/commit/82eefb2c3196770104a1d7229ee9e7a1128bfe78))

## [1.4.0](https://github.com/MAnders333/knowledge-server/compare/v1.3.3...v1.4.0) (2026-03-04)


### Features

* setup-tool opencode auto-registers MCP server in opencode.jsonc ([fe4aafd](https://github.com/MAnders333/knowledge-server/commit/fe4aafdc82469d6e2484167708fe631ea0d560dd))

## [1.3.3](https://github.com/MAnders333/knowledge-server/compare/v1.3.2...v1.3.3) (2026-03-04)


### Bug Fixes

* **scripts:** remove stale LLM env vars from MCP config hints; add Claude Code step ([f5c3d62](https://github.com/MAnders333/knowledge-server/commit/f5c3d62a12170d6dd6e933aedd0ea59282e710d3))

## [1.3.2](https://github.com/MAnders333/knowledge-server/compare/v1.3.1...v1.3.2) (2026-03-04)


### Bug Fixes

* address code review findings (A-1, A-2, B-1, B-3, C-1, C-2, D-1, B-6, W1, W2) ([82d674b](https://github.com/MAnders333/knowledge-server/commit/82d674be007650662990fda8019f3f0d4ffae907))
* raise MAX_MESSAGE_CHARS from 60K to 120K chars ([3b59078](https://github.com/MAnders333/knowledge-server/commit/3b59078ba295d3ed76fdadd1d2a5606533ae175c))
* raise MAX_TOOL_OUTPUT_CHARS from 20K to 40K chars ([3dcc11d](https://github.com/MAnders333/knowledge-server/commit/3dcc11d7d93457998439efcd229630a52c291e36))

## [1.3.1](https://github.com/MAnders333/knowledge-server/compare/v1.3.0...v1.3.1) (2026-03-04)


### Bug Fixes

* **mcp:** remove unused import, add fetch timeout, fix fallback message ([16012d6](https://github.com/MAnders333/knowledge-server/commit/16012d63192c7ed1880080335bb719bec51e11da))

## [1.3.0](https://github.com/MAnders333/knowledge-server/compare/v1.2.3...v1.3.0) (2026-03-04)


### Features

* add Claude Code as a second session source ([c5f6945](https://github.com/MAnders333/knowledge-server/commit/c5f6945aa3f36e37eb94fe32d989067b82d3c6be))


### Bug Fixes

* deduplicate tool name map build in compaction path; add positive allowlist test ([59f67e7](https://github.com/MAnders333/knowledge-server/commit/59f67e7609d6b7f9bf9239c51ea167aff6f00729))
* two bugs in ClaudeCodeEpisodeReader ([d9d06eb](https://github.com/MAnders333/knowledge-server/commit/d9d06eb03d1175eecc9f4b50f65ca6789f3f9e7b))

## [1.2.3](https://github.com/MAnders333/knowledge-server/compare/v1.2.2...v1.2.3) (2026-03-02)


### Bug Fixes

* clamp confidence to [0,1] and default source to empty string in extractKnowledge ([96cf309](https://github.com/MAnders333/knowledge-server/commit/96cf3091817aae927421e8d373eba5730924769c))
* guard against NaN confidence and use dated fallback for missing source ([36df88c](https://github.com/MAnders333/knowledge-server/commit/36df88caad6d31d85ef6a0ab02202668acb87255))

## [1.2.2](https://github.com/MAnders333/knowledge-server/compare/v1.2.1...v1.2.2) (2026-03-02)


### Bug Fixes

* clamp invalid LLM-returned scope to 'personal' before DB insert ([1bd4f6a](https://github.com/MAnders333/knowledge-server/commit/1bd4f6aac3573392ecf7dae5cae258a2954e9170))
* explicit field construction in extractKnowledge map, remove unused import ([08c119d](https://github.com/MAnders333/knowledge-server/commit/08c119d8ccd4952724dc70d88ebdfdca9f324ae2))

## [1.2.1](https://github.com/MAnders333/knowledge-server/compare/v1.2.0...v1.2.1) (2026-03-02)


### Bug Fixes

* address medium/low security findings from security review ([43720be](https://github.com/MAnders333/knowledge-server/commit/43720bef351a76bb207439c8e862482e002cefc8))
* drop darwin-x64 build — macos-13 runner retired, no free Intel Mac runner available ([2bfc637](https://github.com/MAnders333/knowledge-server/commit/2bfc637d60b603f2cc01cc1cce9d924a1403a8fc))

## [1.2.0](https://github.com/MAnders333/knowledge-server/compare/v1.1.0...v1.2.0) (2026-03-02)


### Features

* add darwin-x64 (Intel Mac) build to release matrix and verify binary checksums on install/update ([ae3ad7f](https://github.com/MAnders333/knowledge-server/commit/ae3ad7f2379606ff5068d6500148c69150b65265))

## [1.1.0](https://github.com/MAnders333/knowledge-server/compare/v1.0.1...v1.1.0) (2026-03-02)


### Features

* add binary-first install script, update mechanism, and multi-platform release builds ([197ed66](https://github.com/MAnders333/knowledge-server/commit/197ed668ce53fa0ffaa5ee70f677429a1f94d8d7))
* add knowledge-server update subcommand for in-place binary self-update ([e2d05bc](https://github.com/MAnders333/knowledge-server/commit/e2d05bcd53269e76545bc822d00b6b3219b6d214))


### Bug Fixes

* resolve curl progress-bar flag conflict and add INSTALL_DIR safety guard in install.sh ([8515df9](https://github.com/MAnders333/knowledge-server/commit/8515df9159f3ce8aea51d9eba26400d7e2fd1be8))
* silence small-file downloads in install.sh to reduce terminal noise ([75adb04](https://github.com/MAnders333/knowledge-server/commit/75adb04e5465bc58b4a6f509f45cf358e50857a3))
* use explicit long-form curl flags for clarity and consistent --fail on all downloads ([1a63598](https://github.com/MAnders333/knowledge-server/commit/1a63598d01be8d0bc58581c494867a90ca96d195))

## [1.0.1](https://github.com/MAnders333/knowledge-server/compare/v1.0.0...v1.0.1) (2026-03-02)


### Bug Fixes

* export activateInputSchema from mcp/index.ts and guard main() with import.meta.main to eliminate schema drift in tests ([5c30aae](https://github.com/MAnders333/knowledge-server/commit/5c30aae275a585aad0d025722faa983d2e22b0e2))

## 1.0.0 (2026-03-02)


### Features

* add centralized file logging with stdout tee ([2665702](https://github.com/MAnders333/knowledge-server/commit/266570256298408b6a8bf93888b4b0aaaab38bc8))
* add LLM call timeout, per-call retry with backoff, and timing logs ([9bf518e](https://github.com/MAnders333/knowledge-server/commit/9bf518e72c81718c1bd29cd2026683ecd15097ad))
* expose rawSimilarity, live strength at query time, and fix score labelling ([189f01b](https://github.com/MAnders333/knowledge-server/commit/189f01b7939b151d92f09d0b6157b61192db8c60))
* include allowlisted MCP tool outputs in knowledge extraction ([5b2b97a](https://github.com/MAnders333/knowledge-server/commit/5b2b97a3b651428d8d8e8cf6e6864c38031aa975))
* lower default similarity threshold to 0.35; add limit/threshold params to MCP activate tool ([b876985](https://github.com/MAnders333/knowledge-server/commit/b8769856ddc04ee492c84b939e398070e4ace8f1))
* show full conflicting content in contradiction tags; remove truncation ([3d69719](https://github.com/MAnders333/knowledge-server/commit/3d69719c055d5b4eb0d6916e529a8347beb281d2))
* surface staleness and contradiction warnings in plugin and MCP tool ([ec5dc12](https://github.com/MAnders333/knowledge-server/commit/ec5dc12049c9aa78dd93cc5f0301d07417291318))


### Bug Fixes

* add min floors to all bounded config fields and validate EMBEDDING_DIMENSIONS ([05c61c7](https://github.com/MAnders333/knowledge-server/commit/05c61c78e354e57350a04e8fe9615accdb8e7111))
* add schema version migration guard; clean up stale comments and warnings ([60880d4](https://github.com/MAnders333/knowledge-server/commit/60880d4d5eff79331d98729c4e540e16fd88f2af))
* address logger reviewer feedback ([4ff6cff](https://github.com/MAnders333/knowledge-server/commit/4ff6cffa371faeca74a950745d0bab48ab742430))
* clamp float config fields to min=0 and clarify NaN-safety comments ([5a09030](https://github.com/MAnders333/knowledge-server/commit/5a090306999e071e489f6c162a0ad8968b655274))
* clamp merged type to valid enum before DB write ([a6b665d](https://github.com/MAnders333/knowledge-server/commit/a6b665d9205ad222826ee7e9dd178828f8673628))
* clamp timeoutMs and retryBaseDelayMs to valid ranges ([c3a50cc](https://github.com/MAnders333/knowledge-server/commit/c3a50cc039e0bb0b55ea7f83abd22eb0b1b88373))
* correct inaccurate WAL comment in schema version reset transaction ([34123e4](https://github.com/MAnders333/knowledge-server/commit/34123e4c3fde2310c24532d5940f933a50b5d3f9))
* defer EpisodeReader DB open until first use ([2bf5e4e](https://github.com/MAnders333/knowledge-server/commit/2bf5e4edee987cafae3155b67b1c5a15131671ed))
* detect schema drift by column presence, not just version number ([c731902](https://github.com/MAnders333/knowledge-server/commit/c731902f62075e9fe2f595f379adae84247d3c8d))
* finalize cached statements before db.close() in EpisodeReader ([aac5f12](https://github.com/MAnders333/knowledge-server/commit/aac5f126ee67214a554fe7950f0996d13e73ea18))
* guard conflicting content truncation and align truncation length to 100 ([ed7ed88](https://github.com/MAnders333/knowledge-server/commit/ed7ed88f14ff834ded91944e0500a5bff0e321b4))
* make limit description dynamic to match config.activation.maxResults ([c41513c](https://github.com/MAnders333/knowledge-server/commit/c41513cc57531941d5eaee26c45d82215f24b338))
* make stop.sh process filter actually work ([53283f3](https://github.com/MAnders333/knowledge-server/commit/53283f31da61254a76cf0abfc4aff12538050b66))
* make validateFloatRange upper bound exclusive-capable; fix 0.82 boundary ([31a9cfb](https://github.com/MAnders333/knowledge-server/commit/31a9cfbbe7baddb06b9c2917cdd261c54a80b397))
* quote PRAGMA identifier; add bidirectional sync tests for EXPECTED_TABLE_COLUMNS ([92ae66f](https://github.com/MAnders333/knowledge-server/commit/92ae66f649f68b9b00838505a364871ce8a0985c))
* reference config defaults in validation error messages instead of hardcoding ([387000a](https://github.com/MAnders333/knowledge-server/commit/387000a3756fe1ebc857537f0974ebe49b712d24))
* tighten EpisodeReader statement lifecycle ([64c87c9](https://github.com/MAnders333/knowledge-server/commit/64c87c9b895079498b6de0aad8c8f32a0983e58d))
* tighten stop.sh cmdline filter to entry point path ([2fce5a1](https://github.com/MAnders333/knowledge-server/commit/2fce5a12b4a3f7fee4a418dbeb7556be1cca2893))
* type guard in mergeEntry + cleanup merge case flow ([5df6a60](https://github.com/MAnders333/knowledge-server/commit/5df6a607b9f50721fca93834efdfc17e3b5ad406))
* use config defaults for MCP activate limit and threshold description ([9c994eb](https://github.com/MAnders333/knowledge-server/commit/9c994eb8cba368412e835316ecdc16b369a2330c))
* use serialize() consistently in logger edge paths ([6e7e985](https://github.com/MAnders333/knowledge-server/commit/6e7e98576e5547a34caf864657fd45c2306bda19))
* validate float thresholds against raw env var and add band coherence check ([6d6fa1e](https://github.com/MAnders333/knowledge-server/commit/6d6fa1e6daffae9504597ddfd7a56c4e722ca89c))
* warn instead of silently succeeding when kill fails in stop.sh ([5896286](https://github.com/MAnders333/knowledge-server/commit/589628690bd64440d9e54982fc2932b81b1d0832))
* wrap schema drop sequence in transaction; fix JSDoc examples in decay.ts ([3adb13a](https://github.com/MAnders333/knowledge-server/commit/3adb13a728b09d056ef5babec39712042bcbd736))
