# Changelog

## [3.11.0](https://github.com/MAnders333/knowledge-server/compare/v3.10.3...v3.11.0) (2026-11-13)


### Features

* **activation:** add opt-in remote listwise reranker ([4c442a1](https://github.com/MAnders333/knowledge-server/commit/4c442a10c63c9ffc4e76e2151ea800d927c3fb68))
* **activation:** conservative precision-gate defaults, not replay-calibrated ([971a98c](https://github.com/MAnders333/knowledge-server/commit/971a98c24b3895b18f56f3111edc2748d7af4841))
* **activation:** dryRun option and replay harness ([ac119bc](https://github.com/MAnders333/knowledge-server/commit/ac119bc2f9ef0e9fbeb75d448a91d9f9063a46a2))
* **activation:** gate counts on empty results; session dedupe TTL zero disables ([cc7a2ab](https://github.com/MAnders333/knowledge-server/commit/cc7a2aba0d43c00a9fd84a5338d8bd06694d96d0))
* **activation:** precision gate, token budget and session dedupe ([ed66c00](https://github.com/MAnders333/knowledge-server/commit/ed66c000b8ac409ab6670d5752451ea0f6d41aa3))
* **activation:** precision gate, token budget, session dedupe and injection accounting ([d74e354](https://github.com/MAnders333/knowledge-server/commit/d74e35471ab1ada6c8e1d088b202fe0e4a8ac75d))
* **activation:** show short entry ids and add knowledge_flag ([f933acd](https://github.com/MAnders333/knowledge-server/commit/f933acd2cf29cfbd4ee8e6d6df68618024561765))
* **api:** client tag on /activate and content-free activation events ([e5a24b3](https://github.com/MAnders333/knowledge-server/commit/e5a24b3fc3fb4c88399bc3a133a7d8d1427e7963))
* **api:** session dedupe keys and commit injections after the response ([d5020f8](https://github.com/MAnders333/knowledge-server/commit/d5020f824a80a8707c489f10e26bfdcafc077fe2))
* **api:** validate Host and Origin, serve MCP per request, cap inputs ([aaa0002](https://github.com/MAnders333/knowledge-server/commit/aaa00027b94fa52c44d144f89c33c5dd3b92c9f2))
* call the System One API for remote reranking ([4c2bbe8](https://github.com/MAnders333/knowledge-server/commit/4c2bbe85bfde10e93a00d8c444a7015a0ea06116))
* **config:** DAEMON_AUTO_SPAWN and REDACT_SECRETS use the flag spellings ([3c11b16](https://github.com/MAnders333/knowledge-server/commit/3c11b160192257477f2981ace61f478f62164810))
* **consolidation:** contradiction reply is a {checked, conflicts} object ([3690c02](https://github.com/MAnders333/knowledge-server/commit/3690c023f5de489f5083d0388743889c02fd230c))
* **consolidation:** cross-process run lock shared by server and CLI ([d99413f](https://github.com/MAnders333/knowledge-server/commit/d99413f2c131c53fa7120727f6414b307c440236))
* **consolidation:** per-candidate verdicts by default, conflicts-only opt-in ([76386ca](https://github.com/MAnders333/knowledge-server/commit/76386ca2a3635939dc1263a1b7b920ab297285fd))
* contradiction-scan bounds, injection coverage and opt-in remote rerank ([824ea74](https://github.com/MAnders333/knowledge-server/commit/824ea748cc6541ff647f2ad41a4f1f4496fe4d87))
* **daemon:** add per-session upload watermark table ([4eb4b3b](https://github.com/MAnders333/knowledge-server/commit/4eb4b3ba0eea915c5b7f18659d06344885152f1a))
* **daemon:** exit early for --help and --version ([687041f](https://github.com/MAnders333/knowledge-server/commit/687041f3c4c445f6323ea5fc15790db4cd594309))
* **daemon:** single instance per data dir and a daemon status file ([795527b](https://github.com/MAnders333/knowledge-server/commit/795527bab126c05efadb7b9b6fa7941b9222ac39))
* **db:** add embedding-only write and id-prefix lookup to knowledge stores ([2b165f6](https://github.com/MAnders333/knowledge-server/commit/2b165f699f10849dde8716350045b658dad255a3))
* **db:** honour sslmode verification and sslrootcert for Postgres ([3802d83](https://github.com/MAnders333/knowledge-server/commit/3802d833d7b61598d10ab48c5b19eca0899fdacf))
* **db:** record injections as injection_count, not access_count ([223e1a5](https://github.com/MAnders333/knowledge-server/commit/223e1a5d741b9b6b8cf96a9f57693df3a425cd40))
* default the remote reranker to a 30-candidate pool ([c0106f1](https://github.com/MAnders333/knowledge-server/commit/c0106f12ad4d9d3c4c882327ff982d080ca2c173))
* **logging:** rotate the server log by size (LOG_MAX_BYTES) ([f8ae883](https://github.com/MAnders333/knowledge-server/commit/f8ae883e68e49a431d3e0138b561c7b683916e73))
* **redact:** scrub credentials before storage and embedding ([197b5dd](https://github.com/MAnders333/knowledge-server/commit/197b5dd965a456df44a10b5ddf272cf16b1ac60b))
* **runtime:** add cross-process lockfile and shared runtime paths ([fd3ef49](https://github.com/MAnders333/knowledge-server/commit/fd3ef4910d0bb45218c59643c64c9598045aa250))
* **setup:** add oh-my-pi extension and Codex UserPromptSubmit hook ([526d87f](https://github.com/MAnders333/knowledge-server/commit/526d87f7c724fd5d00c8eece17addf868dccbd80))
* **state:** content-free activation_event table (state schema v3) ([d0f468d](https://github.com/MAnders333/knowledge-server/commit/d0f468dbd21e51f61e1ed8149e515b421c507807))
* **state:** record gate and budget counts in activation_event ([8096c1a](https://github.com/MAnders333/knowledge-server/commit/8096c1a35c905db2ab95012f7c44484a338ca75a))
* **state:** state schema v6 index on pending_episodes(session_id) ([c9aa5e4](https://github.com/MAnders333/knowledge-server/commit/c9aa5e45135486af98b6c750626d3dae1c413b33))
* **status:** per-source captured vs injected counts for 7 days ([cd52e8b](https://github.com/MAnders333/knowledge-server/commit/cd52e8b4568d48963336c448196f6af01b45468a))
* **status:** report ok|degraded|down health; wire startup to locks and housekeeping ([3a9266e](https://github.com/MAnders333/knowledge-server/commit/3a9266e534b99099191beb66bc7dc4ba50a13314))
* System One remote reranker (replaces chat-completions questions) ([edf2d9c](https://github.com/MAnders333/knowledge-server/commit/edf2d9cb3370afe089db3f3686001706b5a2eba6))


### Bug Fixes

* **activation:** bound the injected-tag pattern to avoid quadratic scans ([a948044](https://github.com/MAnders333/knowledge-server/commit/a948044595dc20f6269be66322566f193244d0bf))
* **activation:** build the injected knowledge block on the server with escaping ([651949c](https://github.com/MAnders333/knowledge-server/commit/651949ca88bc7e4e45e5c5840255255fab0f57e8))
* **activation:** gate every unscored ordering, budget rerank, and prefilter the pool ([071ce3c](https://github.com/MAnders333/knowledge-server/commit/071ce3ca5139fee775d650b26da99f2d02e57ab9))
* **activation:** keep dimension-mismatched stores out of scoring and key breakers per operation ([b0c4bf6](https://github.com/MAnders333/knowledge-server/commit/b0c4bf611bf4edd691dcce8af9830aec3c25836c))
* **activation:** keep telemetry rows when a batch write fails ([cf0ce74](https://github.com/MAnders333/knowledge-server/commit/cf0ce74135fa886a8a4829c61a8ae193711871cd))
* **activation:** per-client request budget; sources gated only with rerank or a margin ([2e9cc20](https://github.com/MAnders333/knowledge-server/commit/2e9cc207b745aa641e4827a0a0feae0b182afc9a))
* **activation:** retry a failing telemetry store on the timer, not on each record ([f8e26b2](https://github.com/MAnders333/knowledge-server/commit/f8e26b2c73ae0a4d3d680f3582849fd93a803fe2))
* **activation:** scope embedding keys, refuse clear-text keys, scrub remote rerank input ([402427f](https://github.com/MAnders333/knowledge-server/commit/402427f138e0c37336f72bd8d806239e723d20bc))
* **activation:** warn at startup for keys over plain http; a budget-cut rerank is a skip ([6a9e087](https://github.com/MAnders333/knowledge-server/commit/6a9e087659bba73e504233253cc29c1d2fdc6e15))
* **api:** /mcp requires the stable token whenever one is set, as on earlier releases ([a2bfa6f](https://github.com/MAnders333/knowledge-server/commit/a2bfa6fc858f7e0a9b1a3919db5f23f8f2411afb))
* **api:** a correct admin token is never throttled; log activation text at debug only ([9377d80](https://github.com/MAnders333/knowledge-server/commit/9377d8032e57dca6aae83e0853792245aadbfe56))
* **api:** guard revival of archived and tombstoned entries ([2d90650](https://github.com/MAnders333/knowledge-server/commit/2d90650c4a198ee9ceaaa48a9d4982eca9d41457))
* **api:** hash session ids longer than 256 characters in the dedupe key ([a6cf4f5](https://github.com/MAnders333/knowledge-server/commit/a6cf4f50e203f168ad48c3448b4b52a19d92c885))
* **api:** one cue-count limit, applied after dedupe ([b6c5ee8](https://github.com/MAnders333/knowledge-server/commit/b6c5ee8b2eda3de0f0ba75568f7fafc07c67e648))
* **api:** read protection is opt-in (KNOWLEDGE_PROTECT_READS); admin routes always need the token ([ee002ad](https://github.com/MAnders333/knowledge-server/commit/ee002adec32c8a736b7a6a67a556c05ec80e466d))
* **api:** refuse cross-site fetches, answer non-object JSON with 400, stabilise oversized-request tests ([c5edfcf](https://github.com/MAnders333/knowledge-server/commit/c5edfcff5dc8f274b9a6a8e40874c50b9800f0fc))
* **api:** require the stable admin token on every route except /health ([c699531](https://github.com/MAnders333/knowledge-server/commit/c6995314c6225494670c851b0eeeb069280e7414))
* **auth:** write OAuth tokens through a unique exclusive temp file ([c7856f4](https://github.com/MAnders333/knowledge-server/commit/c7856f4926ec710903b200c2974a863fde33db90))
* **auth:** write the OAuth token file atomically ([7977dff](https://github.com/MAnders333/knowledge-server/commit/7977dff5359133407e31e9ad0f5882a02dbb69a9))
* **cli:** status sends the admin token; the banner echoes only a generated token ([69ff0a6](https://github.com/MAnders333/knowledge-server/commit/69ff0a6fc348d1c5a900780eba63dbfb4e1ac421))
* config, file permissions and test isolation hygiene ([7acb1c2](https://github.com/MAnders333/knowledge-server/commit/7acb1c21e6e5bea380096a4f0ea1f42c04f5ddb2))
* **config:** config.jsonc is owner-only (0600) in an owner-only directory (0700) ([6ea5844](https://github.com/MAnders333/knowledge-server/commit/6ea584483e363cfdd4e412897039a2aa8c95358b))
* **config:** expand ~ in store and stateDb paths ([959b40a](https://github.com/MAnders333/knowledge-server/commit/959b40a35e9fccc44e790385311d092afe5f3390))
* **config:** expose the PID file location used before the data-dir move ([4d1e95f](https://github.com/MAnders333/knowledge-server/commit/4d1e95fd635f80a4a00b08c651ce813a14432a81))
* **config:** honour config.jsonc port in plugins, MCP proxy and /status ([b18a673](https://github.com/MAnders333/knowledge-server/commit/b18a673c06587263827d3a76db5772e8b9c3473e))
* **config:** load .env before config is frozen; OAuth startup check; lenient DAEMON_AUTO_SPAWN ([2a7f6c6](https://github.com/MAnders333/knowledge-server/commit/2a7f6c604edd079e4b82401fe32f89a29305f30e))
* **config:** parse the environment once and validate it where it is read ([a4e67ae](https://github.com/MAnders333/knowledge-server/commit/a4e67ae81c142b51cc58f391225600820f4fd7f6))
* **config:** report zero or non-numeric LLM timeouts instead of clamping ([d5e5ee3](https://github.com/MAnders333/knowledge-server/commit/d5e5ee323db3cd27d09296076b88618433873da8))
* **config:** scope the embedding key to its endpoint ([242ae9c](https://github.com/MAnders333/knowledge-server/commit/242ae9c44f96d34ac11f092f75bb38ef18d476f2))
* consolidate script runs src/index.ts ([d72b9cf](https://github.com/MAnders333/knowledge-server/commit/d72b9cf4c10dcca4c95dca29cd5d1094c7946d2b))
* **consolidation:** check the store lock before LLM extraction ([62404b0](https://github.com/MAnders333/knowledge-server/commit/62404b0f798fa05bfb95a0bc0a64e7813fc02a0e))
* **consolidation:** count only deterministic failures, once per drain ([56e10e9](https://github.com/MAnders333/knowledge-server/commit/56e10e9e91c4443d2c34cb10240676af044293af))
* **consolidation:** escape untrusted text in prompts and validate model output ([252e5d8](https://github.com/MAnders333/knowledge-server/commit/252e5d8f3ca56578a7fc7625564b09421c046064))
* **consolidation:** isolate failing episodes, keep deferred work in the drain, report LLM outages ([8c80420](https://github.com/MAnders333/knowledge-server/commit/8c8042067f5c2a39d7fe1737f6d91c823eb2c7c5))
* **consolidation:** keep episodes blocked after extraction out of the rest of a drain ([fd7a626](https://github.com/MAnders333/knowledge-server/commit/fd7a6263c7f01549ce6a5194961373bf0e3adbe4))
* **consolidation:** keep model output out of info, warn and error logs ([e0f5880](https://github.com/MAnders333/knowledge-server/commit/e0f5880d90d97ae107d6889de68b6e00194c0f89))
* **consolidation:** keep pending episodes until persisted; schema guards ([b18b358](https://github.com/MAnders333/knowledge-server/commit/b18b3587a8cc8c7312778409b27542acefafd807))
* **consolidation:** keep pending episodes until their chunk completes ([8c149eb](https://github.com/MAnders333/knowledge-server/commit/8c149ebef4eb4c49bcf87c94f1f7243ac8d226e4))
* **consolidation:** log entry and episode text at debug only ([01e1057](https://github.com/MAnders333/knowledge-server/commit/01e1057e392ad14504e2ec673c77911a0ff6bee3))
* **consolidation:** route entries only to configured stores and defer when one is down ([8748bf7](https://github.com/MAnders333/knowledge-server/commit/8748bf71057e5667bdfe80210b3dbeef009965db))
* **consolidation:** scrub secrets from episodes before extraction ([c0e4dcd](https://github.com/MAnders333/knowledge-server/commit/c0e4dcdef4e63d70deadcf7c6bc0f08610b64cff))
* **consolidation:** scrub staged titles and LLM-written text before storage ([2d79ba3](https://github.com/MAnders333/knowledge-server/commit/2d79ba398b270a7cc08530c8ffa5e90e545d1341))
* **consolidation:** skip a malformed contradiction batch instead of failing the chunk ([f12aa7f](https://github.com/MAnders333/knowledge-server/commit/f12aa7f249bf4c8e18e07acbed1c7782297d79b2))
* daemon and process-inbox use the config.jsonc userId ([d539a4e](https://github.com/MAnders333/knowledge-server/commit/d539a4e93b4a85f5f3e176ea595367e6d4d1c5ae))
* **daemon:** announce start only after the shutdown handlers exist ([96744da](https://github.com/MAnders333/knowledge-server/commit/96744daf56be48f865f7786cc59b505bb838155b))
* **daemon:** announce start only after the shutdown handlers exist ([82daa3f](https://github.com/MAnders333/knowledge-server/commit/82daa3f43b008014ea89d496c56e0f1e5b064005))
* **daemon:** chunking readers emit only the unuploaded tail of a session ([473cdea](https://github.com/MAnders333/knowledge-server/commit/473cdeafdd03489cd703ca0dbc94690eaa4c2e29))
* **daemon:** honour the full re-ingest request only in the daemon ([449ab48](https://github.com/MAnders333/knowledge-server/commit/449ab48f79ba864acf929c29e9e8757fdb800d51))
* **daemon:** let DAEMON_UPLOAD_INTERVAL_SECONDS set the spawned interval ([a820d07](https://github.com/MAnders333/knowledge-server/commit/a820d0735d652b3dbbe0e586a71902533ebed85f))
* **daemon:** read the spawned daemon's interval through config ([a0e86f3](https://github.com/MAnders333/knowledge-server/commit/a0e86f3bbab422807d506b8ef6abae61ea236ce3))
* **daemon:** reinitialize re-reads full history instead of the lookback window ([c37f160](https://github.com/MAnders333/knowledge-server/commit/c37f160e3622ecf6b89bc2f1e08bf61c72030b5b))
* **daemon:** upload only new session tails, strip NUL bytes, add first-run lookback ([733050b](https://github.com/MAnders333/knowledge-server/commit/733050b11d5d4f858dd788c780425a6e2043eda8))
* **daemon:** wait for the single-instance lock instead of exiting ([612f89a](https://github.com/MAnders333/knowledge-server/commit/612f89af27049eb88a3cb128745a7e770a77f274))
* **db:** busy_timeout on every SQLite connection; state.db housekeeping; SQLite store lockfile ([e278ecb](https://github.com/MAnders333/knowledge-server/commit/e278ecb8ccd48ea2ed832dc24d0e45d991b84a57))
* **db:** clear embedding_vec on Postgres merge resolution ([7ade057](https://github.com/MAnders333/knowledge-server/commit/7ade05745b95a128d6427e5f88ab518f32e92f0e))
* **db:** consolidation lock throws on a second acquire on every backend ([ca3fd3f](https://github.com/MAnders333/knowledge-server/commit/ca3fd3fd6f70febf15f148d8d5a823e9228c4da9))
* **db:** find the userinfo end of a URI before its first query or fragment ([829702b](https://github.com/MAnders333/knowledge-server/commit/829702b42acd5dbec45398026d3239fbfeb2c9af))
* **db:** insertEntry writes injection_count on both backends ([c7328e7](https://github.com/MAnders333/knowledge-server/commit/c7328e78a302eb0b99c4d1f1595d95bbad45bb3d))
* **db:** keep attempt counts out of pending_episodes ([a9be752](https://github.com/MAnders333/knowledge-server/commit/a9be7523829c1001db5ab172e9da68764a9ffa7f))
* **db:** keep pgvector migrations v16/v17 from aborting startup ([d05adc5](https://github.com/MAnders333/knowledge-server/commit/d05adc5dc247e3cd5142bbf4bd4e52f7e555d56e))
* **db:** keep Postgres staging rows when migration v14 retires their tables ([47d2fd9](https://github.com/MAnders333/knowledge-server/commit/47d2fd92909fe23befdeee4ee1c1baeac4f5e0d5))
* **db:** lock staging tables before v14 counts their rows ([adbd568](https://github.com/MAnders333/knowledge-server/commit/adbd568eeb31f3af7ecd4a0a4c03fc90e97444cb))
* **db:** mask connection URI passwords with one helper ([a186b28](https://github.com/MAnders333/knowledge-server/commit/a186b2832ccac7aa1a1c16a57d06371a93f6cc9a))
* **db:** non-owner-safe v19/v4 migrations; count each id once per batch ([75b0689](https://github.com/MAnders333/knowledge-server/commit/75b06892b60a85012ba990dcda9a4b5823405cc8))
* **db:** refuse newer stores and never drop tables; one migration registry per backend ([e9a9a66](https://github.com/MAnders333/knowledge-server/commit/e9a9a663a2c609c51ba6a1d74176f3e2985040c0))
* **db:** tolerate non-owner roles in the v18 migration and clamp cluster join times ([cc85329](https://github.com/MAnders333/knowledge-server/commit/cc85329b4423f163e6a444811d12c142699fe5bf))
* **embedding:** re-embed every stored vector on model change ([491593a](https://github.com/MAnders333/knowledge-server/commit/491593a423f96304efcf5c02b787f8819064a1ca))
* **embedding:** re-embed every stored vector on model change ([d799e7a](https://github.com/MAnders333/knowledge-server/commit/d799e7a973df15a373c8ec90a1861466e47dc404))
* **env:** extend the .env denylist and stop sourcing .env in setup ([6d1f0da](https://github.com/MAnders333/knowledge-server/commit/6d1f0da7c7aa9ac822129167311e01d0ba68eddd))
* **env:** parse .env in one place and keep launchers exec-only ([5d1cb0b](https://github.com/MAnders333/knowledge-server/commit/5d1cb0b3af6cb14ed961b48afc27306238b3201c))
* follow-ups from the independent review of the stack ([9fa1fd8](https://github.com/MAnders333/knowledge-server/commit/9fa1fd87c915d31b8b6ce6590a3726586f47fe9e))
* incremental session upload, single-instance locks, health and log rotation ([28bb02f](https://github.com/MAnders333/knowledge-server/commit/28bb02f69a3e82300c0a15414a2adb1ad8c0e1c0))
* **index:** keep the poll loop alive and clean up on fatal exit ([1a1976f](https://github.com/MAnders333/knowledge-server/commit/1a1976f287b7e2bcfa296b6c5010bf5f953916ce))
* input validation, credential handling and verified releases ([d3ec132](https://github.com/MAnders333/knowledge-server/commit/d3ec1322879e160ee6e410e1268f2eb467fc85cc))
* **install:** create config dir 700 and .env 600 ([2b3af34](https://github.com/MAnders333/knowledge-server/commit/2b3af343611f1871c1a24567638fe1d885f4e5c1))
* **install:** fail closed on checksum gaps and harden the installer ([6472572](https://github.com/MAnders333/knowledge-server/commit/64725721190c0580b7f7f0f0b6a4d24d38bd8c6c))
* **install:** honour every spelling of KNOWLEDGE_REQUIRE_ATTESTATION ([e1762a3](https://github.com/MAnders333/knowledge-server/commit/e1762a3a14cde04d509456c69546f5f186351bbf))
* **install:** refuse pre-manifest releases clearly and point docs at the release installer ([15f8257](https://github.com/MAnders333/knowledge-server/commit/15f8257efeb63a82b98d325617855a0019153243))
* keep extracted knowledge when merge target was deleted ([5076b03](https://github.com/MAnders333/knowledge-server/commit/5076b039dec67814a0b631fd30b13c1924ae5be4))
* key rerank_model by the configured scorer ([0af93c9](https://github.com/MAnders333/knowledge-server/commit/0af93c93f6fdd6932486f0459f730c277d78fa78))
* **llm:** keep provider error bodies out of logs, health and caller errors ([f4ac028](https://github.com/MAnders333/knowledge-server/commit/f4ac0281d78895ccbcb6e8e0be672ced9dd50acc))
* **llm:** throw a sanitized LlmCallError and classify from its cause ([5592386](https://github.com/MAnders333/knowledge-server/commit/5592386ba80bcdfdbcaaf003db5b5f9798956b12))
* **lock:** drop records left by a previous boot or an earlier PID holder ([5b10582](https://github.com/MAnders333/knowledge-server/commit/5b10582ebeb0cf0220d95ebf744abf07188b7930))
* **lock:** keep a live holder's lock and surface a lost lock ([d3c167c](https://github.com/MAnders333/knowledge-server/commit/d3c167c21cf783afca9ba136a7edd67017810648))
* **lock:** take over a stale lock under a takeover file, then re-check ([1b0a181](https://github.com/MAnders333/knowledge-server/commit/1b0a18116e876ca5bf9f2fecd56f6f286842ddac))
* **logger:** reopen the log file when another process rotated it ([27e8c0c](https://github.com/MAnders333/knowledge-server/commit/27e8c0cb2555a04288f9966625c0f5f24373e5ed))
* **logger:** write each line once and reuse one log descriptor ([5254a23](https://github.com/MAnders333/knowledge-server/commit/5254a23b88d506abc60609811d1cd7c5077230a9))
* **mcp:** describe flagged entries accurately in the knowledge_flag reply ([6ec1413](https://github.com/MAnders333/knowledge-server/commit/6ec1413420e4984a56901821fcadf28cacc131aa))
* **mcp:** probe /health for reachability, falling back to /status ([a57677f](https://github.com/MAnders333/knowledge-server/commit/a57677fa7396bbfd2135943383b5c2f4ffe5738e))
* **mcp:** refuse a non-loopback KNOWLEDGE_HOST in the stdio proxy ([a5f4d66](https://github.com/MAnders333/knowledge-server/commit/a5f4d667887e25d0b07313bd27ff7aa295fb2af5))
* **mcp:** send the admin bearer on read calls; flagging needs no admin token ([5f80a05](https://github.com/MAnders333/knowledge-server/commit/5f80a0571ff6b7d935dd857fa7cf5e33f1edeab0))
* owner-only log and DB files; admin token not printed off a terminal ([78f408c](https://github.com/MAnders333/knowledge-server/commit/78f408cd03192aa8c33aa75e372b9d0294bb5008))
* **plugins:** bound the activation query URL so long prompts are not rejected ([ef45e27](https://github.com/MAnders333/knowledge-server/commit/ef45e2715894eab0cf82d7b82d18e22a234004e9))
* **plugins:** dedupe before cutting; drop short truncated segments ([5e92708](https://github.com/MAnders333/knowledge-server/commit/5e927083a57569ce974db9666a8c297ba75474f5))
* **plugins:** mirror the tag and joiner rules and accept an IPv6 loopback URL ([03d4f97](https://github.com/MAnders333/knowledge-server/commit/03d4f97296190191fb42083d41f7e65e4afb861f))
* **plugins:** send the admin bearer on read calls when the token is set ([97efd9e](https://github.com/MAnders333/knowledge-server/commit/97efd9ee043b3dc94608b7d988ba06bcb04b18da))
* **private-files:** never follow symlinks or change files another user owns ([0c40b2b](https://github.com/MAnders333/knowledge-server/commit/0c40b2bd4cb990829e0d7016ff7531245258e119))
* read injection_count into entries on both backends ([15ad338](https://github.com/MAnders333/knowledge-server/commit/15ad3383d41739828eeb21e8a88c1bdc846768a8))
* **readers:** drop injected knowledge from transcripts and label tool output ([4fe0e5e](https://github.com/MAnders333/knowledge-server/commit/4fe0e5ed10d736381c24598ea5203a235a0c12d0))
* reconcile activation dry-run test with perf scan path ([cf4339b](https://github.com/MAnders333/knowledge-server/commit/cf4339b3edf7f12a171b91ab27362292fdbb1772))
* reconcile injected-tag detection with stripInjectedKnowledge ([a8cf2f8](https://github.com/MAnders333/knowledge-server/commit/a8cf2f843e2ea06ad5a4c43566f4794f2b49c041))
* reconcile prompt hardening with activation tag helpers ([c7146c7](https://github.com/MAnders333/knowledge-server/commit/c7146c70b92d8e9c51663f9580b72262f49a4e3d))
* reconcile re-embed and revival with perf's embedding-free reads ([ec576e3](https://github.com/MAnders333/knowledge-server/commit/ec576e3f8329f72a6000660769192bc6a5cf5c5b))
* reconcile revival tests with api-boundary Host check ([a3cb167](https://github.com/MAnders333/knowledge-server/commit/a3cb16727d104831f9f88c97c3ff2d1b0aade093))
* record session dedupe and injections only for delivered responses ([e2ff8cc](https://github.com/MAnders333/knowledge-server/commit/e2ff8ccc80e4ac1d9a68b19fb3f336c5081cb4bf))
* redact userinfo passwords containing @ and empty usernames ([c93f2e2](https://github.com/MAnders333/knowledge-server/commit/c93f2e2aee72ac6d38b468c1276c7f6fb0a83e08))
* **redact:** keep context captures linear on long whitespace runs ([1e99bb8](https://github.com/MAnders333/knowledge-server/commit/1e99bb8791f4c1a2b811828af4bc5e85e6cdb42a))
* **redact:** redact lowercase and mid-line credential assignments ([eba9c5b](https://github.com/MAnders333/knowledge-server/commit/eba9c5bf8c9b6a245034435bec9949b84394759c))
* **redact:** require a secret-like value for keyed and env assignments ([a247005](https://github.com/MAnders333/knowledge-server/commit/a2470056dcd4beb1fb9c80e7edbf97a1fe4247f7))
* **reinitialize:** --reset-state only deletes the current user's pending episodes ([a31b34e](https://github.com/MAnders333/knowledge-server/commit/a31b34e1f6b06060f4dbf7f00a86770d16a4c9fa))
* **reinitialize:** scope --reset-state by the daemon's user id ([c213c2d](https://github.com/MAnders333/knowledge-server/commit/c213c2d5c7a69cdb47fb23e59270c0dc2c8a0a8f))
* **release:** build binaries without cwd autoload and harden publish ([8290c3c](https://github.com/MAnders333/knowledge-server/commit/8290c3ca80236c57fda6648055c67bca699198a2))
* **release:** keep release tags unprefixed and v-prefixed under the config file ([2dceb4e](https://github.com/MAnders333/knowledge-server/commit/2dceb4e9c02f4539768ee17a92a5d4da6850472b))
* **release:** read release-please config, one compile script, pinned typescript ([d39a78a](https://github.com/MAnders333/knowledge-server/commit/d39a78a17b398f55d2ed632e927d30d123f34993))
* **rerank:** expire queued calls by the timeout flag, and give the 2,100-row test room ([9271360](https://github.com/MAnders333/knowledge-server/commit/92713609235cbcfdefbdc6e01bff470f625f0574))
* **rerank:** run local scoring single-flight with a bounded queue ([772a582](https://github.com/MAnders333/knowledge-server/commit/772a582ffd8fc16d9907755e96b41d06b5428687))
* **rerank:** score pairs one at a time under the timeout; pin model downloads ([#98](https://github.com/MAnders333/knowledge-server/issues/98)) ([dfe51f2](https://github.com/MAnders333/knowledge-server/commit/dfe51f2ed0007893edfab840f488809aa36c720b))
* **rerank:** trim overlong pairs longest-first and clip by tail or head (C3) ([e6e3fc9](https://github.com/MAnders333/knowledge-server/commit/e6e3fc9317f9e96f003342119fe6c9bb2f39dda1))
* restore .env ignored-key warning and proxy/CA allowlist ([1267acd](https://github.com/MAnders333/knowledge-server/commit/1267acd0f864a9befdff420de35ab8c9ef6f5e05))
* **review:** leave entries in read-only stores unchanged ([5813ba6](https://github.com/MAnders333/knowledge-server/commit/5813ba66e86c147d4b17360c0875093d16b99133))
* run remote config checks only for an active remote reranker ([076312c](https://github.com/MAnders333/knowledge-server/commit/076312cbf3b5b844486114e49cfae9d136d22128))
* second review round (release pipeline, auth, operability, store, activation) ([1bbc956](https://github.com/MAnders333/knowledge-server/commit/1bbc9569a2b2bb2ba5f31556b8bfa79b939b9c81))
* **security:** send Claude OAuth tokens only to api.anthropic.com ([0dc538d](https://github.com/MAnders333/knowledge-server/commit/0dc538d057b0b9070a97f0c2b3f80181f579c98e))
* **server:** log, do not throw, when deferred state.db housekeeping fails ([54a4540](https://github.com/MAnders333/knowledge-server/commit/54a454038701d7bf4d95039bba00d1af076051c0))
* **server:** look for a pre-upgrade PID file when the data directory moved ([b4e70ae](https://github.com/MAnders333/knowledge-server/commit/b4e70ae73bf52eb4d813f0946620adc36fac3b1d))
* **server:** match only a server started with no arguments, and keep an unverifiable PID file ([3cb5063](https://github.com/MAnders333/knowledge-server/commit/3cb50630bf397d3681065c797d98a404a2c5b55f))
* **server:** one PID file reader policy across stop, start, status and reinitialize ([6345ece](https://github.com/MAnders333/knowledge-server/commit/6345eceb7b682ece13b7a9bfe9d4fab054cee3e9))
* **server:** run state.db housekeeping after the port is bound ([e15e3d3](https://github.com/MAnders333/knowledge-server/commit/e15e3d37a39e8f049c459f40c020757ad730483b))
* **server:** verify the PID file names a knowledge-server before stop or start ([291ef08](https://github.com/MAnders333/knowledge-server/commit/291ef083ff986b20d107ddb1d60511da1c34dca4))
* **setup-tool:** keep config file modes and send the admin bearer from hooks ([5c93c59](https://github.com/MAnders333/knowledge-server/commit/5c93c5996b883299e20fdc4a946dc27c4efe3ce1))
* **setup-tool:** keep the admin token out of the Codex hook's curl argv ([d802260](https://github.com/MAnders333/knowledge-server/commit/d8022609142c1c2e75f34f7ed6bb4867c37366ea))
* **setup-tool:** merge MCP entries instead of replacing them on re-run ([72f9dff](https://github.com/MAnders333/knowledge-server/commit/72f9dffc5e967b6673d51073d34e6702b03b293c))
* **setup-tool:** run the Codex hook under /bin/sh; load .env for source installs ([0ed9ad3](https://github.com/MAnders333/knowledge-server/commit/0ed9ad3f09ced50de7ff7f1a7717f4e72da18f08))
* **setup-tool:** verify downloaded plugin and command files ([989b8e7](https://github.com/MAnders333/knowledge-server/commit/989b8e7d8d67baa559404daf7ead1f732aed3340))
* **setup:** resolve the home directory at call time and harden the Codex hook ([a3cafe8](https://github.com/MAnders333/knowledge-server/commit/a3cafe8690fda9115596f94fa9d49a93c3e3f854))
* **state:** count only activations that injected entries in status ([10e1b59](https://github.com/MAnders333/knowledge-server/commit/10e1b59253376fbbe93f56ca851cd429fe89650f))
* **state:** migrate legacy staging tables only from the knowledge.db passed in ([a25b3bd](https://github.com/MAnders333/knowledge-server/commit/a25b3bd382cdfb4a12997c0f738d414b3f903ead))
* **state:** skip Postgres index steps the role does not own ([4d69a83](https://github.com/MAnders333/knowledge-server/commit/4d69a830d3f2ef8982e92c69577bdb815dc39b01))
* **status:** match oh-my-pi captures with the omp client tag ([bcaeae5](https://github.com/MAnders333/knowledge-server/commit/bcaeae5e4406c7e6047fac32ca6c42c7b81fb82b))
* **status:** reword the coverage warning for calls that returned nothing ([9b297d9](https://github.com/MAnders333/knowledge-server/commit/9b297d90f9d07aeea991ee759797e603e0e971e9))
* summarise object error details from the gateway ([0c9659a](https://github.com/MAnders333/knowledge-server/commit/0c9659a15001ac83cabbbb88140653a27320158f))
* **test:** daemon registration guard holds from any cwd ([0b6581b](https://github.com/MAnders333/knowledge-server/commit/0b6581b1c9b98b16778e3bc65ccc7727a264bff7))
* tighten permissions of owned files at startup ([86dd510](https://github.com/MAnders333/knowledge-server/commit/86dd510b98866c6f3f8a1c2fdfbe9ff7e6c192d4))
* two flaky tests (queued rerank expiry race, slow 2,100-row insert) ([3acf59a](https://github.com/MAnders333/knowledge-server/commit/3acf59adffc8d57c9771ac4c04cfdf80a5348564))
* **untrusted:** match guarded tags linearly and strip injected blocks, keeping the rest ([82e5631](https://github.com/MAnders333/knowledge-server/commit/82e563192eb6b886d37638d164182303a7575e5e))
* **update:** adopt extension copies made before the registry ([161a3bc](https://github.com/MAnders333/knowledge-server/commit/161a3bc53beda646b8dfffb0e2f4532696978716))
* **update:** refresh extension copies that setup-tool placed ([dd8212d](https://github.com/MAnders333/knowledge-server/commit/dd8212da468ed806d1a01df5e8d0f071cecea9a6))
* **update:** restart the running daemon after an update ([0bf946c](https://github.com/MAnders333/knowledge-server/commit/0bf946cd4b8b8832d0673f7b5642f4199adad81d))
* **update:** verify release assets and roll back failed installs ([ae65307](https://github.com/MAnders333/knowledge-server/commit/ae65307b2e90fcdad9fc4768e28e1b15a46d34b1))
* verify daemon identity before signalling ([7676db3](https://github.com/MAnders333/knowledge-server/commit/7676db3725849001ede140f53f596e2c8108d279))
* warn on ignored .env keys and allow proxy and CA variables ([27dab32](https://github.com/MAnders333/knowledge-server/commit/27dab32303d504a41fa4deb3800fc301fb79a067))
* warn when reinitialize runs with a live daemon ([9c83328](https://github.com/MAnders333/knowledge-server/commit/9c83328aa354e90118d3aa28d2790385f3bb48f8))


### Refactoring

* **activation:** split activate() into stage functions ([97fa900](https://github.com/MAnders333/knowledge-server/commit/97fa900e010f1e25c56a8430c39a3731ca389eac))
* **api:** take createApp options as one object ([d10dcfa](https://github.com/MAnders333/knowledge-server/commit/d10dcfabc0f36cd0c5f9d44fc3011027a85278a9))
* **api:** unexport symbols with no users outside their module ([49e8fc0](https://github.com/MAnders333/knowledge-server/commit/49e8fc0cee2a58a66ec2386d8d382e6ca9988026))
* **db:** drop ORDER BY strength on reads that callers re-sort ([4c8b913](https://github.com/MAnders333/knowledge-server/commit/4c8b9139451d3c3af1add13b27ae2622207a886f))
* **db:** one INSERT for Postgres entries ([7006d3e](https://github.com/MAnders333/knowledge-server/commit/7006d3e43926eb49fbe9443f6bfd36713f6bf292))
* **db:** one shared plan for contradiction resolution ([2c21767](https://github.com/MAnders333/knowledge-server/commit/2c217674a6c5e389e344ecd4e2b05497d4921e5b))
* **db:** one UPDATE for Postgres mergeEntry ([090e7af](https://github.com/MAnders333/knowledge-server/commit/090e7aff6185081af1c4e7b6f4ebc2034bf0c4d9))
* **db:** read pool size, init timeout and state DB URI from config ([7876636](https://github.com/MAnders333/knowledge-server/commit/7876636f057925d328f7b1515610fafc297350aa))
* **layering:** move shared domain code to src/domain; lazy default paths ([1f23d0d](https://github.com/MAnders333/knowledge-server/commit/1f23d0dcc80b7c552100254e0ccc2e0bc403dea8))
* **mcp:** keep readCallHeaders module-private ([4e8954a](https://github.com/MAnders333/knowledge-server/commit/4e8954a8705dd198e30ec7b4219e73d36f0b5e0a))
* **reinitialize:** use the shared currentUserId helper ([b67dbd9](https://github.com/MAnders333/knowledge-server/commit/b67dbd99536ba1d0843c3771566c8f632eb7cd32))


### Performance

* activation metrics, DB/background efficiency and non-blocking rerank ([395cc22](https://github.com/MAnders333/knowledge-server/commit/395cc22d9c748138f395ead2d0122bd8bb99e30e))
* **activation:** cap and dedupe cues server-side ([6fae7d5](https://github.com/MAnders333/knowledge-server/commit/6fae7d5ab9f72794e35f0a7596e712c5eab5f6ee))
* **activation:** circuit-break dead stores in the fan-out ([151c0d1](https://github.com/MAnders333/knowledge-server/commit/151c0d1871ce3db5381e2481559117c5c1aa1b9b))
* **activation:** score the full scan from id + embedding rows ([af1ef90](https://github.com/MAnders333/knowledge-server/commit/af1ef90bedbb2bbf6ecf0899b926c218bc39436c))
* **consolidation:** bound contradiction checks per entry and skip unchanged merges ([fbf2f7e](https://github.com/MAnders333/knowledge-server/commit/fbf2f7efe231cea8039fa946e12776a02c5e452a))
* **consolidation:** decay on its own daily timer; no empty trailing batch ([0e12f81](https://github.com/MAnders333/knowledge-server/commit/0e12f81596bb89e6db0b73e84be73009f3c2a393))
* **consolidation:** load each store's entries once per batch ([7a7122a](https://github.com/MAnders333/knowledge-server/commit/7a7122a126f14c1f0a39a2e6e77dffb5a2086339))
* **db:** compute strength on read; drop the strength and accessed indexes ([a95fccd](https://github.com/MAnders333/knowledge-server/commit/a95fccdb39eac09409899dbd55f1149b659f19ab))
* **db:** explicit entry column lists; plain reads omit the embedding ([bb051dd](https://github.com/MAnders333/knowledge-server/commit/bb051dd860e57e6aae90d3f05226c89d3e469cc8))
* **llm:** per-call-type timeouts and no retry on timeout ([bf13d21](https://github.com/MAnders333/knowledge-server/commit/bf13d2118ba964a90d55d5c3dc374d142de91fca))
* **synthesis:** cap peers and token budget per synthesis prompt ([c59755c](https://github.com/MAnders333/knowledge-server/commit/c59755c3ea19e366615b7c3d31096a2e64ed7f4d))
* **synthesis:** keep cluster member joined_at across clustering passes ([5e3bf8d](https://github.com/MAnders333/knowledge-server/commit/5e3bf8d9c8ac2a6a892466cd7f26b81228df3069))
* **synthesis:** synthesize only after a material membership change ([6e40526](https://github.com/MAnders333/knowledge-server/commit/6e405265593ea9a4d62a6a521c019fd7b2b4070b))


### Documentation

* **activation:** retrieval stays in activate.ts ([0100b7d](https://github.com/MAnders333/knowledge-server/commit/0100b7d2cd60e70c8c70693180a871d6a02dc9f0))
* add SECURITY.md and CODEOWNERS ([4839e94](https://github.com/MAnders333/knowledge-server/commit/4839e94fcfff9da5f3af8e5a33933cd85368d286))
* add verify-before-pipe steps and point references at the README section ([2c3429c](https://github.com/MAnders333/knowledge-server/commit/2c3429c1d4e048d831ccf1e140bbe2e226eeba13))
* admin token is printed only on a terminal ([8169acb](https://github.com/MAnders333/knowledge-server/commit/8169acb6eff38df0b4a9b40698316f71f6064320))
* **commands:** send the admin token in a stdin header, not curl argv ([8a02357](https://github.com/MAnders333/knowledge-server/commit/8a0235774c337aeb78d9e2e3746ac9e7d1908d9e))
* **config:** correct embedding key, Bun, daemon, poll and gate settings ([418cf96](https://github.com/MAnders333/knowledge-server/commit/418cf962ab683084c917cb8e75c7de3463c67418))
* correct rerank_model meaning, origin fallback and the pplx model name ([564861e](https://github.com/MAnders333/knowledge-server/commit/564861e13d2a877397a97febfe5240d40b08f36c))
* correct threshold and contradiction model defaults ([84aa67e](https://github.com/MAnders333/knowledge-server/commit/84aa67e91bd33348780894a91e8a07fd83becd2e))
* describe host allow-list, redaction, input limits and Postgres TLS ([b714bcf](https://github.com/MAnders333/knowledge-server/commit/b714bcf7d13d08142740f02a190f7c76c0a99b8e))
* describe read protection and /mcp accurately; note the upgrade ([80dbaf6](https://github.com/MAnders333/knowledge-server/commit/80dbaf655b33993d52e10cc8266d57ad7a6471d7))
* describe the remote reranker, its hosts and its settings ([90cd825](https://github.com/MAnders333/knowledge-server/commit/90cd8252208a7845cab2cd08b9a6f29ad949185e))
* document activation gate settings as uncalibrated placeholders ([2f69871](https://github.com/MAnders333/knowledge-server/commit/2f69871171d7579290637a5af60ab9ce46acbdbc))
* document CONTRADICTION_CONFLICTS_ONLY as an unvalidated opt-in ([532ff5c](https://github.com/MAnders333/knowledge-server/commit/532ff5c049c7ae749bbc4ee98d31bffaf7520f84))
* document update verification, rollback and attestation checks ([d978eb7](https://github.com/MAnders333/knowledge-server/commit/d978eb710769c3ce2193780595436d443b5764cb))
* flagging needs no admin token; review reads send the token only when set ([c629056](https://github.com/MAnders333/knowledge-server/commit/c6290564822373595f573fd49cda6a13b5ca717f))
* knowledge-review token note matches TTY-only printing ([5c96855](https://github.com/MAnders333/knowledge-server/commit/5c968555d9d3cf1535b0b21cf9952c40346ddf2c))
* load the admin token first in review; state the flag auth rule ([a2f2e83](https://github.com/MAnders333/knowledge-server/commit/a2f2e83e03da5b80f4c4ce55ea2041c2fdf2dc2d))
* LOG_LEVEL, client tag, activation telemetry and replay harness ([756e625](https://github.com/MAnders333/knowledge-server/commit/756e625e9aff2da88060ed4c881681b1429f1d53))
* measured System One numbers, floor trade-off, EU twins, candidate default ([fa3ba77](https://github.com/MAnders333/knowledge-server/commit/fa3ba778267637db92a6dd7a12667d996d3b10a1))
* **plugins:** describe the cue cap as the plugin's own, not boundCues ([bec55e0](https://github.com/MAnders333/knowledge-server/commit/bec55e0bd5a7725312293b75aa2e002aa0e903d8))
* **plugins:** document KNOWLEDGE_ADMIN_TOKEN in the plugin headers ([f0c65c5](https://github.com/MAnders333/knowledge-server/commit/f0c65c5f15f7efe9716dcb16b7e9d5849682165b))
* **readme:** precision gate, token budget, session dedupe and access settings ([a99ab9c](https://github.com/MAnders333/knowledge-server/commit/a99ab9c3b609c2e4a0436c754efb7c5d18bc5c29))
* **readme:** separate captured-from and injected-into per tool ([0af4453](https://github.com/MAnders333/knowledge-server/commit/0af4453bb50065715bc2cd2bc7a34af72b62a611))
* **rerank:** document RERANK_* options and the English-only default ([168339c](https://github.com/MAnders333/knowledge-server/commit/168339c20b8044c58e0a8c3f75d03bfc0c76c28c))
* state what update refreshes, flag effect, LOG_LEVEL, remote rerank model ([d16bc0e](https://github.com/MAnders333/knowledge-server/commit/d16bc0e4af6ce3cedb73850e2b1b9fec0b7d2e7f))
* upgrade notes, legacy PID stop, and the request budget for rerank ([e9dfa20](https://github.com/MAnders333/knowledge-server/commit/e9dfa20772d9d467644eeabb41decef1d7093e4e))

## [3.10.3](https://github.com/MAnders333/knowledge-server/compare/v3.10.2...v3.10.3) (2026-11-12)


### Bug Fixes

* **embeddings:** validate embedBatch responses and retry bulk chunks ([b1d32b2](https://github.com/MAnders333/knowledge-server/commit/b1d32b2801464a8409e60568ad23ae548f62916d))
* **embeddings:** validate embedBatch responses and retry bulk chunks ([1f32684](https://github.com/MAnders333/knowledge-server/commit/1f3268485d9b01e3d0abc3d589c751c88f629e0c)), closes [#100](https://github.com/MAnders333/knowledge-server/issues/100)

## [3.10.2](https://github.com/MAnders333/knowledge-server/compare/v3.10.1...v3.10.2) (2026-11-10)


### Bug Fixes

* **db:** raise hnsw.ef_search for ANN candidates and skip embedding_vec ([f3ffc54](https://github.com/MAnders333/knowledge-server/commit/f3ffc54515e5a2f00f9204e71ea6cec1c3dd6596))

## [3.10.1](https://github.com/MAnders333/knowledge-server/compare/v3.10.0...v3.10.1) (2026-11-09)


### Bug Fixes

* **activation:** choose ANN vs full scan per store instead of for all stores ([be1071d](https://github.com/MAnders333/knowledge-server/commit/be1071d321628140c5021a2273eb490b5e16f458))
* **activation:** choose ANN vs full scan per store instead of for all stores ([a49d327](https://github.com/MAnders333/knowledge-server/commit/a49d32743358044c7874463b1be60b18c02d8924)), closes [#93](https://github.com/MAnders333/knowledge-server/issues/93)

## [3.10.0](https://github.com/MAnders333/knowledge-server/compare/v3.9.2...v3.10.0) (2026-09-30)


### Features

* **daemon:** add oh-my-pi (omp) episode reader ([0d767d0](https://github.com/MAnders333/knowledge-server/commit/0d767d041def0ccb55c459ad1bea7e88a00d1c81))
* **daemon:** add oh-my-pi (omp) episode reader ([e1430c2](https://github.com/MAnders333/knowledge-server/commit/e1430c2a75d59bcee35f89a4744a24547176bf1f))

## [3.9.2](https://github.com/MAnders333/knowledge-server/compare/v3.9.1...v3.9.2) (2026-09-30)


### Bug Fixes

* **db:** bound store init so a silent store cannot block degraded startup ([f6ae24f](https://github.com/MAnders333/knowledge-server/commit/f6ae24f4edee16020b824b5c1cb2cc4d41467630))
* **db:** bound store init so a silent store cannot block degraded startup ([3209277](https://github.com/MAnders333/knowledge-server/commit/3209277f446c348414a21c30012e5019f345a096))
* **llm:** add OPENAI_DROP_PARAMS to omit unsupported request parameters ([0a9a34a](https://github.com/MAnders333/knowledge-server/commit/0a9a34ae1227319806cad35aa41da9c318aba5d8))
* **llm:** configurable OpenAI-compatible request transforms for gpt-5.x/o-series models ([9ce754f](https://github.com/MAnders333/knowledge-server/commit/9ce754fc3221c07dfc139826c423e252559d883c))
* **llm:** make OpenAI token-limit parameter configurable via OPENAI_MAX_TOKENS_PARAM ([b101239](https://github.com/MAnders333/knowledge-server/commit/b101239f69ff7af36fb5aa952de45c932d8b7412))

## [3.9.1](https://github.com/MAnders333/knowledge-server/compare/v3.9.0...v3.9.1) (2026-08-05)


### Bug Fixes

* **activation:** repair reranker WASM load under Bun source/dev runs ([9a906a2](https://github.com/MAnders333/knowledge-server/commit/9a906a27f0eb0de5bc5d9de15b398e109aba4a98))
* **activation:** repair reranker WASM load under Bun source/dev runs ([56fb7f4](https://github.com/MAnders333/knowledge-server/commit/56fb7f4cf19bae6f2016ea40c5574fa8a25f71ba))

## [3.9.0](https://github.com/MAnders333/knowledge-server/compare/v3.8.9...v3.9.0) (2026-08-05)


### Features

* **activation:** add local cross-encoder rerank stage ([00276ac](https://github.com/MAnders333/knowledge-server/commit/00276ac38d6e3132960b5e5513e3eb6f673c02d3))
* **activation:** add pi knowledge extension + setup-tool pi ([1c78722](https://github.com/MAnders333/knowledge-server/commit/1c787227e62a7c0dc8b30a0460b7a0e4427f7bca))
* **daemon:** add pi session reader ([234a00e](https://github.com/MAnders333/knowledge-server/commit/234a00e1140f5b34396c6cc8952c4a6f8322326f))
* **db:** multi-store support with per-store ids and fan-out ([21f1161](https://github.com/MAnders333/knowledge-server/commit/21f116191a9075621f09af8565899d43e5c0e18a))


### Bug Fixes

* **activation:** make reranker work in single-file compiled binaries ([ce4552c](https://github.com/MAnders333/knowledge-server/commit/ce4552c9bb5f449335190396ab9cd6507e69f105))

## [3.8.9](https://github.com/MAnders333/knowledge-server/compare/v3.8.8...v3.8.9) (2026-06-07)


### Bug Fixes

* **pgvector:** skip HNSW index creation when index already exists ([6480890](https://github.com/MAnders333/knowledge-server/commit/6480890c4e287def2a3d11278adb30554510eb53))
* **pgvector:** skip HNSW index creation when index already exists ([db45ca7](https://github.com/MAnders333/knowledge-server/commit/db45ca7af5cd7d3797b421d88108e0629674feba))

## [3.8.8](https://github.com/MAnders333/knowledge-server/compare/v3.8.7...v3.8.8) (2026-06-05)


### Bug Fixes

* **activation:** add per-call timeout to embedding fetch ([79d3cf8](https://github.com/MAnders333/knowledge-server/commit/79d3cf8859d2d409015f16a51fa7f3ada4b14154))
* **activation:** add per-call timeout to embedding fetch ([5507fd1](https://github.com/MAnders333/knowledge-server/commit/5507fd169f2567d41f77504526096c8b891c672f))

## [3.8.7](https://github.com/MAnders333/knowledge-server/compare/v3.8.6...v3.8.7) (2026-05-26)


### Bug Fixes

* **local-files:** floor mtimeMs to integer for BIGINT compatibility ([66e1135](https://github.com/MAnders333/knowledge-server/commit/66e113544539883acd62f8a1d20782a222869145))
* **local-files:** floor mtimeMs to integer for BIGINT compatibility ([d7fcf86](https://github.com/MAnders333/knowledge-server/commit/d7fcf867afe0ab75544991e6c8db25d40dbccc6d))

## [3.8.6](https://github.com/MAnders333/knowledge-server/compare/v3.8.5...v3.8.6) (2026-04-16)


### Bug Fixes

* **db:** make ANN candidate selection index-first ([25d39dd](https://github.com/MAnders333/knowledge-server/commit/25d39ddbfd27d085889a54116e119749e7678b74))

## [3.8.5](https://github.com/MAnders333/knowledge-server/compare/v3.8.4...v3.8.5) (2026-04-15)


### Bug Fixes

* **db:** add v17 pgvector ANN repair migration and startup self-heal ([d60e43e](https://github.com/MAnders333/knowledge-server/commit/d60e43ea8f9bb65d2f15ff5a716aea2361690b88))

## [3.8.4](https://github.com/MAnders333/knowledge-server/compare/v3.8.3...v3.8.4) (2026-04-15)


### Bug Fixes

* **release:** avoid daemon smoke-test hang in macOS verification ([001b861](https://github.com/MAnders333/knowledge-server/commit/001b861c3f4b50559c2e39d844f2b86075194bb9))

## [3.8.3](https://github.com/MAnders333/knowledge-server/compare/v3.8.2...v3.8.3) (2026-04-15)


### Bug Fixes

* **release:** ad-hoc re-sign darwin binaries before verification ([0953c9d](https://github.com/MAnders333/knowledge-server/commit/0953c9d812cdcf41ddbbd0f22920bf727a17b9fe))

## [3.8.2](https://github.com/MAnders333/knowledge-server/compare/v3.8.1...v3.8.2) (2026-04-15)


### Bug Fixes

* **release:** pin Bun and verify macOS binaries before upload ([7db3982](https://github.com/MAnders333/knowledge-server/commit/7db3982565cd6dff2c0c12ca1ee92ce3a65d645e))

## [3.8.1](https://github.com/MAnders333/knowledge-server/compare/v3.8.0...v3.8.1) (2026-04-15)


### Bug Fixes

* **db:** use halfvec ANN + exact rerank for high-dimension embeddings ([5d791f8](https://github.com/MAnders333/knowledge-server/commit/5d791f8e3511fab81ee806997a0e40a8f170961a))

## [3.8.0](https://github.com/MAnders333/knowledge-server/compare/v3.7.0...v3.8.0) (2026-04-14)


### Features

* pgvector ANN search for Postgres stores (schema v16) ([6ba75f7](https://github.com/MAnders333/knowledge-server/commit/6ba75f784980cebf76f433d618ab8df0169ca9b7))


### Bug Fixes

* close and reopen db pool after DROP COLUMN in pg test beforeEach ([a5e2e25](https://github.com/MAnders333/knowledge-server/commit/a5e2e25e4fb310c6a1a98ec5793e07e4859af8e6))
* **db:** correct JSONB serialization and replace invalid bit cast with JS backfill ([cae0084](https://github.com/MAnders333/knowledge-server/commit/cae00843491b2ba999a0dc5dc6d646bc39a38615))
* guard embedding_vec column references behind pgvectorReady ([c0b8658](https://github.com/MAnders333/knowledge-server/commit/c0b86584c80855f7e675f85ffd87bdfd253cf38a))
* null embedding_vec in mergeEntry when no embedding provided ([a4b2721](https://github.com/MAnders333/knowledge-server/commit/a4b2721df49817f68c210143c6089734bd20b587))

## [3.7.0](https://github.com/MAnders333/knowledge-server/compare/v3.6.7...v3.7.0) (2026-04-14)


### Features

* Claude Pro/Max subscription OAuth authentication ([d1d9320](https://github.com/MAnders333/knowledge-server/commit/d1d932032c9c786d60620fcf46e0e5ea9dc998be))
* Claude subscription OAuth, process-inbox command, starvation fix ([#71](https://github.com/MAnders333/knowledge-server/issues/71)) ([421c356](https://github.com/MAnders333/knowledge-server/commit/421c3563d3ddb3f7791d555847cc1c9bfb98b0e7))


### Bug Fixes

* daemon cursor starvation when sessions produce zero episodes ([d1d9320](https://github.com/MAnders333/knowledge-server/commit/d1d932032c9c786d60620fcf46e0e5ea9dc998be))

## [3.6.7](https://github.com/MAnders333/knowledge-server/compare/v3.6.6...v3.6.7) (2026-03-27)


### Bug Fixes

* binaries self-load .env; daemon drops unneeded validateConfig ([23dc6a1](https://github.com/MAnders333/knowledge-server/commit/23dc6a10cc0183c9dcb9fa676ece30ce856d5ea4))

## [3.6.6](https://github.com/MAnders333/knowledge-server/compare/v3.6.5...v3.6.6) (2026-03-26)


### Bug Fixes

* **extraction:** extend user-specific detail rule to usernames and account names ([53c59d9](https://github.com/MAnders333/knowledge-server/commit/53c59d9a68add0996dc9d60e29a6f270380c1b66))
* **extraction:** replace enumerated user-specific list with a general principle ([3c59d93](https://github.com/MAnders333/knowledge-server/commit/3c59d936ff3ed0bb5c099806f221d9e7724abd09))

## [3.6.5](https://github.com/MAnders333/knowledge-server/compare/v3.6.4...v3.6.5) (2026-03-26)


### Bug Fixes

* **extraction:** generalise local-path and domain routing rules ([c49c1e7](https://github.com/MAnders333/knowledge-server/commit/c49c1e766b66abd8a2053f3bf4086df3d16bdc01))
* **extraction:** improve prompt — local paths, episode independence ([48a8252](https://github.com/MAnders333/knowledge-server/commit/48a82522a432a49ad2f6b4dfce566e53f710da80))
* **extraction:** local paths route to personal domain, not excluded entirely ([e81da25](https://github.com/MAnders333/knowledge-server/commit/e81da253a014e600a7763a5e0342705fd98d647e))

## [3.6.4](https://github.com/MAnders333/knowledge-server/compare/v3.6.3...v3.6.4) (2026-03-26)


### Bug Fixes

* resolve vscode-remote:// workspace URIs for SSH and Dev Container sessions ([3d29fed](https://github.com/MAnders333/knowledge-server/commit/3d29fedf78bdd0675606417a82784bcdcee37ff1))
* resolve vscode-remote:// workspace URIs for SSH and Dev Container sessions ([835e47f](https://github.com/MAnders333/knowledge-server/commit/835e47fe41a1a095c5e0356eac1766f7417bc10f))
* resolve workspace root for Cursor sessions to prevent cross-project contamination ([8449a28](https://github.com/MAnders333/knowledge-server/commit/8449a28d3a18e86f47ad3c237ba5cc2198c5dc23))
* resolve workspace root for Cursor sessions to prevent cross-project contamination ([d88f6df](https://github.com/MAnders333/knowledge-server/commit/d88f6df38d9eb611daffedd9453583dacc889e42))
* return empty string on Dev Container decode failure and unknown schemes; add tests ([c9c2489](https://github.com/MAnders333/knowledge-server/commit/c9c2489eec7cd03a79646c6a01e92b40aa9f6023))

## [3.6.3](https://github.com/MAnders333/knowledge-server/compare/v3.6.2...v3.6.3) (2026-03-25)


### Bug Fixes

* add mock.restore() to afterEach in the ConsolidationEngine lock suite. ([5878478](https://github.com/MAnders333/knowledge-server/commit/58784781d5c9d0b3a5f363add3001674e360b4ac))
* cast bigint OID to Number for postgres.js template parameter ([698e66f](https://github.com/MAnders333/knowledge-server/commit/698e66fdc727ba675518400d612451d18c3c0231))
* OID bigint cast, releaseConsolidationLock try/finally, stale JSDoc ([0174bd5](https://github.com/MAnders333/knowledge-server/commit/0174bd5b3a6bde993bbdccd2470ae8b74cac5d9a))
* **tests:** restore prototype spies in afterEach to prevent cross-file leakage ([5878478](https://github.com/MAnders333/knowledge-server/commit/58784781d5c9d0b3a5f363add3001674e360b4ac))

## [3.6.2](https://github.com/MAnders333/knowledge-server/compare/v3.6.1...v3.6.2) (2026-03-25)


### Bug Fixes

* **consolidation:** checkPending() calls prepare() for accurate session count ([5f9c691](https://github.com/MAnders333/knowledge-server/commit/5f9c6919f1ec21bc6c87df67d9b727528e35135b))
* **consolidation:** checkPending() reads countPendingSessions() directly ([b140f90](https://github.com/MAnders333/knowledge-server/commit/b140f907dd558765141540d2c7654a1dbc37feeb))

## [3.6.1](https://github.com/MAnders333/knowledge-server/compare/v3.6.0...v3.6.1) (2026-03-25)


### Bug Fixes

* **consolidation:** restore within-batch dedup; remove double JSDoc ([9defc8f](https://github.com/MAnders333/knowledge-server/commit/9defc8f35696dbd519b2fa92ac7883df89654613))


### Performance Improvements

* **consolidation:** batch decideMerge — N LLM calls → 1 per chunk ([880883b](https://github.com/MAnders333/knowledge-server/commit/880883b9c7c9392e8a896ea0941bc638d897095b))
* **llm:** switch contradictionModel default from sonnet to haiku ([9e55a7a](https://github.com/MAnders333/knowledge-server/commit/9e55a7a6d19c857a0ea7bb80e1aba58f3e777d07))

## [3.6.0](https://github.com/MAnders333/knowledge-server/compare/v3.5.1...v3.6.0) (2026-03-25)


### Features

* **setup-tool:** auto-register daemon as system service when setting up AI tools ([d496cde](https://github.com/MAnders333/knowledge-server/commit/d496cde5487addd8fb5b6aa55563de97b44564c0))


### Bug Fixes

* **consolidate:** write CLI consolidation logs to server.log ([b677fd2](https://github.com/MAnders333/knowledge-server/commit/b677fd2b9d64f1fe856db59b9909816b3988e5d5))

## [3.5.1](https://github.com/MAnders333/knowledge-server/compare/v3.5.0...v3.5.1) (2026-03-24)


### Bug Fixes

* **activation:** raise default similarity threshold from 0.30 to 0.42 ([5554fd1](https://github.com/MAnders333/knowledge-server/commit/5554fd18ed4992389c049cf2b676d7fb5c533baf))

## [3.5.0](https://github.com/MAnders333/knowledge-server/compare/v3.4.2...v3.5.0) (2026-03-24)


### Features

* remove scope field; strengthen domain descriptions; fix description length limit ([11af789](https://github.com/MAnders333/knowledge-server/commit/11af789e45c07cee0c1a606d009936d19e777f5e))

## [3.4.2](https://github.com/MAnders333/knowledge-server/compare/v3.4.1...v3.4.2) (2026-03-24)


### Bug Fixes

* **uploader:** split getProcessedEpisodeRanges from getUploadedEpisodeRanges ([c0093cf](https://github.com/MAnders333/knowledge-server/commit/c0093cf8e4f205dfd34957efb1c06a49a75e4911))

## [3.4.1](https://github.com/MAnders333/knowledge-server/compare/v3.4.0...v3.4.1) (2026-03-24)


### Bug Fixes

* **daemon:** drain on any upload, not just full batches ([245da37](https://github.com/MAnders333/knowledge-server/commit/245da372b4f1e5693f6d15d61e42a308a4ea7229))

## [3.4.0](https://github.com/MAnders333/knowledge-server/compare/v3.3.0...v3.4.0) (2026-03-24)


### Features

* **daemon:** concurrent source uploads; drain backlog without waiting ([7317181](https://github.com/MAnders333/knowledge-server/commit/731718187ce5aa5a3f27e4d923bb65e3928570f8))

## [3.3.0](https://github.com/MAnders333/knowledge-server/compare/v3.2.0...v3.3.0) (2026-03-24)


### Features

* **daemon-db:** split daemon_cursor into DaemonDB; add PostgresServerStateDB ([d630ab4](https://github.com/MAnders333/knowledge-server/commit/d630ab4f7e347ad0aaa93b5499defd48c59a5009))


### Bug Fixes

* **daemon-db:** address post-merge reviewer findings ([f655796](https://github.com/MAnders333/knowledge-server/commit/f65579657df6fc9c871c94980ca594b1418c5a38))
* **migration:** add transaction, simplify table check, fix key prefix convention ([c6c9d18](https://github.com/MAnders333/knowledge-server/commit/c6c9d18d86f226a6201049e484ae959bf048b662))
* **migration:** drop orphaned daemon_cursor from state.db on upgrade ([dc5a4c7](https://github.com/MAnders333/knowledge-server/commit/dc5a4c7e5956946870478f7d3051b87166022fc9))
* **postgres:** remove unused content_type column from getProcessedEpisodeRanges query ([67139ab](https://github.com/MAnders333/knowledge-server/commit/67139abf8bb062439e94aa3287ff53dc3c8b9c0d))
* **schema:** align id patterns with runtime validator; fix schema self-ref ([2e67388](https://github.com/MAnders333/knowledge-server/commit/2e673887265161ca03c8e8d47610b6bb26ece24a))
* **types:** fix typecheck errors and address cleanliness audit findings ([f5e1f0a](https://github.com/MAnders333/knowledge-server/commit/f5e1f0adc1b896a1b49e8f8405ad0791f3e9e297))

## [3.2.0](https://github.com/MAnders333/knowledge-server/compare/v3.1.0...v3.2.0) (2026-03-24)


### Features

* **reinitialize:** granular reset flags — daemon cursor, state, and store ([4cb9935](https://github.com/MAnders333/knowledge-server/commit/4cb9935e82057e18940520b31324cfaf53bcb455))


### Bug Fixes

* **api:** extend cross-store guard to delete resolution; document decayStores/writableDbs coupling ([688ea52](https://github.com/MAnders333/knowledge-server/commit/688ea52f0bfb7522ece384c886f9e15428a908fb))
* **consolidation:** per-store episode recording; fix empty-extraction recording ([20a26ce](https://github.com/MAnders333/knowledge-server/commit/20a26cefca683a7b426cdc0f8422d51a49ac1aa2))
* **consolidation:** use Promise.allSettled for per-store concurrent dispatch ([430c775](https://github.com/MAnders333/knowledge-server/commit/430c775be20b7c28557968828cb12b36f1e5658f))
* **contradiction:** enforce within-store-only contradiction scanning ([f826f4e](https://github.com/MAnders333/knowledge-server/commit/f826f4e050c8e8a32ded1433a33029cee06b1f18))
* **multi-store:** fan out decay, embeddings, re-embed, and API reads ([514676b](https://github.com/MAnders333/knowledge-server/commit/514676bf5184bfd94ac655126a03e5fb63ae7471))
* **multi-store:** fan out review/status/reinitialize CLI and API to all stores ([8d0d788](https://github.com/MAnders333/knowledge-server/commit/8d0d788fa56ea65c3580bd8802a9cda01b2ba8d8))
* **reinitialize:** align PID liveness check with stop.ts; fix storeFlag in suggest ([d1d287c](https://github.com/MAnders333/knowledge-server/commit/d1d287c50e55ccc35b7765feed06b728368ca017))
* **reinitialize:** fix EPERM liveness check inversion; hoist server guard ([034a196](https://github.com/MAnders333/knowledge-server/commit/034a1960c91598dceb93733d05b2f32275c3136c))
* **review:** assert store invariant instead of silent fallback; fix NaN in /status stats ([aa88206](https://github.com/MAnders333/knowledge-server/commit/aa88206487ed1015d574b7aa0edc058436ebaaf7))
* **types:** replace NodeJS.ErrnoException with Bun-safe pattern in stop+reinitialize ([1f936cd](https://github.com/MAnders333/knowledge-server/commit/1f936cd2a0c180e373b950f490a29e3d057919c2))

## [3.1.0](https://github.com/MAnders333/knowledge-server/compare/v3.0.4...v3.1.0) (2026-03-23)


### Features

* **synthesis:** scope runSynthesis to touched stores per consolidation run ([787e1fa](https://github.com/MAnders333/knowledge-server/commit/787e1fa796d3e5e216cd9c7b66bff0fd17e883c5))
* **synthesis:** track touched stores per run; gate multi-store synthesis on follow-up ([9890375](https://github.com/MAnders333/knowledge-server/commit/98903752ae873910b0244a248e20c6dcc6901f6b))


### Bug Fixes

* **consolidation:** address holistic review findings ([e3f09e4](https://github.com/MAnders333/knowledge-server/commit/e3f09e42174ff599d122955df2a4b7f84662341c))
* **contradiction:** route scanner to correct domain store per chunk ([6e52ed5](https://github.com/MAnders333/knowledge-server/commit/6e52ed5a138c51c3233a161d485372ebb3e87bc7))
* **migration:** seed daemon_cursor from source_cursor on pre-v12 upgrade ([f5ff28b](https://github.com/MAnders333/knowledge-server/commit/f5ff28bb0767cda1b1b4e73c2c49a74b72c8f32c))
* **synthesis:** add mergeDb param to reconsolidate(); enable per-store synthesis ([5c4942e](https://github.com/MAnders333/knowledge-server/commit/5c4942ec4598070f47e58da5a4ab7b706a7f9e9a))

## [3.0.4](https://github.com/MAnders333/knowledge-server/compare/v3.0.3...v3.0.4) (2026-03-23)


### Bug Fixes

* **llm:** remove hardcoded domain names from extraction prompt example ([93f739f](https://github.com/MAnders333/knowledge-server/commit/93f739f0ff535119eab845c13e9ae35f0f8d2c78))

## [3.0.3](https://github.com/MAnders333/knowledge-server/compare/v3.0.2...v3.0.3) (2026-03-23)


### Bug Fixes

* drop staging tables in PG hard-reset path; update stale test labels ([542da87](https://github.com/MAnders333/knowledge-server/commit/542da8754e5dc96528d0157712a74d56a438cf68))
* run migrateFromKnowledgeDb before store initialization ([cc054e0](https://github.com/MAnders333/knowledge-server/commit/cc054e09fac5717b7bd9e1c4c4bdfb50afe1a205))
* rw handle leak in drop phase; update stale migrateFromKnowledgeDb comments ([642b67e](https://github.com/MAnders333/knowledge-server/commit/642b67e53e7e11df6458a4a312ddb0dad8b31b5b))
* split migration into two independent idempotent steps; atomic transaction guard ([a841d1d](https://github.com/MAnders333/knowledge-server/commit/a841d1d84f7e01cd18ecef4af51ce2aee1b4fe9e))

## [3.0.2](https://github.com/MAnders333/knowledge-server/compare/v3.0.1...v3.0.2) (2026-03-23)


### Bug Fixes

* add STORE_ and DAEMON_ prefixes to .env allowlist ([ad4e6f0](https://github.com/MAnders333/knowledge-server/commit/ad4e6f02e3ac6d1607664af84b22da659e1073f9))
* architectural cleanup post server.db split ([35d476c](https://github.com/MAnders333/knowledge-server/commit/35d476ccca2a325fe00f2daf74feed330007a3e1))
* countPendingSessions() for O(1)-memory pending count in status ([d09d755](https://github.com/MAnders333/knowledge-server/commit/d09d755c69f36f1443a4ff01b24ef0ccae3fdc4e))
* cursor only advances past successfully uploaded episodes; close src DB in finally ([d6fea91](https://github.com/MAnders333/knowledge-server/commit/d6fea910ed330beecd0511706099730d890ce6ae))
* daemon log lines double-stamped in server log file ([750a414](https://github.com/MAnders333/knowledge-server/commit/750a414f1e0bfb3c7e627aa67cfcce201512e7cc))
* flush TextDecoder carry-over buffer after daemon stream closes ([3472d1b](https://github.com/MAnders333/knowledge-server/commit/3472d1bd1fec5bc1f2f2b8b98742c845700654bd))
* optional chaining in migration test afterEach ([cd7d08e](https://github.com/MAnders333/knowledge-server/commit/cd7d08e58739c7c534e240a195086c2e75a160c9))
* PG integration tests + post-refactor cleanup ([8d1389c](https://github.com/MAnders333/knowledge-server/commit/8d1389cd5b178d580b20e99fbf0f50d6d2214686))
* remove dead _SERVER_LOCAL_TABLE_COLUMNS; improve partial reinitialize note ([f598d0e](https://github.com/MAnders333/knowledge-server/commit/f598d0e9bd88cc65c4ef7db2eb7f7308806f26cd))
* revert clearConsolidatedEpisodes; partial reinitialize leaves staging intact ([d273eda](https://github.com/MAnders333/knowledge-server/commit/d273eda638bc847413da8a8cc762c9097447e7e2))
* status.ts no-limit pending count; setup-tool.ts warn on bad config.jsonc ([4f21d33](https://github.com/MAnders333/knowledge-server/commit/4f21d339f864789e95ed8c20e23d17e6c305d661))
* type cast and error handling in countPendingSessions ([47ed63f](https://github.com/MAnders333/knowledge-server/commit/47ed63f3c05c0c518176c25959ebb1681f36dc4b))
* use counter for ServerLocalDB filename uniqueness, surface close errors ([57c5414](https://github.com/MAnders333/knowledge-server/commit/57c5414e3c239fea3a9b4456856c6d14261cd759))

## [3.0.1](https://github.com/MAnders333/knowledge-server/compare/v3.0.0...v3.0.1) (2026-03-23)


### Bug Fixes

* log download start path and version, clarify silent comment ([ade27ec](https://github.com/MAnders333/knowledge-server/commit/ade27ecbbe2129ba3303c383d73a9bb782d2e299))
* suppress progress output in downloadBinary when called silently ([81a351f](https://github.com/MAnders333/knowledge-server/commit/81a351fd725ca6d8bead6df7f831dd641266d534))
* v prefix on version, merge import, fix cleanup ownership in downloadAndInstallDaemon ([cd9c731](https://github.com/MAnders333/knowledge-server/commit/cd9c731f14c7c3032a9ba8352281404b9a65916c))

## [3.0.0](https://github.com/MAnders333/knowledge-server/compare/v2.10.0...v3.0.0) (2026-03-23)


### ⚠ BREAKING CHANGES

* daemon-only consolidation, remove source_cursor and per-user scoping
* Configuration has moved from environment variables to ~/.config/knowledge-server/config.jsonc. Run 'knowledge-server migrate-config' after updating to generate the config file from your existing env vars.

### Features

* auto-spawn daemon, install both binaries, v3 migration notice ([3ba42b6](https://github.com/MAnders333/knowledge-server/commit/3ba42b63bd0a971e36dd82e5b748c5cc78833af7))
* daemon-only consolidation, remove source_cursor and per-user scoping ([baafd24](https://github.com/MAnders333/knowledge-server/commit/baafd24eca8fa2021ed7a0920e307e6df218bf97))
* **daemon:** episode uploader daemon — pending_episodes staging table + PendingEpisodesReader ([a0bce0e](https://github.com/MAnders333/knowledge-server/commit/a0bce0e5073ed3b5b885b24f67102cfb9c851bb7))
* **service:** add KnowledgeService with auto-re-embed and review CLI command ([1a40189](https://github.com/MAnders333/knowledge-server/commit/1a40189746aafd2bba63f0048b70a57c3b3965c8))
* **stores:** degraded-mode startup when stores are unreachable ([32f88b4](https://github.com/MAnders333/knowledge-server/commit/32f88b440f82d27966482f0902b0e4ba66841a4b))
* v3.0.0 — N-store architecture, domain routing, daemon, multi-user ([1cc4fec](https://github.com/MAnders333/knowledge-server/commit/1cc4fec2550e03663bea5fc7c5360662ade850b8))


### Bug Fixes

* always print auto-spawn note after daemon service registration ([bf6144e](https://github.com/MAnders333/knowledge-server/commit/bf6144ef5bf07837d81edcbd452a4335da6db412))
* checkAndReEmbed metadata after loop; domain-aware chunk grouping; new tests ([ec7de8b](https://github.com/MAnders333/knowledge-server/commit/ec7de8b4f2e0235b443ba79516ccb9d30ca3ffa3))
* clean up stale comments after getPendingEpisodes source removal ([c26af6c](https://github.com/MAnders333/knowledge-server/commit/c26af6cf7155480d723c30123a03c5d442c03b0a))
* co-locate help-advanced with --help; clarify embedding doc ([83887ec](https://github.com/MAnders333/knowledge-server/commit/83887ecbeec6a353266466be3b8a84200cc8c5c4))
* **config:** allow multiple writable stores when domains are configured ([93fe487](https://github.com/MAnders333/knowledge-server/commit/93fe487260edaad27300ed4b6c989e1c1f3ac434))
* **consolidation:** guard prepare() hook with try/catch so a failing reader skips rather than aborting the run ([4c6db18](https://github.com/MAnders333/knowledge-server/commit/4c6db18482815e8ed39c528e04dccfda2b9d4af1))
* **daemon:** correct userId resolution order in comment ([5821fcc](https://github.com/MAnders333/knowledge-server/commit/5821fcc7d2be54b9b2ef21bbb8c9d777c33ca0c3))
* **daemon:** process.on over once; reset cache state in prepare() ([e5535a3](https://github.com/MAnders333/knowledge-server/commit/e5535a3eff229f1a4581393deeba8bacc2e532c6))
* **daemon:** resolve userId from registry instead of config.ts (no userId field there) ([a4bf717](https://github.com/MAnders333/knowledge-server/commit/a4bf7170efd2ad5926132928fc546f6c283d6e68))
* **daemon:** set _prepared only after successful cache population in prepare() ([9a56662](https://github.com/MAnders333/knowledge-server/commit/9a566627f07c09ce2bc47135970b5a7454450679))
* **domains:** address reviewer findings ([0aa3a61](https://github.com/MAnders333/knowledge-server/commit/0aa3a61b889599e8964b974e643b880af906df03))
* **domains:** flatten IIFE anti-pattern; clarify targetDb comment in reconsolidate ([7fc887e](https://github.com/MAnders333/knowledge-server/commit/7fc887ecee940448f6c0d8e23c699f81b5d95835))
* **domains:** remove redundant parsed null-check in hallucination warn loop ([2bad7f9](https://github.com/MAnders333/knowledge-server/commit/2bad7f9aab71e9777b3996d80a3662aa3c2317fd))
* **domains:** revert JSON comment in LLM prompt; fix newline regex; blank line ([8fdafa9](https://github.com/MAnders333/knowledge-server/commit/8fdafa96729ceb7a1bd22a3404cb4738119abd29))
* graceful shutdown blocked by idle polling loop sleep ([e6f168e](https://github.com/MAnders333/knowledge-server/commit/e6f168e4c3ef2e59ae313d60c8825e80f9470203))
* host security check post-registry, shared parsePortEnvVar helper, test coverage ([4b8d3b0](https://github.com/MAnders333/knowledge-server/commit/4b8d3b0f0b6459a2d7a6565020c78c53ba5940df))
* improve install.sh env detection and hints ([1d5f6a7](https://github.com/MAnders333/knowledge-server/commit/1d5f6a75ced0e460064d604143eaebc49faf0e3b))
* **lock:** catch releaseConsolidationLock error in close() so sql.end() always runs ([43e5822](https://github.com/MAnders333/knowledge-server/commit/43e582259255ec078eadecbcfae89efade891095))
* **lock:** close() releases lock connection; fix warnIfMixedTopology type+URI ([aa83e75](https://github.com/MAnders333/knowledge-server/commit/aa83e75e2d4368bfe23b7963774ffa90edf62ac5))
* **lock:** handle sql.reserve() throw before try block to prevent pool leak ([fef105b](https://github.com/MAnders333/knowledge-server/commit/fef105b0ded6ee1a0a30e0a46be1b4515b6d2f9f))
* **multi-user:** address reviewer findings ([132e40d](https://github.com/MAnders333/knowledge-server/commit/132e40dc6575e226478562f4550d5f0935ad3a8e))
* **multi-user:** fail loudly on missing PK in PG migration; gate userId behind admin ([01472f4](https://github.com/MAnders333/knowledge-server/commit/01472f42f2eddf36ffddfb04a2f89d53fe037129))
* **multi-user:** fix remaining stale USER_ID refs; clean up minor code style ([6dd3368](https://github.com/MAnders333/knowledge-server/commit/6dd336881c5c63beef9ae8e1f110a5474e862fe4))
* **pg:** add user_id to PG_CREATE_TABLES for fresh DB bootstrap ([2754058](https://github.com/MAnders333/knowledge-server/commit/2754058c7be411cd9c527f709fc7d79bd2e8b942))
* **pg:** specify explicit conflict target in recordEpisode ON CONFLICT clause ([cd9b2d2](https://github.com/MAnders333/knowledge-server/commit/cd9b2d2cebe3b9d04dd3b860d66d7c83f1641ba8))
* properly delete env vars in test teardown using Reflect.deleteProperty ([90b490e](https://github.com/MAnders333/knowledge-server/commit/90b490ee4574b46840f3f2c8af24fdc679dcec93))
* **refactor:** fix daemon path in setup-tool; use process.execPath for binary dir ([a121d22](https://github.com/MAnders333/knowledge-server/commit/a121d2262a4f544c7e1e4831d32101d4fc8a03a9))
* **refactor:** fix self-referential import in daemon/index.ts; add PendingEpisodesReader note ([cb32bbb](https://github.com/MAnders333/knowledge-server/commit/cb32bbb7d7a35556fbed37c5b0a5062d08678093))
* resolve all typecheck errors (bun build silently passed these) ([4726848](https://github.com/MAnders333/knowledge-server/commit/4726848a94a7e53a717b78fda955d958f421d0a5))
* restore daemonBin declaration accidentally deleted in previous commit ([f6ed8d3](https://github.com/MAnders333/knowledge-server/commit/f6ed8d3e93292296b7de5e07c83f8864c92a455f))
* **service:** address post-merge review warnings ([39300b1](https://github.com/MAnders333/knowledge-server/commit/39300b122dcfc9d0ba2c5b4f7baa51b7c7727671))
* show correct auto-spawn note on service registration success vs failure ([112ee06](https://github.com/MAnders333/knowledge-server/commit/112ee064b3636a6524fe83db49300c0fb4c75430))
* **stores:** address reviewer findings — database.ts fallback, migrate-config injection ([cfd526b](https://github.com/MAnders333/knowledge-server/commit/cfd526b7ea00e8825104018adc300b6279136a9c))
* **stores:** address reviewer warnings ([7cc0fa7](https://github.com/MAnders333/knowledge-server/commit/7cc0fa791e23e0288d91d05012d306ca98582a7a))
* **stores:** derive fallback store ID from map instead of hardcoding 'default' ([cef729b](https://github.com/MAnders333/knowledge-server/commit/cef729bc609ad3e56a524ae26e8f1e15fd4e3f93))
* **stores:** tighten domain-router warn message for fallback store lookup ([3e33d7d](https://github.com/MAnders333/knowledge-server/commit/3e33d7d1128e3fa9e930fe83fc7e4b3037983ae0))
* **stores:** tighten duplicate-id test assertion and clarify checkAndReEmbed JSDoc ([87cee74](https://github.com/MAnders333/knowledge-server/commit/87cee74749358ea34b5a7db2d29d37dd723f69cf))
* **stores:** warn when fallbackStore not found in stores map ([2508a33](https://github.com/MAnders333/knowledge-server/commit/2508a3380d67b9cc6afe978690f39de7d1a04cee))
* **tests:** add missing userId args in pg-integration reinitialize test ([a6a6c3a](https://github.com/MAnders333/knowledge-server/commit/a6a6c3adb4cfb668536a912687448e0ad960df1d))
* **types:** resolve two tsc errors flagged by bun run typecheck ([82c6a05](https://github.com/MAnders333/knowledge-server/commit/82c6a053241009fa645e5818ebb2c04c81a5db2f))
* **types:** update stale null-path comments after domainId null→undefined change ([bff9f00](https://github.com/MAnders333/knowledge-server/commit/bff9f00660e46fc7efe92169e2ccfa23df31ec89))
* **types:** use undefined over null for DomainResolution.domainId ([12245e7](https://github.com/MAnders333/knowledge-server/commit/12245e7bb5bf7271299d154b5cc96d9bf97e1a38))
* warn about DAEMON_AUTO_SPAWN when registering daemon as system service ([30fc103](https://github.com/MAnders333/knowledge-server/commit/30fc10312c7b4596d7f5a83708145b0c7493d19c))

## [2.10.0](https://github.com/MAnders333/knowledge-server/compare/v2.9.0...v2.10.0) (2026-03-12)


### Features

* **db:** add PostgreSQL backend as alternative to SQLite ([27cc669](https://github.com/MAnders333/knowledge-server/commit/27cc6698b703f85963879fe6eb2a45e311de24cd))
* **db:** add PostgreSQL backend as alternative to SQLite ([27cc669](https://github.com/MAnders333/knowledge-server/commit/27cc6698b703f85963879fe6eb2a45e311de24cd))
* **db:** add PostgreSQL backend as alternative to SQLite ([a767608](https://github.com/MAnders333/knowledge-server/commit/a7676084700be562785a3e6533732855e5ed48c0))


### Bug Fixes

* **db:** address review findings in PostgresKnowledgeDB ([38591a8](https://github.com/MAnders333/knowledge-server/commit/38591a85b952065920d6ed94b40048f82336cf78))
* **db:** build array param outside transaction in persistClusters ([48dcd8c](https://github.com/MAnders333/knowledge-server/commit/48dcd8cbb4b8f3deb5be61c602a8cac710f98bf1))
* **db:** drop string type arg from sql.array — expects OID number ([cde5f17](https://github.com/MAnders333/knowledge-server/commit/cde5f17b8fc121f596d1ce2e0e8e31a0a6fa6059))
* **db:** harden PostgresKnowledgeDB and wire PG into CI ([b535c10](https://github.com/MAnders333/knowledge-server/commit/b535c10811d454db2dc01304e83b84d53e4ca502))
* **test:** correct TRUNCATE comment — knowledge_relation cascades from knowledge_entry ([8fdfc55](https://github.com/MAnders333/knowledge-server/commit/8fdfc55c2f737eee817a65463992659833c02e08))
* **test:** restore knowledge_cluster to TRUNCATE list in pg-integration ([47486d5](https://github.com/MAnders333/knowledge-server/commit/47486d57e780e3c87a9a53e8b70599470f4554c6))

## [2.9.0](https://github.com/MAnders333/knowledge-server/compare/v2.8.0...v2.9.0) (2026-03-12)


### Features

* **extraction:** encode historical data quality events explicitly ([d668aea](https://github.com/MAnders333/knowledge-server/commit/d668aea21cb8ac05ec87f6044d71293c1cbe41fe))
* **extraction:** source density assessment before extraction ([b784014](https://github.com/MAnders333/knowledge-server/commit/b78401440635aaaaf6fe33111dc9a359f758a9b7))


### Bug Fixes

* **extraction:** anchor mixed-content rule to authorial-intent test ([2c05b65](https://github.com/MAnders333/knowledge-server/commit/2c05b65ba3bbe94e33657d16e5c49b78285a6bee))
* **extraction:** generic examples and type hint for data quality events ([328bd98](https://github.com/MAnders333/knowledge-server/commit/328bd98a3b60c6f265352c654c459e51fd48fcca))
* **extraction:** handle mixed-content episodes in density assessment ([67634b2](https://github.com/MAnders333/knowledge-server/commit/67634b286b7adde386bd9e52d51dcc223bad6116))
* **extraction:** restore authorial-intent tie-breaker in mixed-content clause ([4e3741d](https://github.com/MAnders333/knowledge-server/commit/4e3741dd5532fee359a6e6c05275ab3d425f127c))
* **logging:** full content in insert/update logs, Untitled fallback in cursor+vscode readers ([d2a77a4](https://github.com/MAnders333/knowledge-server/commit/d2a77a4489d429fa1cce77706668b88ff98c2afc))
* **logging:** revert JSON.stringify on type field, add clarifying comment ([6ad6c36](https://github.com/MAnders333/knowledge-server/commit/6ad6c3634f849fd4192676a18e7686a070177059))

## [2.8.0](https://github.com/MAnders333/knowledge-server/compare/v2.7.0...v2.8.0) (2026-03-12)


### Features

* **sources:** add local-files source for Markdown knowledge ingestion ([2fb698d](https://github.com/MAnders333/knowledge-server/commit/2fb698dabd95eec124ba5aaef9b4e271537d6440))


### Bug Fixes

* **llm:** remove redundant (document) label in document episode header ([61a781e](https://github.com/MAnders333/knowledge-server/commit/61a781ed7bf63a1a759ff1be6e5e2d05778d6565))
* **local-files:** address post-review warnings ([27b7b5c](https://github.com/MAnders333/knowledge-server/commit/27b7b5ccbd4f7c3745155eb69d050ee884825634))
* **local-files:** explicit Dirent&lt;string&gt; type to satisfy strict tsc ([9e82976](https://github.com/MAnders333/knowledge-server/commit/9e829768d91e94ac47d536fea58ec1016a511278))

## [2.7.0](https://github.com/MAnders333/knowledge-server/compare/v2.6.3...v2.7.0) (2026-03-11)


### Features

* **schema:** add is_synthesized column to knowledge_entry (v10) ([e79caae](https://github.com/MAnders333/knowledge-server/commit/e79caae7c92988f75ce2fcb343bcbfdec97b62af))
* **synthesis:** expose entry origin to synthesis LLM ([02d1cee](https://github.com/MAnders333/knowledge-server/commit/02d1ceea913feabf20459ca32e805eb0c81c5483))
* **synthesis:** make cluster thresholds env-configurable; tighten defaults ([570cc0c](https://github.com/MAnders333/knowledge-server/commit/570cc0c4d09e548ee7209ad63ca5dbba38af5042))


### Bug Fixes

* **synthesis:** correctly propagate isSynthesized flag through all insert/update paths ([c711d8d](https://github.com/MAnders333/knowledge-server/commit/c711d8d8969491a742187ea3fd8036d9da554827))
* **tests:** update synthesis cluster tests for CLUSTER_MIN_MEMBERS=5 ([a2322a7](https://github.com/MAnders333/knowledge-server/commit/a2322a7c9123e81eeeda52c53869d47e182dbe18))

## [2.6.3](https://github.com/MAnders333/knowledge-server/compare/v2.6.2...v2.6.3) (2026-03-10)


### Bug Fixes

* **api:** re-embed patched entries and refresh docs ([ccdac7e](https://github.com/MAnders333/knowledge-server/commit/ccdac7e42cf7d3fc8f24304d152cc66c179d8674))

## [2.6.2](https://github.com/MAnders333/knowledge-server/compare/v2.6.1...v2.6.2) (2026-03-09)


### Bug Fixes

* **consolidate-cmd:** run synthesis even when no sessions pending; guard synthesis errors ([49729c5](https://github.com/MAnders333/knowledge-server/commit/49729c54dd5d17feefec5720e5fdc2886259f05c))
* **consolidation:** add synthesis to CLI command; fix test guards; run synthesis unconditionally after drain ([d83511d](https://github.com/MAnders333/knowledge-server/commit/d83511ded7c9a4009136b3a4df311f0fb236638e))
* **opencode-reader:** exclude synthetic (injected) text parts from consolidation ([2d8375b](https://github.com/MAnders333/knowledge-server/commit/2d8375b9f89a8cfc93c7de657c675f4d0fc6d9a9))
* **synthesis:** count onUpdate path toward synthesized total ([9d8cf54](https://github.com/MAnders333/knowledge-server/commit/9d8cf54af327f0f0d8682ee2936273d40817572f))
* **synthesis:** guard per-result reconsolidate() in try/catch; fix result.reason log injection; fix indentation regressions; extract stale threshold constant; O(n) membership check ([7f0af66](https://github.com/MAnders333/knowledge-server/commit/7f0af661f526b70c0c7879f18912d96205167f21))
* **synthesis:** warn when all results for a cluster fail reconsolidation ([5367433](https://github.com/MAnders333/knowledge-server/commit/5367433e8ee0243645006cc72fc19299a68ca214))

## [2.6.1](https://github.com/MAnders333/knowledge-server/compare/v2.6.0...v2.6.1) (2026-03-09)


### Bug Fixes

* **logs:** route synthesis reconsolidation under [synthesis] prefix; wrap user content with JSON.stringify; fix switch-case indentation regression ([e97487b](https://github.com/MAnders333/knowledge-server/commit/e97487b22cf8765f0eabe978660b846d26fb6187))
* **stop:** replace misleading consolidation-specific drain message ([a5f8937](https://github.com/MAnders333/knowledge-server/commit/a5f8937fef9182561f569c5e4a346d767dd87fd2))

## [2.6.0](https://github.com/MAnders333/knowledge-server/compare/v2.5.0...v2.6.0) (2026-03-09)


### Features

* **config:** make reconsolidation similarity threshold configurable via env var ([b3d1fd9](https://github.com/MAnders333/knowledge-server/commit/b3d1fd9b61d31a8a1a85e5d1bf093304ef76455b))
* **status:** show version in knowledge-server status output ([51ef29a](https://github.com/MAnders333/knowledge-server/commit/51ef29a80f4ece6a9912cf4b8981b0909c930614))
* **synthesis:** cluster-first synthesis with persistent cluster tables and relation-aware activation ([ee02b76](https://github.com/MAnders333/knowledge-server/commit/ee02b7668229f6638073a65c6a1981618c4debdc))


### Bug Fixes

* **activation:** snapshot scored array before iterating to prevent cascade and maxResults violation ([f328fa2](https://github.com/MAnders333/knowledge-server/commit/f328fa2de66da0fb1c5ea78374471b74110e4957))
* **config:** guard liveReconsolidationThreshold against out-of-range values ([d2b282e](https://github.com/MAnders333/knowledge-server/commit/d2b282e7f167a4f5810c5f61a482f191d53c4847))
* **contradiction:** address post-review findings from batching refactor ([4672842](https://github.com/MAnders333/knowledge-server/commit/4672842ac4c32d8207fbef87d08391585cd63ab8))
* **contradiction:** batch LLM calls to prevent timeouts as KB grows ([ba4c3d1](https://github.com/MAnders333/knowledge-server/commit/ba4c3d1b487938d15f4e5a8d992b082a3c02fa95))
* iterate a [...scored] snapshot; add outer-loop maxResults break. ([f328fa2](https://github.com/MAnders333/knowledge-server/commit/f328fa2de66da0fb1c5ea78374471b74110e4957))
* **types:** resolve tsc TS2322 in activate.ts; remove dead KnowledgeType import; clean stale comments ([638d9d3](https://github.com/MAnders333/knowledge-server/commit/638d9d3ab5a5444e72eeac6f53cba86d5e131bab))

## [2.5.0](https://github.com/MAnders333/knowledge-server/compare/v2.4.1...v2.5.0) (2026-03-09)


### Features

* **consolidation:** cross-session synthesis via observation threshold ([42e480b](https://github.com/MAnders333/knowledge-server/commit/42e480bcc8791a696a9fd5ba7970be97bcc76202))
* **embedding:** auto re-embed all entries when embedding model changes ([5576a91](https://github.com/MAnders333/knowledge-server/commit/5576a9158a85fe2f5b33eb8410b1f42f0323332e))
* **embedding:** auto re-embed all entries when embedding model changes ([#30](https://github.com/MAnders333/knowledge-server/issues/30)) ([1fa5e82](https://github.com/MAnders333/knowledge-server/commit/1fa5e820f540fa87b24de1b19d8218130fe274ef))
* **synthesis:** add dedicated LLM_SYNTHESIS_MODEL config slot ([0920f4d](https://github.com/MAnders333/knowledge-server/commit/0920f4d94e6a8017967ef576144efdc88c25e317))
* **synthesis:** replace per-entry trigger with KB-wide cluster synthesis pass ([145daef](https://github.com/MAnders333/knowledge-server/commit/145daefb728156f653effa92c2e23b5cc32e58e8))


### Bug Fixes

* **embedding:** address post-merge review findings ([8d88e5d](https://github.com/MAnders333/knowledge-server/commit/8d88e5db8ab19e3a9ab6c01134c5902dd319f6f1))
* **embedding:** address review findings — correct v7/v8 references, harden migrations ([09a715e](https://github.com/MAnders333/knowledge-server/commit/09a715e747d7f73de1c7684c1fd137fa040d0e72))
* **embedding:** re-embed in-place to avoid broken activation on failure ([f9dee69](https://github.com/MAnders333/knowledge-server/commit/f9dee69b93a48fddce8ff897a08236976620c445))
* **stop:** show progress message when draining in-flight consolidation ([4144956](https://github.com/MAnders333/knowledge-server/commit/4144956bdb2dcce44878cee8381e623c4f0cd752))
* **synthesis:** address post-merge reviewer warnings ([1313cbd](https://github.com/MAnders333/knowledge-server/commit/1313cbd35e87869259cac7daf97e76e8f593b7ad))
* **synthesis:** address reviewer criticals + embeddings timing gap ([ab02062](https://github.com/MAnders333/knowledge-server/commit/ab02062926d21b68c727f1764e4edfd89789caef))
* **synthesis:** update stale attemptSynthesis comment to runKBSynthesis ([d106548](https://github.com/MAnders333/knowledge-server/commit/d10654827f10a56dd2f2d8bface5603142bedd1b))

## [2.4.1](https://github.com/MAnders333/knowledge-server/compare/v2.4.0...v2.4.1) (2026-03-08)


### Bug Fixes

* apply newline cue-splitting to claude-code hook ([f0eee5e](https://github.com/MAnders333/knowledge-server/commit/f0eee5e988f64d0aa6625b7d9b1c2af30685f909))
* **logging:** log full query and entry content in activation log ([f90f5b5](https://github.com/MAnders333/knowledge-server/commit/f90f5b5ba299beb7717f2d81d7496cf5dcbf19de))
* **stop:** raise poll timeout to 35s to outlast server's 30s shutdown drain ([0df09ff](https://github.com/MAnders333/knowledge-server/commit/0df09ffe632331d17d90d1cee80a3107db7969a5))

## [2.4.0](https://github.com/MAnders333/knowledge-server/compare/v2.3.0...v2.4.0) (2026-03-08)


### Features

* **logging:** add activation logging to all call sites ([c90dd41](https://github.com/MAnders333/knowledge-server/commit/c90dd4125dfa0576e7a5efd7bab61ee38102e25a))


### Bug Fixes

* remove duplicate log line in claude-code-hook activation path ([88877aa](https://github.com/MAnders333/knowledge-server/commit/88877aa8945974dc76280543d1f21065b331f86d))

## [2.3.0](https://github.com/MAnders333/knowledge-server/compare/v2.2.0...v2.3.0) (2026-03-08)


### Features

* **cli:** merge cli.ts subcommands into binary, add --help, update README ([eba6068](https://github.com/MAnders333/knowledge-server/commit/eba6068b304cf8d329629e2cd33fea1b9837f270))


### Bug Fixes

* address reviewer-flagged bugs from security hardening commit ([b187254](https://github.com/MAnders333/knowledge-server/commit/b1872549fbc0cda53c229852dd2dad4d356e6e28))
* **config:** handle missing baseURL for direct provider credentials ([0091cd7](https://github.com/MAnders333/knowledge-server/commit/0091cd7a4f384da00945fa12c9c097bd6847059f))
* correct reduce seed in chunkSessionTimestamp and sanitise log output ([da19b5a](https://github.com/MAnders333/knowledge-server/commit/da19b5aa1b1757113e8cabd84e6f1ea3347223c6))
* security hardening, CLI edge cases, and timestamp-aware entry decay ([c4a2e69](https://github.com/MAnders333/knowledge-server/commit/c4a2e69749c3b2a33647e7d823fa91b29c57097b))

## [2.2.0](https://github.com/MAnders333/knowledge-server/compare/v2.1.0...v2.2.0) (2026-03-07)


### Features

* **config:** support XDG config dir (~/.config/knowledge-server/.env) ([9b671df](https://github.com/MAnders333/knowledge-server/commit/9b671df05753b1d02f0e434f90ed13d16a5b9907))


### Bug Fixes

* **config:** detect placeholder credentials and point to correct .env path ([9c9dd4d](https://github.com/MAnders333/knowledge-server/commit/9c9dd4d0a592f392836ce4795ebc336ecc481f51))
* **startup:** handle port bind failure and stale PID file TOCTOU race ([67b5dc1](https://github.com/MAnders333/knowledge-server/commit/67b5dc15abaf8d33f0ae628ac970a1f03f96836e))
* **startup:** update banner to 'AI coding agents', fix PID file timing and EPERM handling ([87a1bcc](https://github.com/MAnders333/knowledge-server/commit/87a1bcc6c51084fcbfd6403c44fca59f8d347d46))
* **startup:** use .code for EADDRINUSE check instead of string matching ([62eb1f3](https://github.com/MAnders333/knowledge-server/commit/62eb1f32c0b3ea3523fc3e0547ba77220dbf5be5))
* **stop:** move PID guard before serve(), use readFileSync, handle EPERM ([9ca0b79](https://github.com/MAnders333/knowledge-server/commit/9ca0b795d6bb3672ab0733af7d8912fa634e9251))

## [2.1.0](https://github.com/MAnders333/knowledge-server/compare/v2.0.4...v2.1.0) (2026-03-07)


### Features

* add VSCode / GitHub Copilot Chat integration ([8253414](https://github.com/MAnders333/knowledge-server/commit/825341445626bf71f60ef6043d680963ec2ca7e6))
* add VSCode / GitHub Copilot Chat integration with tests and review fixes ([7ff1af1](https://github.com/MAnders333/knowledge-server/commit/7ff1af1760072c2d08338796ae60e3672f2243c6)), closes [#23](https://github.com/MAnders333/knowledge-server/issues/23)


### Bug Fixes

* **vscode:** remove dead parts type, document idempotency and kind allowlist, add tests ([0df62d6](https://github.com/MAnders333/knowledge-server/commit/0df62d6c4d0db1bfc4ef299e073f294bc588f7cd))

## [2.0.4](https://github.com/MAnders333/knowledge-server/compare/v2.0.3...v2.0.4) (2026-03-06)


### Bug Fixes

* only show MCP migration notice when upgrading from v1.x (&lt; v1.7.0) ([b67fcda](https://github.com/MAnders333/knowledge-server/commit/b67fcda41bd940f08250d00ff7c9bbdc3b19474d))
* use argv[2] for subcommand in both binary and source installs ([1375095](https://github.com/MAnders333/knowledge-server/commit/1375095e1d9fc55196174dedcc80854c7347eae1))

## [2.0.3](https://github.com/MAnders333/knowledge-server/compare/v2.0.2...v2.0.3) (2026-03-06)


### Bug Fixes

* correct argv offset for binary vs source installs; prevent mcp fall-through ([7248889](https://github.com/MAnders333/knowledge-server/commit/72488896c8dd032d804cd6ed87dd286e1dc17667))

## [2.0.2](https://github.com/MAnders333/knowledge-server/compare/v2.0.1...v2.0.2) (2026-03-06)


### Bug Fixes

* validate pkg.version format before interpolating into download URL ([bc83752](https://github.com/MAnders333/knowledge-server/commit/bc837529ff5882f5a02cc2774361def807750472))

## [2.0.1](https://github.com/MAnders333/knowledge-server/compare/v2.0.0...v2.0.1) (2026-03-06)


### Bug Fixes

* upload uncompressed binary alongside .gz so v1.x updaters can reach v2.0.0 ([7f514d5](https://github.com/MAnders333/knowledge-server/commit/7f514d5574858a71855eec4b1b87599da3b7896d))

## [2.0.0](https://github.com/MAnders333/knowledge-server/compare/v1.6.0...v2.0.0) (2026-03-06)


### ⚠ BREAKING CHANGES

* the separate knowledge-server-mcp binary no longer exists. MCP clients must use ['knowledge-server', 'mcp'] as the command instead of ['knowledge-server-mcp']. Running 'knowledge-server update' will automatically remove the obsolete binary and print instructions to re-run setup-tool. Users who have not yet run 'knowledge-server update' should run:   knowledge-server setup-tool <opencode|claude-code|cursor|codex>

### Features

* remove knowledge-server-mcp binary; MCP is now 'knowledge-server mcp' ([ba9b00f](https://github.com/MAnders333/knowledge-server/commit/ba9b00f29113dd93bbc280a4c2f983cfcfabe5f8))
* single binary, /mcp streamable-http endpoint, streaming update fix ([c3cd1fd](https://github.com/MAnders333/knowledge-server/commit/c3cd1fd8bd1a349145c31194db19cf1c38b862c7))


### Bug Fixes

* always show MCP migration notice when updating to v1.7.0+, not only when old binary found ([e1cc3df](https://github.com/MAnders333/knowledge-server/commit/e1cc3df0ab23a247633e27b48d16bd27bf1e5ed7))
* assert version regex match instead of silent ?? [] fallback ([4937ac2](https://github.com/MAnders333/knowledge-server/commit/4937ac26263d6056dc79ff32821b852e97f7109f))
* double-cast res.body through unknown to satisfy tsc (dom vs node ReadableStream types) ([821de4b](https://github.com/MAnders333/knowledge-server/commit/821de4bc8c0f6faabe8a175d713f2da20d5bb88c))
* restore Readable.fromWeb cast (non-null assertion blocked by biome) ([d2a7892](https://github.com/MAnders333/knowledge-server/commit/d2a78927d70300e97d29002ff3d5649a874e9a3b))
* use top-level Readable import in update.ts; remove ?? undefined noise in server.ts ([577ea9d](https://github.com/MAnders333/knowledge-server/commit/577ea9d97dd3cd4626beb749b800e1b4b4f62f7f))

## [1.6.0](https://github.com/MAnders333/knowledge-server/compare/v1.5.0...v1.6.0) (2026-03-06)


### Features

* add Codex CLI episode reader and setup-tool support for Cursor + Codex ([71119e7](https://github.com/MAnders333/knowledge-server/commit/71119e768d2353c04d1208e5ce6ffd3d72c572cd))
* add Cursor episode reader with cross-platform DB path resolution ([b8b69fd](https://github.com/MAnders333/knowledge-server/commit/b8b69fdc1e023fbb2512808a9d4b10aa76028848))


### Bug Fixes

* **cursor:** bound _sessionCache to selected sessions per cycle to prevent unbounded growth ([03b0833](https://github.com/MAnders333/knowledge-server/commit/03b08337ab19c72b2ace32fd4968c40aac327363))

## [1.5.0](https://github.com/MAnders333/knowledge-server/compare/v1.4.3...v1.5.0) (2026-03-05)


### Features

* add pre-flight token guard for extraction chunks, restore chunkSize default to 10 ([246e921](https://github.com/MAnders333/knowledge-server/commit/246e9217b6e5e6a80b884898cbaff9ace2fa4383))

## [1.4.3](https://github.com/MAnders333/knowledge-server/compare/v1.4.2...v1.4.3) (2026-03-04)


### Bug Fixes

* also use formatLlmError on final give-up log line ([f8b1f20](https://github.com/MAnders333/knowledge-server/commit/f8b1f20dca56ba3fc1c0888f4bbef4d80aaa2367))
* remove existing knowledge context from extractKnowledge, reduce chunkSize to 5 ([bc3a3a2](https://github.com/MAnders333/knowledge-server/commit/bc3a3a29f8b2076dc778af41eab5d28902854321))
* unwrap AI_RetryError to log actual HTTP status and response body ([846e851](https://github.com/MAnders333/knowledge-server/commit/846e851b7e51d93eb20c630cea7abfcc0779fb8c))

## [1.4.2](https://github.com/MAnders333/knowledge-server/compare/v1.4.1...v1.4.2) (2026-03-04)


### Bug Fixes

* remove MAX_TOOL_OUTPUT_CHARS; revert MAX_MESSAGE_CHARS to 60K; fix macOS checksum verification ([908df60](https://github.com/MAnders333/knowledge-server/commit/908df602e400f913956b86d2e68d83d53d6db0f3))

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
