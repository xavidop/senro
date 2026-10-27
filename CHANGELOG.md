# Changelog

All notable changes to senro are documented here. This file is generated from Conventional Commits by semantic-release.

## [1.5.2](https://github.com/xavidop/senro/compare/v1.5.1...v1.5.2) (2026-10-27)


### Bug Fixes

* **engine:** emit control.applied before a held step is let go ([8b47b5f](https://github.com/xavidop/senro/commit/8b47b5f9972500b23793559c4308d88dd5754cf7))

## [1.5.1](https://github.com/xavidop/senro/compare/v1.5.0...v1.5.1) (2026-10-27)


### Bug Fixes

* fetch the latest Node 22 for the site build ([8d3e4e3](https://github.com/xavidop/senro/commit/8d3e4e352f2686b6820ab9d9cc769c54e05f0a1c))

# [1.5.0](https://github.com/xavidop/senro/compare/v1.4.5...v1.5.0) (2026-10-27)


### Bug Fixes

* **api:** clear service readiness on retry and make a breakpoint hit set Held ([69b40d2](https://github.com/xavidop/senro/commit/69b40d268af4c94afc7a150dd228adbcb2409991))
* **api:** keep a recovered test recovered on replay or later pass ([a272216](https://github.com/xavidop/senro/commit/a272216d87ec89622b5e74eab69173bea74a48fe))
* **api:** say which a breakpoint.clear released and keep a still-held step held ([fe70dc8](https://github.com/xavidop/senro/commit/fe70dc8b78593be22b190a674a8e4a086efa1836))
* **api:** use a parseable retry predicate in the tests-flaky fixture ([c8664b5](https://github.com/xavidop/senro/commit/c8664b552a8c59dd8cb8291802eaaf9f415fad4b))
* **cache:** fold a consumer's direct services into its cache key ([dff72d5](https://github.com/xavidop/senro/commit/dff72d53470e0da83763cd087be194b1d96a1a0e))
* **cli:** build a pipeline from its own module directory ([0da54ea](https://github.com/xavidop/senro/commit/0da54ea5e2310b90ccafb9e8dd048de7ac758016))
* **cli:** keep loader and startup variables out of the offline shell ([b66fcac](https://github.com/xavidop/senro/commit/b66fcacec0a80a91e3a501d3a27a66df5c19da39))
* **cli:** senro init prints Run errors, scopes the host module, quotes its hint ([65166b1](https://github.com/xavidop/senro/commit/65166b1dd0cd24c367ee35460677ae575041b066))
* **cli:** senro init renders what it detected ([44e92a5](https://github.com/xavidop/senro/commit/44e92a5c6f9ce25e2121b0c81b1d4f6861a87702))
* **cli:** senro up prints a snapshot's endpoints only for a ready service ([b056c13](https://github.com/xavidop/senro/commit/b056c133266ed3e828b4ff1edaa5e3665349f283))
* **cli:** senro up recognises a refused start by its message, not the exit code ([106b8a2](https://github.com/xavidop/senro/commit/106b8a2b16853f971f3c7be658e208393ff203b5))
* **containerexec:** cut the stats stream after the container exits ([123d7e4](https://github.com/xavidop/senro/commit/123d7e4d80fe5b651acab16c213410d1e51067f0))
* **dispatcher:** close checks on every path, cancelled kills, token refresh ([63ab4fc](https://github.com/xavidop/senro/commit/63ab4fc1292ff7b5e38e68926d7320ee7a15abc4))
* **dispatcher:** only a cancelled context closes a check as cancelled ([38be1bc](https://github.com/xavidop/senro/commit/38be1bcaec1ef4b331f23ee8446b66df37738823))
* **dispatcher:** scope the installation token to one repository ([d0a1463](https://github.com/xavidop/senro/commit/d0a146335bffd96209290a2bfb7c4b0c8b2ca663))
* **duration:** allocate the cpu map on a cold start ([070db77](https://github.com/xavidop/senro/commit/070db77a54dc6e285f5869aa9646f8529a91fc86))
* **engine:** a cancel after a retryable held failure settles cancelled ([6b65fae](https://github.com/xavidop/senro/commit/6b65fae8ef8e83ec65e72ddf60645889de095a4a))
* **engine:** a shell on a held step skips its own mount lock; no retry after a cancelled hold ([73a9d73](https://github.com/xavidop/senro/commit/73a9d73bec8a3e0df5e730dfdf44c80d1ee0235e))
* **engine:** CheckGenerated enters the cache key and refuses what it cannot check ([e62654f](https://github.com/xavidop/senro/commit/e62654ff32f4ea226957478fe9e63cd157d4eef3))
* **engine:** copy a service's workspace directory rather than restore its snapshot ([c537d2f](https://github.com/xavidop/senro/commit/c537d2f13b98b6d5031afde0745a2aae54ef2008))
* **engine:** end a self-exited service's group, wait every retry on its services ([fd4b90d](https://github.com/xavidop/senro/commit/fd4b90d1a8b69e845a8c22f5b01340a6b220ec77))
* **engine:** give a service a private copy of its workspaces ([184951c](https://github.com/xavidop/senro/commit/184951c74fa7dccce53d1201b207eb40005f507c))
* **engine:** mount a service's workspaces read-only and drop the hold once it starts ([fb73e17](https://github.com/xavidop/senro/commit/fb73e176c70e2d6c1216b8bfe2a8e991d558e260))
* **engine:** read a dropped probe connection as not ready and address services by both executors ([ff63c72](https://github.com/xavidop/senro/commit/ff63c728515f2af5792de5cbd86376ae71a7ddeb))
* **engine:** read a JUnit report best-effort on a non-zero exit ([0eb49fb](https://github.com/xavidop/senro/commit/0eb49fb0c5a31199951d04fd0c93248aea9894b6))
* **engine:** read a JUnit report from the sandbox's real working directory ([bfefa88](https://github.com/xavidop/senro/commit/bfefa88976adb69a7cb425dfc24f8b01f741d88a))
* **engine:** recheck a re-held step's token before a shell takes the lock ([f89d622](https://github.com/xavidop/senro/commit/f89d6227d0aa08a0daf5513552ec304ca82040a0))
* **engine:** refuse a manual retry of a CheckGenerated step ([2d8e4d1](https://github.com/xavidop/senro/commit/2d8e4d143a9baba20cdee3ea7961ca718810fa91))
* **engine:** refuse rerun_from through a CheckGenerated step ([e50c366](https://github.com/xavidop/senro/commit/e50c3662f18574cb44e46be8992968e50f59a1a1))
* **engine:** restart a stopped service when its consumer is retried or rerun ([b5626f4](https://github.com/xavidop/senro/commit/b5626f4059d7291ce1e8b7ddff105730f59057da))
* **engine:** settle a retried service recovered, analyze service failures, clamp its stop ([2d28291](https://github.com/xavidop/senro/commit/2d2829199e68215b52cec6da0f3d1ea8065369d7))
* **engine:** settle a retried service's generation, register a dispatch's stop at once ([ab408eb](https://github.com/xavidop/senro/commit/ab408eb7a7a014aed8c13e857d9bdf84c17b3e73))
* **engine:** skip an unreadable file in a service's workspace copy ([24bb1b1](https://github.com/xavidop/senro/commit/24bb1b12e3144c4499706df97f39a332018d5487))
* **engine:** warn and do not hold under --break-on-failure with nothing attached ([bd881b1](https://github.com/xavidop/senro/commit/bd881b19ace741a0be0534c147e61dcdcca0c07c))
* **executor:** refuse a wildcard-held service port, kill on a refused or late stop ([0b5363e](https://github.com/xavidop/senro/commit/0b5363e4ba2445d69555ae91415bce1eef3d267c))
* **kit:** Walk skips a directory it cannot read instead of failing ([2962861](https://github.com/xavidop/senro/commit/29628610ade0d95994cec583824563f19be068fe))
* **lint:** stop discarding error receives errcheck now flags ([0ffd0f9](https://github.com/xavidop/senro/commit/0ffd0f99a5dba53352242d342a3693c7c544bfde))
* **localexec:** never signal a swept service group from a late Stop ([5ea032b](https://github.com/xavidop/senro/commit/5ea032bbab5b851955265712ce8c091037c24966))
* **localexec:** refuse a service whose port is taken and kill what a self-exited wrapper left ([85dd445](https://github.com/xavidop/senro/commit/85dd44572550d74916c2252a1737470f8aab67d5))
* **lock:** senro lock --check exits 1 on drift, lock modes precede the trigger ([601e91e](https://github.com/xavidop/senro/commit/601e91ea85e272e2076be56cea03d6e8ee35956b))
* **notify:** annotate GitHub Checks from the folded tests ([5785b00](https://github.com/xavidop/senro/commit/5785b00b156460735eba75063aa3486b38da6f5d))
* **notify:** cut a test annotation's message at a rune boundary ([a655377](https://github.com/xavidop/senro/commit/a655377d1ff52cb6cbef04bfc883c10ba4c57d3e))
* **notify:** path-escape the run id in DetailsURL ([609d5bb](https://github.com/xavidop/senro/commit/609d5bbe582a46a7ae6fcdf463675119e8c6cbf0))
* **plan:** refuse out-of-range or duplicate service ports, negative durations and a TCP probe path ([dfdc294](https://github.com/xavidop/senro/commit/dfdc29457a77970f7f09e56a0cdef4ea12651cd9))
* **render:** plain prints a failure hold without an exit clause ([ed00f9b](https://github.com/xavidop/senro/commit/ed00f9b0dc9ed594f5134ecc82d584d2bbe4540a))
* **rerun:** refuse a bad test selection before any run state exists ([912af7d](https://github.com/xavidop/senro/commit/912af7d58d8800e87024c65987a72f0026de8d80))
* **rerun:** refuse a test selection on a step WithOnlySteps leaves out ([64ae87a](https://github.com/xavidop/senro/commit/64ae87ab0f529fb08cde8d4c19d3160394588d08))
* **rerun:** validate --test names and shape them for go test -run ([7bb2455](https://github.com/xavidop/senro/commit/7bb24559736ea4ee69afab60a705b90bc56d174e))
* **service:** accept container services and retry a service as a service ([35ec852](https://github.com/xavidop/senro/commit/35ec85264f10519c41ae90ebac15082f510dae62))
* **shell:** deny BASH_FUNC_* offline and name the mounts not restored ([656f44b](https://github.com/xavidop/senro/commit/656f44b64c7a7be7d23240f3f94a8e746a48655c))
* **testparse:** reject JUnit input with no testsuite root ([478c53c](https://github.com/xavidop/senro/commit/478c53c85d0417f4a69f72384fae8d9c90d628f6))
* **testparse:** TAP subtest boundary, rune-safe Tail, junit stray cases ([8501017](https://github.com/xavidop/senro/commit/850101708ddf3eb6188579261fdb10c293de2f95))
* **tui:** clip the tests pane to the terminal, failures kept on top ([ff63c31](https://github.com/xavidop/senro/commit/ff63c311f3ee06e28d296d16a31c40f7209e93f8))
* **tui:** keep unknown outcomes in the tests pane; cut long output lines ([3d541fb](https://github.com/xavidop/senro/commit/3d541fbdfe50c1056096ea3c68cf12171ef4034e))
* **tui:** say ran at, not ready at, for a finished service ([38e5013](https://github.com/xavidop/senro/commit/38e501379a5115ffffcb6ebd846fd50d3e37fb6d))
* **ui:** uniform CPU seconds, no false rss, byte unit boundaries ([87600e4](https://github.com/xavidop/senro/commit/87600e4d7a585a8efdc71bf7f237cd099a85b334))
* **webui:** separate the tests list signature's fields with NUL ([b73a0a5](https://github.com/xavidop/senro/commit/b73a0a5426d25b227b4fda218f974081d7a2a7ac))


### Features

* **api:** add service.ready and the failure hold to the fold ([fda0e69](https://github.com/xavidop/senro/commit/fda0e69220678af977c6881f8c018f6f2dc825aa))
* **api:** add test.finished event and per-test fold ([2a0c3bd](https://github.com/xavidop/senro/commit/2a0c3bd502035ee96988200bd919830770c396f4))
* **cli:** senro init writes a pipeline from the detected kits ([5cd4813](https://github.com/xavidop/senro/commit/5cd48136e954c03d5f02effac783df24323101d9))
* **cli:** senro lock pins images to digests and runs apply the lock ([db3e15d](https://github.com/xavidop/senro/commit/db3e15d963707183093a75f9fcdab7565da040f7))
* **cli:** senro shell opens a recorded step's workspaces offline ([d40a6f1](https://github.com/xavidop/senro/commit/d40a6f120948335c55f0a0fa80b995457a8e0662))
* **cli:** senro up runs a pipeline's services and holds them ([676221e](https://github.com/xavidop/senro/commit/676221e509b5c0e16f7501aa25a8c3dbd758ae37))
* **containerexec:** report a service container's bridge address and run it under an init ([b974f68](https://github.com/xavidop/senro/commit/b974f68b04858a49b49035150e2f6aa9a7ccc681))
* **dispatcher:** GitHub App mode with queued check runs ([6b95242](https://github.com/xavidop/senro/commit/6b952428890df03e682cce001b8e0d5fad6f3ffa))
* **engine:** CheckGenerated fails a step whose workspace drifted ([1ad0bff](https://github.com/xavidop/senro/commit/1ad0bff0672217223a4dc2d64c0ca3ce15e68496))
* **engine:** emit test.finished from a step's declared test report ([ac1a3c5](https://github.com/xavidop/senro/commit/ac1a3c534d20e39f682181262ac2a68a630699b1))
* **engine:** give a shell on a consumer the addresses of its ready services ([9b1686b](https://github.com/xavidop/senro/commit/9b1686bb67a525f927bae074bb9ed1607d7d8d65))
* **engine:** hold a failed attempt for inspection with --break-on-failure ([7c2b2f5](https://github.com/xavidop/senro/commit/7c2b2f5ee200aaf91060a1662ba34bde31602af9))
* **engine:** run a service step: start, probe, publish, stop ([25da7e7](https://github.com/xavidop/senro/commit/25da7e76f05f37bb581cee3c42bbce20d0ed9286))
* **engine:** step.finished carries CPU and memory usage ([22af265](https://github.com/xavidop/senro/commit/22af265babc8da5b93ec701d599d45345b933a63))
* **executor:** start and stop a service with grace, publish its address ([7179bd1](https://github.com/xavidop/senro/commit/7179bd17e39e0c4768f1ed8d0c5b8932e72f1ca7))
* **kit/gotest:** preset go test step with per-test reporting ([ce65db1](https://github.com/xavidop/senro/commit/ce65db14c12f12b50892925757e2657d6d712eab))
* **kit/pytest:** preset pytest step with JUnit reporting and -k re-runs ([64af99b](https://github.com/xavidop/senro/commit/64af99b0d0a5c8ca751ba5af758842c0ecd02828))
* **kit:** eslint and playwright presets ([118df77](https://github.com/xavidop/senro/commit/118df77dbdb41b3cb27499999d30cb6e8881a92e))
* **kit:** shared match, report path, tree walk and tool detection ([c31442d](https://github.com/xavidop/senro/commit/c31442ddda036c1abb8a70764810ee6c308d4da1))
* **lock:** lock file and registry tag resolution ([8ce6355](https://github.com/xavidop/senro/commit/8ce6355eae54a2a95434880f6633174ee6735dfe))
* **lock:** reruns reapply the recorded pins ([bcd2967](https://github.com/xavidop/senro/commit/bcd296719d914882ed675e4d72b20f3c562355a1))
* **notify:** adopt an existing check run, details URL, GitHubChecksFromEnv ([f3bced3](https://github.com/xavidop/senro/commit/f3bced333732d4aa8e3d3397d025938c9c9a21ad))
* **notify:** annotate failed tests in GitHub Checks ([28cf047](https://github.com/xavidop/senro/commit/28cf047bc37932974ffc839f30a53bddf69da16e))
* per-test results, services, kits and senro init, GitHub App mode, senro lock, CheckGenerated, step usage ([#47](https://github.com/xavidop/senro/issues/47)) ([2150294](https://github.com/xavidop/senro/commit/2150294d8a60c6f144b572c472a8f8697b02dfb3))
* **render:** name failing tests and summarise counts in the plain renderer ([fe34ee0](https://github.com/xavidop/senro/commit/fe34ee04370cc05161a422b878f9ae8ae98096f1))
* **rerun:** rerun only chosen or failed tests of a step ([9fbedd3](https://github.com/xavidop/senro/commit/9fbedd32644a8c4345ea97aa2c39e0e7e5ab1975))
* **senro:** WorkflowBuilder.Add adopts a preset step ([dd1a5ad](https://github.com/xavidop/senro/commit/dd1a5adfe0f2f1b954d9c1a5f95a0c4ca68ecd9d))
* **service:** declare a long-running step with ports and a probe ([cee3804](https://github.com/xavidop/senro/commit/cee3804cababb1def5681c37c155cfb580615597))
* **testparse:** parse go test json, TAP and JUnit into per-test results ([624c669](https://github.com/xavidop/senro/commit/624c6691afecb5799f333ef4ebc0bc213478e8bf))
* **testreport:** declare how a step reports tests ([6223608](https://github.com/xavidop/senro/commit/622360855076bdb49d320ca5d988057a10e72a38))
* **tui:** show a step's tests on its row and in a t-toggled pane ([8001eae](https://github.com/xavidop/senro/commit/8001eaee8168a5c30f980bf575ad7a14378c1167))
* **ui:** label a ready service and show its endpoints ([ca51766](https://github.com/xavidop/senro/commit/ca517669b1c31df481652e33ed8ab7c372e0dcef))
* **ui:** show step CPU and memory, export and record them ([519a3b3](https://github.com/xavidop/senro/commit/519a3b3aabd96bf99f35990ac45c977033bc9fbd))
* **webui:** list a step's tests in the detail pane, failures first ([4b981ff](https://github.com/xavidop/senro/commit/4b981ffde9ed991fa3f67177b881d36d1f128672))

## [1.4.5](https://github.com/xavidop/senro/compare/v1.4.4...v1.4.5) (2026-10-15)


### Bug Fixes

* ci ([39b0471](https://github.com/xavidop/senro/commit/39b0471724f4ea07e3da784f44b4c5b497dbe99c))

## [1.4.4](https://github.com/xavidop/senro/compare/v1.4.3...v1.4.4) (2026-10-15)


### Bug Fixes

* ci ([e3ff576](https://github.com/xavidop/senro/commit/e3ff576a93a54a56fb61cdc5c72a7f6406afca35))

## [1.4.3](https://github.com/xavidop/senro/compare/v1.4.2...v1.4.3) (2026-10-15)


### Bug Fixes

* ci ([1d66c87](https://github.com/xavidop/senro/commit/1d66c875b78ca0c9b79e05bb69933f9d24ef6faa))

## [1.4.2](https://github.com/xavidop/senro/compare/v1.4.1...v1.4.2) (2026-10-15)


### Bug Fixes

* ci ([a691f13](https://github.com/xavidop/senro/commit/a691f13398a795301978c9db7d4d73fe28ee5b19))

## [1.4.1](https://github.com/xavidop/senro/compare/v1.4.0...v1.4.1) (2026-10-05)


### Bug Fixes

* tests ([3242a60](https://github.com/xavidop/senro/commit/3242a6076e87ffe41652ff2b732a8e6a6cad54a8))

# [1.4.0](https://github.com/xavidop/senro/compare/v1.3.2...v1.4.0) (2026-08-28)


### Features

* Errnomatch and senro runs cli command ([8f55921](https://github.com/xavidop/senro/commit/8f55921c649164c18ca5d17b07bb4258e38673f6))

## [1.3.2](https://github.com/xavidop/senro/compare/v1.3.1...v1.3.2) (2026-08-25)


### Bug Fixes

* ci ([2f7e63c](https://github.com/xavidop/senro/commit/2f7e63c8f6548793c66fcad205994abd2f3d3e13))

## [1.3.1](https://github.com/xavidop/senro/compare/v1.3.0...v1.3.1) (2026-08-25)


### Bug Fixes

* tests ([22327e3](https://github.com/xavidop/senro/commit/22327e3ba11ccf0c6f3dbb41e446ddf6c65512f5))

# [1.3.0](https://github.com/xavidop/senro/compare/v1.2.1...v1.3.0) (2026-08-24)


### Features

* scratch cache in S3 and between exectors, improved docs ([2b3a11f](https://github.com/xavidop/senro/commit/2b3a11ff45ce96888379ef1c5010d232714b5e34))

## [1.2.1](https://github.com/xavidop/senro/compare/v1.2.0...v1.2.1) (2026-08-24)


### Bug Fixes

* bug fixes and improved logs ([2d85898](https://github.com/xavidop/senro/commit/2d85898c865d02f7c8f2b013d791cdd398d0e493))

# [1.2.0](https://github.com/xavidop/senro/compare/v1.1.0...v1.2.0) (2026-08-21)


### Features

* go func as a handler and trigger servers ([18f00bb](https://github.com/xavidop/senro/commit/18f00bb7f5720e56dbcdbf6329b7d001fe37e5b5))

# [1.1.0](https://github.com/xavidop/senro/compare/v1.0.6...v1.1.0) (2026-08-20)


### Features

* better docs and subgraphs ([c981231](https://github.com/xavidop/senro/commit/c98123123c7e6c437fd87432fe890cb4b74a6376))

## [1.0.6](https://github.com/xavidop/senro/compare/v1.0.5...v1.0.6) (2026-08-19)


### Bug Fixes

* test ([70d8c06](https://github.com/xavidop/senro/commit/70d8c06d400b40dba8ac325b5a58aa99d9fdedb3))

## [1.0.5](https://github.com/xavidop/senro/compare/v1.0.4...v1.0.5) (2026-08-19)


### Bug Fixes

* tests ([9ee14ea](https://github.com/xavidop/senro/commit/9ee14ea8c562c14370643ef56c415372a301bff0))

## [1.0.4](https://github.com/xavidop/senro/compare/v1.0.3...v1.0.4) (2026-08-19)


### Bug Fixes

* tests ([dbf4db9](https://github.com/xavidop/senro/commit/dbf4db93835fbcd873da7dcf8d16f87ab8a62da2))

## [1.0.3](https://github.com/xavidop/senro/compare/v1.0.2...v1.0.3) (2026-08-18)


### Bug Fixes

* docs and test ([db9b0ce](https://github.com/xavidop/senro/commit/db9b0cecdd38ad0d8d161303ee475c6026856e9f))

## [1.0.2](https://github.com/xavidop/senro/compare/v1.0.1...v1.0.2) (2026-08-18)


### Bug Fixes

* test ([efa9b4a](https://github.com/xavidop/senro/commit/efa9b4ac955a0c08dfbb242b0fb020471fc49085))

## [1.0.1](https://github.com/xavidop/senro/compare/v1.0.0...v1.0.1) (2026-08-18)


### Bug Fixes

* **deps:** update every dependency dependabot opened a PR for ([b82b00e](https://github.com/xavidop/senro/commit/b82b00e5b6e868d28ab606d17f1f44e1bbf2f61e)), closes [#1](https://github.com/xavidop/senro/issues/1) [#5](https://github.com/xavidop/senro/issues/5) [#4](https://github.com/xavidop/senro/issues/4) [#5](https://github.com/xavidop/senro/issues/5) [#4](https://github.com/xavidop/senro/issues/4) [#5](https://github.com/xavidop/senro/issues/5)
* **site:** make the docs work at the URL they are actually served from ([ac1c719](https://github.com/xavidop/senro/commit/ac1c7195f920879a1851822bc53be0454ad9b859))

# 1.0.0 (2026-08-18)


### Features

* senro, a pipeline engine defined in Go ([3711f07](https://github.com/xavidop/senro/commit/3711f07c87a0fa2c991f74764bf2035900bcccdb))
