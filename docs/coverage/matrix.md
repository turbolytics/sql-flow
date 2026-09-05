# Feature coverage matrix

Generated from `docs/coverage/matrix.json` by `make coverage-matrix`.
Do not edit by hand.

Features are declared in `docs/coverage/features.yml`. A test attaches
to one by name -- `sink.clickhouse` is covered by `TestSinkClickhouse*`
or `test_sink_clickhouse*` -- and that is the cheap default: rename a
test and it is attributed, with no import and no marker.

Levels are derived from where a test ran, never declared, so they
cannot drift. A **skipped** test is not coverage: a skip that reads as
a pass is how `sink.iceberg` shipped for months without ever being
written to.

| Feature | What it does | unit | release | Tests |
| --- | --- | --- | --- | --- |
| `source.kafka` | Consumes a Kafka topic, tracking offsets and leader epochs. | ✅ | ✅ | `TestSourceKafka_CommitMarksCommitsOnlyTheProcessedPosition`, `TestSourceKafka_MessagesCarryHighWatermark`, `TestSourceKafka_SecurityOptionsPlaintextNeedsNoOptions` +19 more |
| `source.webhook` | Accepts records over HTTP, with optional HMAC signature checks. | ✅ | — | `TestSourceWebhook_BackpressureHoldsSecondRequest`, `TestSourceWebhook_CloseIsIdempotent`, `TestSourceWebhook_CloseReleasesBlockedRequest` +12 more |
| `source.websocket` | Consumes a websocket stream, reconnecting on drop. | ✅ | ✅ | `TestSourceWebsocket_CloseEndsStream`, `TestSourceWebsocket_ReadsLargeMessages`, `TestSourceWebsocket_ReconnectsAfterDrop` +3 more |
| `sink.kafka` | Publishes result rows to a Kafka topic. | ❌ **missing** | ✅ | `test_handler_inferred_mem_aggregates_every_message` |
| `sink.clickhouse` | Inserts result batches into a ClickHouse table. | ✅ | ✅ | `TestSinkClickhouse_BatchIsNil`, `TestSinkClickhouse_EmptyTableIsNoop`, `TestSinkClickhouse_InsertsArrays` +11 more |
| `sink.iceberg` | Appends result batches to an Iceberg table through a catalog. | ❌ **missing** | ✅ | `test_sink_iceberg_writes_every_row` |
| `sink.parquet` | Writes result batches as parquet files to a local path. | — | ✅ | `test_sink_parquet_writes_every_row` |
| `sink.sqlcommand` | Runs a SQL command against the pipeline's own DuckDB connection. | ❌ **missing** | — | — |
| `sink.console` | Writes result rows to stdout as JSON. | ✅ | ✅ | `TestSinkConsole_RowsAsJSONEmptyTable`, `TestSinkConsole_RowsAsJSONOneObjectPerRow`, `test_handler_inferred_mem_invoke_renders_rows` |
| `sink.retry` | Retries a sink whose destination is not answering, bounded by a deadline. | ✅ | — | `TestSinkRetry_BackoffGrowsAndIsCapped`, `TestSinkRetry_BoundsCancellationMidLadderStops`, `TestSinkRetry_BoundsErrorSaysAttemptsWereExhausted` +66 more |
| `handler.inferred_mem` | Infers a schema per batch and runs the query in memory. | ✅ | ✅ | `TestHandlerInferredMem_BatchAfterEmptyBatch`, `TestHandlerInferredMem_ColumnsComeFromFirstRow`, `TestHandlerInferredMem_ConflictingListElementTypesError` +39 more |
| `handler.inferred_disk` | Infers a schema per batch, staging the batch on disk. | ✅ | — | `TestHandlerInferredDisk_BatchTableDroppedAfterInvoke`, `TestHandlerInferredDisk_CloseRemovesFiles`, `TestHandlerInferredDisk_CreatesCacheDir` +6 more |
| `handler.structured` | Binds a declared schema, ingesting through Arrow. | ✅ | ✅ | `TestHandlerStructured_FilterSeesRowsIngestedAfterPrepare`, `TestHandlerStructured_LargeBatch`, `TestHandlerStructured_ListElementSeesRowsIngestedAfterPrepare` +4 more |
| `state.durability` | Window state and the offsets that produced it commit together. | ✅ | ✅ | `TestStateDurability_DBSecondConnectionSeesOnlyCommittedState`, `TestStateDurability_OpenPathEmptyPathIsInMemory`, `TestStateDurability_OpenPathPersistsAcrossProcesses` +8 more |
| `state.offsets` | Kafka positions are stored in DuckDB and resumed on restart. | ✅ | ✅ | `TestStateOffsets_InitAcceptsAFreshDatabase`, `TestStateOffsets_InitAcceptsAPriorRunAndKeepsItsRows`, `TestStateOffsets_InitIsIdempotent` +19 more |
| `state.corruption` | A damaged state file fails the start rather than silently resetting. | ✅ | ✅ | `TestStateCorruption_CreatesAMissingFile`, `TestStateCorruption_LeavesADamagedFileOnDisk`, `TestStateCorruption_RejectsAFileThatIsNotADatabase` +3 more |
| `lifecycle.drain` | SIGTERM writes the buffered batch before exiting. | ✅ | ✅ | `TestLifecycleDrain_CancelDrainsTheBufferedBatch`, `test_lifecycle_drain_writes_the_buffered_batch_on_sigterm` |
| `lifecycle.exit_codes` | The process exit status carries the error code a supervisor reads. | ✅ | ✅ | `TestLifecycleExitCodes_CorruptStateFileExitsTerminal`, `TestLifecycleExitCodes_MalformedConfigIsTerminal`, `TestLifecycleExitCodes_MissingConfigIsTerminal` +5 more |
| `core.consume_loop` | Accumulates a batch, flushes it, and commits in that order. | ✅ | — | `TestCoreConsumeLoop_CommitsOnlyProcessedMarks`, `TestCoreConsumeLoop_FlushesFinalBatchWhenMaxMsgsReached`, `TestCoreConsumeLoop_FlushesPartialBatchOnFlushInterval` +11 more |
| `error.taxonomy` | Every failure carries a class.domain.reason code. | ✅ | — | `TestErrorTaxonomy_CodeSplitsIntoThreeParts`, `TestErrorTaxonomy_CodeStaysOnTheFirstLineOfAMultiLineCause`, `TestErrorTaxonomy_CodeSurvivesWrapping` +13 more |
| `error.raise` | Policy RAISE stops the pipeline on a bad record. | ✅ | — | `TestErrorRaise_ConsumeLoopStopsOnWriteError` |
| `error.ignore` | Policy IGNORE drops a bad record and keeps the pipeline running. | ✅ | ✅ | `TestErrorIgnore_BatchOfOnlyBadMessagesIsNotAHandlerError`, `TestErrorIgnore_ConsumeLoopContinuesAfterInvokeError`, `TestErrorIgnore_ConsumeLoopSkipsBadMessage` +2 more |
| `error.dlq` | Policy DLQ diverts a bad record to a sink instead of dropping it. | ✅ | ✅ | `TestErrorDlq_ConsumeLoopRoutesInvokeError`, `TestErrorDlq_ConsumeLoopRoutesWriteError`, `test_error_dlq_diverts_a_batch_the_handler_cannot_query` +1 more |
| `manager.tumbling_window` | Publishes and deletes closed windows on an interval. | ✅ | ✅ | `TestManagerTumblingWindow__ClockAdvancesOnlyAcrossACommit`, `TestManagerTumblingWindow__DeleteFailureIsReported`, `TestManagerTumblingWindow__DeleteJoinsThePipelineTransaction` +13 more |
| `config.templating` | Renders a config through Jinja2 against SQLFLOW_ environment variables. | ✅ | ✅ | `TestConfigTemplating_Load_AllExampleConfigs`, `TestConfigTemplating_Load_AllExampleConfigs/attach-geoip.yml`, `TestConfigTemplating_Load_AllExampleConfigs/basic.agg.mem.yml` +42 more |
| `config.validation` | Validates a config against the schema and reports where it is wrong. | ✅ | ✅ | `TestConfigValidation_AcceptsStatePath`, `TestConfigValidation_ExampleConfigsBuildRealComponents`, `TestConfigValidation_ExampleConfigsBuildRealComponents/attach-geoip.yml` +69 more |
| `observability.metrics` | Exports pipeline counters and histograms over Prometheus. | ✅ | — | `TestObservabilityMetrics_StateGaugesNoProviderRecordsNothing`, `TestObservabilityMetrics_StateGaugesReportsSizeAndRows`, `TestObservabilityMetrics_StateGaugesSurvivesCollectionFailure` +4 more |
| `observability.debug_api` | Serves ad-hoc SQL against the live DuckDB connection. | ✅ | — | `TestObservabilityDebugApi_HandlerRejectsMissingQuery`, `TestObservabilityDebugApi_HandlerReportsQueryErrors`, `TestObservabilityDebugApi_HandlerReturnsEmptyArrayForNoRows` +4 more |
| `cli.invocation` | Resolves the config path and message limits from either flag form. | ✅ | — | `TestCliInvocation_NewCommandConfigFlagIsNotRequired`, `TestCliInvocation_NewCommandHasPythonMaxMsgsFlag`, `TestCliInvocation_NewCommandRejectsTwoPositionalArgs` +10 more |
| `cli.dev_invoke` | Runs a pipeline against a fixture file, without a source. | ✅ | ✅ | `TestCliDevInvoke_BlueskyFirehose`, `TestCliDevInvoke_EmptyFixture`, `TestCliDevInvoke_FixtureOfOnlyBlankLines` +6 more |

**30 features declared, 27 fully covered, 3 gap(s).**

## Gaps

These fail `make coverage-matrix`. There is no baseline: a gap
is closed by a test, or by the registry honestly no longer
requiring that level.

- `sink.kafka` requires **unit** coverage and is *missing*.
- `sink.iceberg` requires **unit** coverage and is *missing*.
- `sink.sqlcommand` requires **unit** coverage and is *missing*.

## Covered only by another test's marker

These features have no test named for them. That is legitimate for a
capability an end-to-end run proves in passing, and a smell for one
that deserves its own test.

- `source.kafka` (release) — via `test_handler_inferred_mem_aggregates_every_message`, `test_state_durability_survives_a_restart`
- `source.websocket` (release) — via `test_handler_inferred_mem_preserves_arrays_and_unioned_fields`
- `sink.kafka` (release) — via `test_handler_inferred_mem_aggregates_every_message`
- `sink.console` (release) — via `test_handler_inferred_mem_invoke_renders_rows`
- `handler.structured` (release) — via `test_handler_inferred_mem_preserves_arrays_and_unioned_fields`
- `state.offsets` (release) — via `test_state_durability_survives_a_restart`
- `state.corruption` (release) — via `test_lifecycle_exit_codes_carry_the_error_code`
- `manager.tumbling_window` (release) — via `test_state_durability_survives_a_restart`
- `config.templating` (release) — via `test_config_validation_accepts_a_shipped_example`
- `cli.dev_invoke` (release) — via `test_handler_inferred_mem_invoke_renders_rows`

## Unattributed release tests (1)

These match no declared feature. Either rename them to the
convention, or add the feature to `features.yml`.

- `test_sqlflow_docker_version`

