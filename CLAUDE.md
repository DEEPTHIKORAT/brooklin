# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Brooklin is a distributed system for streaming data between heterogeneous source and destination
systems (e.g. mirroring Kafka clusters, Change Data Capture with bootstrap). It is built around
pluggable extension points (connectors, transport providers, dedupers, assignment strategies)
coordinated by a ZooKeeper-backed leader/follower control plane.

## Build and test commands

This is a multi-module Gradle project (`settings.gradle` lists all modules) built with Java 17
(`sourceCompatibility`/`targetCompatibility` = 17). Tests use TestNG, not JUnit.

- Build everything: `./gradlew build`
- Run all tests: `./gradlew test`
- Run tests for a single module: `./gradlew :datastream-server:test`
- Run a single test class: `./gradlew :datastream-server:test --tests "com.linkedin.datastream.server.TestCoordinator"`
- Run a single test method: `./gradlew :datastream-server:test --tests "com.linkedin.datastream.server.TestCoordinator.testMethodName"`
- Checkstyle (style rules in `checkstyle/checkstyle.xml`): `./gradlew checkstyleMain checkstyleTest`
- SpotBugs (exclude filter in `findbugs/excludeFilter.xml`): `./gradlew spotbugsMain`
- `check` also runs license header verification (`licenseMain`/`licenseTest`, header text in `HEADER`)
- Build the release tarball: `./gradlew :datastream-tools:releaseTarGz`
- Start/stop a locally built server: `scripts/brooklin-server-start.sh` / `scripts/brooklin-server-stop.sh` (config in `config/server.properties`)
- Start a standalone REST server for the DMS API only: `./gradlew :datastream-server-restli:startStandaloneRestServer`

Notes on the test setup: `maxParallelForks = 1` (tests are not run in parallel within a module),
and JDK 17 `--add-opens` flags are set in `build.gradle` for reflection-heavy dependencies (Kafka,
ZooKeeper, Avro, Mockito/ByteBuddy) — needed if running tests outside Gradle.

Compiler warnings are treated as errors (`-Werror` with a specific `-Xlint` set in `build.gradle`),
so new code must compile cleanly under lint.

## Architecture

### Module layout and dependency direction

- `datastream-common` — core data model (`Datastream`, `DatastreamSource`, `DatastreamDestination`,
  etc.), generated via Pegasus data templates. Nearly every other module depends on it.
- `datastream-server-api` — the extension-point interfaces that pluggable implementations must
  satisfy: `Connector`/`ConnectorFactory`, `TransportProvider`/`TransportProviderAdmin`,
  `DatastreamDeduper`, `AssignmentStrategy`, `Authorizer`, `SerdeAdmin`. This is the module to read
  first to understand what a new source/destination/connector integration must implement.
- `datastream-server` — the core runtime: `Coordinator`, ZooKeeper adapter (`server/zk`),
  task assignment strategies (`server/assignment`), and checkpoint/state providers
  (`server/providers`).
- `datastream-server-restli` — the Datastream Management Service (DMS), a Rest.li REST API for
  CRUD and management operations (`pause`/`resume`, etc.) on datastreams, plus a diagnostics
  endpoint.
- `datastream-client` — Rest.li client for talking to the DMS.
- `datastream-kafka` / `datastream-kafka-connector` / `datastream-kafka-factory-impl` — the
  Kafka-specific connector and transport provider implementation (mirroring, CDC via Kafka).
- `datastream-file-connector`, `datastream-directory` — additional connector implementations.
- `datastream-utils` — shared utilities (including ZooKeeper client helpers) used across modules.
- `datastream-testcommon` — shared test harness code depended on by most modules' test sources.
- `datastream-tools` — CLI tooling and the release tarball assembly (`releaseTarGz` task).

### Coordinator and leader election

`Coordinator` (in `datastream-server`) bridges ZooKeeper with `Connector` implementations. One
`Coordinator` instance runs per deployable Brooklin service instance; a single Coordinator can host
multiple connectors, but only one connector per connector type (enforced via
`Connector.getConnectorType()`). All ZooKeeper interaction is wrapped by `ZkAdapter`
(`server/zk`), which drives leader/follower callbacks on the Coordinator:

- `onBecomeLeader()` — fired when this instance is elected cluster leader.
- `onDatastreamAddOrDrop()` — only the leader watches datastream definitions in ZooKeeper; when
  the DMS creates/modifies/removes datastreams, the leader reassigns datastream tasks across live
  instances using the configured `AssignmentStrategy`.

Because only the leader performs assignment and other leader-only responsibilities, code adding
new leader-triggered behavior should generally be guarded by an `isLeader` check, mirroring
existing callback handlers.

### Extension points

New source/destination integrations plug in via `datastream-server-api` interfaces rather than
by modifying the core runtime:

- `ConnectorFactory` / `Connector` — produces/manages tasks for a specific source system.
- `TransportProviderAdminFactory` / `TransportProviderAdmin` / `TransportProvider` — handles
  sending events to a destination system.
- `DatastreamDeduperFactory` / `DatastreamDeduper` — decides whether a new `Datastream` can reuse
  (dedup onto) an existing one's destination/tasks, e.g. `SourceBasedDeduper` in `datastream-server`.
- `AssignmentStrategyFactory` / `AssignmentStrategy` — decides how datastream tasks are distributed
  across live instances (see `server/assignment` for load-based and other strategies).
- `Authorizer`, `SerdeAdmin` — pluggable authorization and serialization.

## Contribution conventions

- Java coding style is enforced by the Checkstyle config at `checkstyle/checkstyle.xml`.
- Commit messages: separate subject from body with a blank line; subject line capitalized, no
  trailing period, limited to 120 characters; body wrapped at ~100 characters; no internal ticket
  references.
- Use Apache Commons for argument validation.
- PRs are made by forking the repo and targeting `master`.
