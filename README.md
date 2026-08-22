# hive-qdrant

<!-- hive-badges -->

[![Clojars Project](https://img.shields.io/clojars/v/io.github.hive-agi/hive-qdrant.svg)](https://clojars.org/io.github.hive-agi/hive-qdrant)
[![cljdoc](https://cljdoc.org/badge/io.github.hive-agi/hive-qdrant)](https://cljdoc.org/d/io.github.hive-agi/hive-qdrant/CURRENT)
[![release](https://github.com/hive-agi/hive-qdrant/actions/workflows/release.yml/badge.svg)](https://github.com/hive-agi/hive-qdrant/actions/workflows/release.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

<!-- /hive-badges -->

**A [Qdrant](https://qdrant.tech) memory backend for the hive ecosystem, built
to degrade instead of fail.** An unreachable cluster does not turn into an
exception at the call site: writes queue, reads answer degraded, and the queue
drains when the circuit closes again.

## Coordinates

```clojure
;; deps.edn
io.github.hive-agi/hive-qdrant {:mvn/version "0.1.9"}
```

## What it is

`QdrantMemoryStore` implements `IMemoryStore` from
[hive-spi](https://github.com/hive-agi/hive-spi) — the port leaf, **not**
`hive-mcp`. The published jar therefore carries no host dependency: the core
store, queue and migrate namespaces depend only on the ports, and the host
integration files (`addon.clj`, `lifecycle.clj`) are excluded from both the jar
and the pom.

That is the ecosystem rule this library exists to respect: the core owns the
ports, an addon owns the adapter, and a new vector backend requires zero edits
to the core.

## The fail-soft path

| Namespace | Role |
|---|---|
| `hive-qdrant.store` | `IMemoryStore` implementation over [clj-qdrant](https://github.com/hive-agi/clj-qdrant) |
| `hive-qdrant.circuit` | Circuit breaker — `:closed` (normal), `:open` (fail fast), `:half-open` (probing) |
| `hive-qdrant.queue` | JVM-wide write queue; on `:open` mutations enqueue rather than error, reads return a degraded response |
| `hive-qdrant.failure` | Closed `:error/*` taxonomy plus translation to the legacy `{:success? false :errors […]}` shape |
| `hive-qdrant.config` | Typed env wiring — host, port, api-key, TLS. Only the connection surface is operator-tunable |
| `hive-qdrant.migrate` | Chroma / Milvus → Qdrant migration, with the extraction step behind a pluggable `:source-fn` |
| `hive-qdrant.addon` | `IAddon` registration (host-side, not shipped) |
| `hive-qdrant.lifecycle` | `IShutdownHook`, priority 210 — the client band (host-side, not shipped) |

When the breaker transitions back to `:closed`, `drain!` flushes the queue
through a single-writer `core.async` pipeline, coalescing by `(op, id)` so only
the latest mutation for an entry is replayed.

## Multiple instances

The store slot and the addon id are both parameterizable, so several
`QdrantAddon` instances can coexist — carto, kanban, and so on — each backed by
its own collection, and each visible to the addon registry as distinct.

## License

MIT.
