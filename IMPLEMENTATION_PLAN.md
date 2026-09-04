# HierEB Direction 1 Implementation Plan

## 1. Module map

| Vertical slice | Modules | Contract / invariant | Verification |
|---|---|---|---|
| Config and domain | `config.py`, `domain/models.py` | Typed, immutable inputs; fail-fast validation; no degenerate true hierarchy | Config unit tests and CLI validation |
| Loader and splits | `data/loader.py`, `data/splits.py` | CSV/Parquet, filter counts, stable `(timestamp, household_id, plug_id, original_row_index)` ordering, disjoint splits | Loader/split unit tests |
| Predictor | `predictor/slot_median.py` | Warm-up-only plug-slot median, plug-global fallback, no evaluation mutation | Predictor/no-leakage tests |
| Residual state | `residual/state.py` | Rolling exact or uniform-censor second moment; proxy never observes hidden residual | Residual unit tests |
| Baseline allocators | `allocator/base.py`, `baselines.py` | Common immutable request/result; non-negative thresholds; total budget bound; legacy equals flat | Unit and Hypothesis properties |
| True hierarchy | `allocator/hierarchical.py` | Household risk/fairness and within-household fairness; must differ from flat on multi-household fixture | Unit/property tests |
| Cap redistribution | `allocator/redistribution.py` | Deterministic bounded water filling; finite rounds; explicit unused budget | Unit/property tests |
| Replay | `simulation/replay.py`, `active_set.py` | Atomic timestamp batches; strict suppression boundary; reactivation; allocation at `B` effective only for `t>B` | Integration and determinism tests |
| Evaluation/artifacts | `evaluation/*` | Core/tail/safety metrics and all PRD artifacts in deterministic order | Schema and integration tests |
| CLI/synthetic | `cli.py`, `data/synthetic.py` | validate/generate/run/compare/equivalence/outlier workflows | CLI smoke and end-to-end tests |
| Bounded-memory path | `data/streaming.py`, `simulation/streaming_replay.py` | External stable sort, timestamp carry across chunks, no full evaluation event retention | Six-mode eager equivalence, chunk-boundary, artifact, and CLI tests |
| Direction 3 predictor | `predictor/adaptive.py`, `predictor/drift.py`, `simulation/adaptive_replay.py` | No hidden update, synchronized dual state, drift/resync transmissions counted, atomic commit | EWMA/drift/resync unit tests, synthetic qualitative and CLI tests |
| Direction 3 validation | `experiment.py`, `evaluation/artifacts.py` | Tune on validation only; staged alpha/CUSUM/resync search; reject unsafe/high-TR/unresolved candidates | Validation-isolation and selected-config tests |

## 2. Cross-cutting invariants

- Algorithms use event time only; seed defaults to 42.
- Predictor and cap/floor statistics are fit only from warm-up data.
- Events sharing a timestamp are decided against one immutable threshold version.
- A threshold version created after batch `B` has `effective_after=B` and is first usable at a later timestamp.
- Suppression occurs exactly when a prediction exists, the plug is not reactivating, and `abs(residual) <= threshold_used`.
- Allocated thresholds sum to at most the house budget; capped allocation also reports `used + unused = budget`.
- Ground-truth event-aligned error is used for evaluation even with the uniform-censor residual estimator.
- Public APIs are typed and allocator formulas reference `docs/MATH_SPEC.md`.

## 3. Build order and gates

1. Scaffold source layout, package metadata, README, and tests.
2. Implement config/domain, run its focused tests.
3. Implement loader/splits and predictor, run focused tests.
4. Implement residual estimator and all allocation paths, run unit/property tests.
5. Implement replay, then metrics/artifacts, run integration tests.
6. Implement CLI and synthetic fixture, run CLI tests.
7. Run Ruff, Mypy, and the complete Pytest suite.
8. Run all required CLI commands and a six-mode synthetic smoke experiment.
9. Record implementation, formulas, verification evidence, and limitations in `IMPLEMENTATION_REPORT.md`.
10. Add keyed Parquet staging, aggregate warm-up fitting, compact replay, and
    verify exact six-mode equivalence against the eager reference path.
11. Add Direction 3 config/runtime/metrics/artifacts and isolate predictor
    comparison by holding one allocator fixed.
12. Add validation-only staged search, matched-TR Pareto analysis, and predictor
    root-cause diagnostics before final evaluation.

## 4. Acceptance traceability

The final full test suite and smoke commands cover all 18 items in
`docs/ACCEPTANCE_CRITERIA.md`; the implementation report records the exact
commands and outcomes. Bounded-memory offline file streaming is included; no
network service, database, or container code is introduced.
