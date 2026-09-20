# Independent Quality Review — HierEB Direction 1

## Scope

Review covered `AGENTS.md`, all specifications under `docs/`, implementation
plans/reports, all Python source and tests, synthetic end-to-end behavior, and
a real DEBS house-0 smoke run against `data/house-0.csv`.

## Quality gates

Baseline before this phase:

```text
uv sync                 PASS
ruff check .            PASS
mypy src                PASS (28 files)
pytest                   PASS (29 tests)
```

After review fixes and experiment features, the final gate is recorded at the
end of this report.

Final gate:

```text
uv sync                 PASS (40 resolved, 39 checked)
ruff check .            PASS
mypy src                PASS (31 source files)
pytest                   PASS (32 passed in 4.46s)
```

## Findings

### High — repeated full-file scans and hashes — fixed

The loader previously scanned the 3 GB CSV separately for every filter count,
and each mode hashed the input again. Filter counts now use one aggregate lazy
scan followed by one filtered materialization; SHA-256 is computed once in
`PreparedExperiment` and reused by every mode.

### High — cap quantile recomputed every allocation cycle — fixed

Warm-up cap quantiles were recomputed from all residual samples at every
allocation. Quantiles for the base/OFAT values are now precomputed once from
warm-up and cap values are cached per plug per replay. Only the rolling-window
tail of squared residuals is retained for initial residual state.

### High — full scientific windows exceed safe memory on this host — open

The real smoke contains 4,915,985 accepted rows and peaks at 6.55 GiB. Two-run
determinism verification peaks at 7.89 GiB. The source contains 52,156,471 data
rows; eager conversion to Python `Event` objects for the configured 7-day
warm-up, 3-day validation, and available evaluation rows cannot safely fit a
15 GiB host.

Do not run `debs_house0_validation.yaml` or `debs_house0_evaluation.yaml` on
this host until replay consumes a compact Parquet/Polars stream and aggregates
event diagnostics online instead of retaining all Python event/result rows.
This is an experiment-execution blocker, not evidence of an allocator formula
error, and no production scalability claim is made.

### Medium — requested evaluation coverage is unavailable in the input

The configured evaluation ends at `2013-09-17T23:59:59Z`, while the supplied
file ends at Unix timestamp `1379177999`, or `2013-09-14T16:59:59Z`. A future
scientific report must state the actual coverage or obtain the missing rows;
it must not call the available subset a full seven-day evaluation.

### Correctness review

No Critical correctness finding remains in allocator formulas, strict
suppression, batch atomicity, effective-after timing, cap bounds, or
event-aligned evaluation. Added gates fail the house runner on legacy mismatch
or any suppression/budget/bound violation.

## Real DEBS smoke evidence

- Warm-up: 3,929,146 events (2013-09-01 UTC).
- Evaluation: 986,839 events (2013-09-02 00:00–05:59:59 UTC).
- Six modes plus 36 Pareto points and exact/proxy comparison completed.
- Runtime: 1,363.69 seconds; peak RSS: 6,548.39 MiB.
- Hierarchy changed all 72 eligible cycles; mean normalized threshold L1:
  0.311686.
- Flat/legacy maximum threshold difference: `8.88e-16`; decisions equal.
- All safety violation counts: zero.
- Cap hit rate: zero; the configured cap had no smoke effect.
- Two-run determinism: 61 artifact checksums, zero mismatches after excluding
  runtime/output-path metadata.

## Review verdict

Synthetic correctness and real smoke verification pass. The full scientific
validation/evaluation gate remains blocked by the open High memory finding and
missing final evaluation dates. Results in `HOUSE0_DIRECTION1_REPORT.md` are
therefore explicitly labeled smoke evidence rather than final Direction 1
scientific conclusions.
