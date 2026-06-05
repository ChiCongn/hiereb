"""
HierEB Threshold Allocator.

Implements the two-stage budget allocation from the paper:

  Stage 1 – House → Household:
    ε_hh = ε_h × (Σ σ_p for plugs in hh) / (Σ σ_p for all plugs in house)

  Stage 2 – Household → Plug (water-filling):
    δ*_p = ε_hh × σ_p / (Σ σ_{p'} for plugs in hh)

Variance tracking:
  - Rolling 1000 effective residual samples per plug
  - Transmitted events contribute the observed signed residual squared
  - Suppressed events contribute the censored approximation delta_used² / 3

House structure format:
  {house_id: {household_id: [plug_uid, ...]}}

This module is stateful: it tracks sigma estimates and delta assignments
across reallocations. Designed to run in a single asyncio event loop.

NOT responsible for:
  - Kafka communication
  - Prediction computation
  - Drift detection
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from statistics import median

import structlog

log = structlog.get_logger(__name__)

SIGMA_FLOOR_FALLBACK = 1.0
DEFAULT_MIN_VARIANCE_SAMPLES = 30
DEFAULT_ROLLING_WINDOW_SIZE = 1000
DEFAULT_BUDGET_TOLERANCE = 1e-9


# ─── Variance tracking ────────────────────────────────────────────────────────

@dataclass
class WelfordState:
    """
    Online Welford estimator retained for diagnostics.
    Allocation uses PlugVarianceState.effective_variance_samples.
    """
    n: int = 0
    mean: float = 0.0
    M2: float = 0.0  # sum of squared deviations

    def update(self, residual: float) -> None:
        """Add one observed residual (actual - predicted)."""
        self.n += 1
        delta = residual - self.mean
        self.mean += delta / self.n
        delta2 = residual - self.mean
        self.M2 += delta * delta2

    @property
    def variance(self) -> float:
        """Sample variance. Returns 0 if fewer than 2 samples."""
        if self.n < 2:
            return 0.0
        return self.M2 / (self.n - 1)

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)


@dataclass
class PlugVarianceState:
    """
    Per-plug rolling variance state combining transmitted and censored events.

    Censored events are suppressed timesteps where only |e| <= delta_used is
    known; they enter the rolling window as delta_used² / 3.
    """
    plug_uid: int
    rolling_window_size: int = DEFAULT_ROLLING_WINDOW_SIZE
    welford: WelfordState = field(default_factory=WelfordState)

    # Effective residual variance samples. Transmitted events contribute r^2;
    # suppressed events contribute delta_used^2 / 3.
    effective_variance_samples: deque[float] = field(init=False)

    # Current sigma estimate (updated after each reallocation)
    sigma: float = 0.0

    # Current delta assignment
    delta: float = math.inf

    def __post_init__(self) -> None:
        self.effective_variance_samples = deque(maxlen=self.rolling_window_size)

    def record_transmitted(self, residual: float) -> None:
        """Called when plug transmitted: we observe the true residual."""
        self.welford.update(residual)
        self.effective_variance_samples.append(residual ** 2)

    def record_suppressed(self, delta_used: float) -> None:
        """
        Called when plug was suppressed.
        We only know |e| ≤ delta_used, so the allocator uses the uniform
        censored approximation Var[uniform(-δ, δ)] = δ² / 3.
        """
        if delta_used < 0 or not math.isfinite(delta_used):
            raise ValueError(f"delta_used must be finite and non-negative, got {delta_used}")
        self.effective_variance_samples.append((delta_used ** 2) / 3.0)

    @property
    def effective_sample_count(self) -> int:
        return len(self.effective_variance_samples)

    @property
    def variance(self) -> float:
        if not self.effective_variance_samples:
            return 0.0
        return sum(self.effective_variance_samples) / len(self.effective_variance_samples)

    def estimate_sigma(self) -> float:
        """Estimate std dev from the rolling effective residual window."""
        sigma = math.sqrt(max(self.variance, 0.0))
        self.sigma = sigma
        return sigma

    def reset_period(self) -> None:
        """No-op kept for compatibility with older callers."""
        return None


@dataclass(frozen=True)
class ThresholdTrace:
    """Internal metadata for one house allocation cycle."""
    house_id: int
    allocation_time: int | None
    effective_after_time: int | None
    threshold_version: int
    sigma_floor: float
    n_budget_active_plugs: int
    delta_h: float
    budget_sum: float
    deltas: dict[int, float]


# ─── Allocator ────────────────────────────────────────────────────────────────

# House structure type alias
HouseStructure = dict[int, dict[int, list[int]]]  # house → hh → [plug_uid]


class HierEBAllocator:
    """
    Two-stage hierarchical threshold allocator.

    Stage 1: distribute ε_h to households proportional to their total sigma
    Stage 2: distribute ε_hh to plugs proportional to fixed-floor weights
    """

    def __init__(
        self,
        epsilon_h: float,
        *,
        sigma_floor_by_house: dict[int, float] | None = None,
        min_variance_samples: int = DEFAULT_MIN_VARIANCE_SAMPLES,
        rolling_window_size: int = DEFAULT_ROLLING_WINDOW_SIZE,
        budget_tolerance: float = DEFAULT_BUDGET_TOLERANCE,
    ) -> None:
        if epsilon_h <= 0:
            raise ValueError(f"epsilon_h must be positive, got {epsilon_h}")
        if min_variance_samples <= 0:
            raise ValueError("min_variance_samples must be positive")
        if rolling_window_size <= 0:
            raise ValueError("rolling_window_size must be positive")
        self.epsilon_h = epsilon_h
        self.min_variance_samples = min_variance_samples
        self.rolling_window_size = rolling_window_size
        self.budget_tolerance = budget_tolerance
        self._sigma_floor_by_house = sigma_floor_by_house or {}
        self._plug_states: dict[int, PlugVarianceState] = {}
        self._realloc_count = 0
        self._threshold_versions: dict[int, int] = {}
        self.threshold_trace: list[ThresholdTrace] = []
        self.latest_trace_by_house: dict[int, ThresholdTrace] = {}

    # ── State management ──────────────────────────────────────────────────

    def get_or_create_plug(self, plug_uid: int) -> PlugVarianceState:
        if plug_uid not in self._plug_states:
            self._plug_states[plug_uid] = PlugVarianceState(
                plug_uid=plug_uid,
                rolling_window_size=self.rolling_window_size,
            )
        return self._plug_states[plug_uid]

    def set_sigma_floor_by_house(self, sigma_floor_by_house: dict[int, float]) -> None:
        """Set fixed warm-up sigma floors. Values are not recomputed in evaluation."""
        for house_id, floor in sigma_floor_by_house.items():
            if floor <= 0 or not math.isfinite(floor):
                raise ValueError(f"sigma floor must be positive and finite, got {floor}")
            self._sigma_floor_by_house[int(house_id)] = float(floor)

    def sigma_floor_for_house(self, house_id: int) -> float:
        """Return fixed sigma_floor_H, falling back to 1W when warm-up has no signal."""
        return self._sigma_floor_by_house.get(house_id, SIGMA_FLOOR_FALLBACK)

    def seed_warmup_residuals(self, residuals_by_plug: dict[int, list[float]]) -> None:
        """Initialise rolling variance windows from warm-up residuals."""
        for plug_uid, residuals in residuals_by_plug.items():
            state = self.get_or_create_plug(plug_uid)
            for residual in residuals[-self.rolling_window_size:]:
                state.record_transmitted(float(residual))

    def update_from_transmitted(self, plug_uid: int, residual: float) -> None:
        """Called by simulator when a plug transmits. Updates Welford state."""
        state = self.get_or_create_plug(plug_uid)
        state.record_transmitted(residual)

    def update_from_suppressed(self, plug_uid: int, delta: float) -> None:
        """Called by simulator when a plug is suppressed. Records censored obs."""
        state = self.get_or_create_plug(plug_uid)
        state.record_suppressed(delta)

    def get_delta(self, plug_uid: int) -> float:
        """Return current delta for a plug. Returns inf if unknown (full_tx fallback)."""
        if plug_uid not in self._plug_states:
            return math.inf
        return self._plug_states[plug_uid].delta

    # ── Core reallocation ─────────────────────────────────────────────────

    def reallocate(
        self,
        house_structure: HouseStructure,
        epsilon_h_override: float | None = None,
        active_plug_uids_by_house: dict[int, set[int]] | None = None,
        allocation_time: int | None = None,
        effective_after_time: int | None = None,
        threshold_version: int | None = None,
    ) -> dict[int, float]:
        """
        Run full two-stage water-filling allocation.

        Args:
            house_structure: {house_id: {hh_id: [plug_uid, ...]}}
            epsilon_h_override: override self.epsilon_h for this call (e.g., ablation)

        Returns:
            dict mapping plug_uid → new delta value

        Side effects:
            - Updates self._plug_states[*].sigma with censored-corrected estimates
            - Updates self._plug_states[*].delta with new assignments
            - Clears censored buffers for next period
        """
        eps_h = epsilon_h_override if epsilon_h_override is not None else self.epsilon_h
        if eps_h < 0:
            raise ValueError(f"epsilon_h must be non-negative, got {eps_h}")
        self._realloc_count += 1
        new_deltas: dict[int, float] = {}

        for house_id, hh_structure in house_structure.items():
            all_plug_uids = [p for hh in hh_structure.values() for p in hh]
            active_set = (
                set(all_plug_uids)
                if active_plug_uids_by_house is None
                else set(active_plug_uids_by_house.get(house_id, set()))
            )
            house_deltas = self._allocate_house(
                house_id=house_id,
                hh_structure=hh_structure,
                active_set=active_set,
                eps_h=eps_h,
                allocation_time=allocation_time,
                effective_after_time=effective_after_time,
                threshold_version=threshold_version,
            )
            new_deltas.update(house_deltas)

        # Compatibility no-op for older state shape.
        for state in self._plug_states.values():
            state.reset_period()

        log.info(
            "reallocation_done",
            realloc_n=self._realloc_count,
            epsilon_h=eps_h,
            plugs_updated=len(new_deltas),
            avg_delta=round(sum(new_deltas.values()) / len(new_deltas), 2) if new_deltas else 0,
        )

        return new_deltas

    def _allocate_house(
        self,
        *,
        house_id: int,
        hh_structure: dict[int, list[int]],
        active_set: set[int],
        eps_h: float,
        allocation_time: int | None,
        effective_after_time: int | None,
        threshold_version: int | None,
    ) -> dict[int, float]:
        all_plug_uids = [p for hh in hh_structure.values() for p in hh]
        for plug_uid in all_plug_uids:
            self.get_or_create_plug(plug_uid)

        active_uids = [p for p in all_plug_uids if p in active_set]
        floor = self.sigma_floor_for_house(house_id)
        weights_by_hh = self._weights_by_household(hh_structure, active_set, floor)
        house_weight = sum(sum(weights.values()) for weights in weights_by_hh.values())

        house_deltas = {plug_uid: 0.0 for plug_uid in all_plug_uids}
        if active_uids and house_weight <= 0:
            equal = eps_h / len(active_uids)
            for plug_uid in active_uids:
                house_deltas[plug_uid] = equal
        elif active_uids:
            for hh_id, weights in weights_by_hh.items():
                group_weight = sum(weights.values())
                if group_weight <= 0:
                    continue
                delta_g = eps_h * group_weight / house_weight
                for plug_uid, weight in weights.items():
                    house_deltas[plug_uid] = delta_g * weight / group_weight

        budget_sum = sum(house_deltas[p] for p in active_uids)
        tolerance = max(self.budget_tolerance, abs(eps_h) * 1e-12)
        if budget_sum - eps_h > tolerance:
            raise AssertionError(
                f"HierEB budget exceeded for house {house_id}: "
                f"sum(delta)={budget_sum}, Delta_H={eps_h}"
            )

        for plug_uid, delta in house_deltas.items():
            self._plug_states[plug_uid].delta = delta

        version = self._next_threshold_version(house_id, threshold_version)
        trace = ThresholdTrace(
            house_id=house_id,
            allocation_time=allocation_time,
            effective_after_time=(
                allocation_time if effective_after_time is None else effective_after_time
            ),
            threshold_version=version,
            sigma_floor=floor,
            n_budget_active_plugs=len(active_uids),
            delta_h=eps_h,
            budget_sum=budget_sum,
            deltas=dict(house_deltas),
        )
        self.threshold_trace.append(trace)
        self.latest_trace_by_house[house_id] = trace
        return house_deltas

    def _next_threshold_version(
        self,
        house_id: int,
        threshold_version: int | None,
    ) -> int:
        if threshold_version is not None:
            version = int(threshold_version)
        else:
            version = self._threshold_versions.get(house_id, -1) + 1
        self._threshold_versions[house_id] = version
        return version

    def _weights_by_household(
        self,
        hh_structure: dict[int, list[int]],
        active_set: set[int],
        sigma_floor: float,
    ) -> dict[int, dict[int, float]]:
        mature_weights: dict[int, float] = {}

        for plug_uids in hh_structure.values():
            for plug_uid in plug_uids:
                if plug_uid not in active_set:
                    continue
                state = self.get_or_create_plug(plug_uid)
                sigma = state.estimate_sigma()
                if state.effective_sample_count >= self.min_variance_samples:
                    mature_weights[plug_uid] = max(sigma, sigma_floor)

        house_mature_values = list(mature_weights.values())
        result: dict[int, dict[int, float]] = {}

        for hh_id, plug_uids in hh_structure.items():
            hh_mature_values = [
                mature_weights[p] for p in plug_uids if p in mature_weights
            ]
            hh_weights: dict[int, float] = {}
            for plug_uid in plug_uids:
                if plug_uid not in active_set:
                    continue
                if plug_uid in mature_weights:
                    weight = mature_weights[plug_uid]
                elif hh_mature_values:
                    weight = median(hh_mature_values)
                elif house_mature_values:
                    weight = median(house_mature_values)
                else:
                    weight = SIGMA_FLOOR_FALLBACK
                hh_weights[plug_uid] = float(weight)
            result[hh_id] = hh_weights

        return result

    def _water_fill(
        self,
        plug_uids: list[int],
        epsilon_hh: float,
    ) -> dict[int, float]:
        """
        Stage-2 water-filling: distribute ε_hh among plugs proportional to σ_p.

        δ*_p = ε_hh × σ_p / Σ σ_{p'}

        Kept for compatibility with old tests/callers; baseline allocation now
        uses _allocate_house() and never applies a positive minimum delta.
        """
        if not plug_uids:
            return {}

        total_sigma = sum(self._plug_states[p].sigma for p in plug_uids)

        if total_sigma == 0:
            # Equal allocation if no sigma info
            equal = epsilon_hh / len(plug_uids)
            return {p: equal for p in plug_uids}

        result: dict[int, float] = {}
        for plug_uid in plug_uids:
            sigma = self._plug_states[plug_uid].sigma
            delta = epsilon_hh * (sigma / total_sigma)
            result[plug_uid] = delta

        return result

    # ── Diagnostics ───────────────────────────────────────────────────────

    def sigma_summary(self) -> dict[int, float]:
        """Return current sigma estimates for all plugs. Used for logging."""
        return {uid: s.sigma for uid, s in self._plug_states.items()}

    def delta_summary(self) -> dict[int, float]:
        """Return current delta assignments for all plugs."""
        return {uid: s.delta for uid, s in self._plug_states.items()}
