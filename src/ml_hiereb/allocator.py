"""
HierEB Threshold Allocator.

Implements the two-stage budget allocation from the paper:

  Stage 1 – House → Household:
    ε_hh = ε_h × (Σ σ_p for plugs in hh) / (Σ σ_p for all plugs in house)

  Stage 2 – Household → Plug (water-filling):
    δ*_p = ε_hh × σ_p / (Σ σ_{p'} for plugs in hh)

Variance tracking:
  - Welford online algorithm for transmitted residuals (actual - predicted)
  - Censored correction for suppressed timesteps (we only know |e| ≤ δ)
  - 5-iteration solver sufficient per paper

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
from dataclasses import dataclass, field

import structlog

log = structlog.get_logger(__name__)


# ─── Variance tracking ────────────────────────────────────────────────────────

@dataclass
class WelfordState:
    """
    Online variance estimator (Welford's algorithm).
    Updates incrementally with each transmitted residual.
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
    Per-plug variance state combining transmitted and censored observations.

    Censored observations: timesteps where plug was suppressed (|e| ≤ δ).
    We don't observe the actual error, but we know it's bounded.
    """
    plug_uid: int
    welford: WelfordState = field(default_factory=WelfordState)

    # Censored observations buffer: list of δ values at suppressed timesteps
    censored_deltas: list[float] = field(default_factory=list)

    # Current sigma estimate (updated after each reallocation)
    sigma: float = 1.0   # default 1W to avoid zero-division at startup

    # Current delta assignment
    delta: float = math.inf

    def record_transmitted(self, residual: float) -> None:
        """Called when plug transmitted: we observe the true residual."""
        self.welford.update(residual)

    def record_suppressed(self, delta: float) -> None:
        """
        Called when plug was suppressed.
        We only know |e| ≤ delta (censored observation).
        Store delta for use in censored variance correction.
        """
        self.censored_deltas.append(delta)

    def estimate_sigma(self, n_iter: int = 5) -> float:
        """
        Estimate std dev of residuals incorporating censored observations.

        Algorithm (iterative, 5 iterations sufficient per paper):
          1. Start with Welford estimate from transmitted residuals
          2. For each censored obs (bounded by δ), add its contribution
             proportional to a truncated normal approximation
          3. Repeat until convergence

        Falls back to Welford std if no censored observations.

        Returns:
            Updated sigma estimate (minimum 0.1W to avoid numerical issues)
        """
        # Base estimate from transmitted residuals
        base_std = self.welford.std

        if not self.censored_deltas or self.welford.n == 0:
            sigma = max(base_std, 0.1)
            self.sigma = sigma
            return sigma

        sigma = max(base_std, 0.1)  # starting point

        n_obs = self.welford.n + len(self.censored_deltas)

        for _ in range(n_iter):
            # Sum of squared contributions
            ss = self.welford.M2  # from transmitted

            # Add truncated normal contribution from each censored observation
            # E[e² | |e| ≤ δ] ≈ σ² × (1 - 2φ(δ/σ)/(2Φ(δ/σ)-1)) [normal approximation]
            # Simplified: use δ²/3 for uniform approximation (conservative, fast)
            for delta in self.censored_deltas:
                # Truncated normal 2nd moment approximation
                ratio = delta / sigma if sigma > 0 else 1.0
                # Mills ratio approximation for moderate ratio
                # For ratio > 3: contribution ≈ 0 (almost no censoring effect)
                if ratio >= 3.0:
                    contribution = 0.0
                else:
                    # Conservative: assume uniform on [-delta, delta]
                    # Var[uniform(-δ, δ)] = δ²/3
                    contribution = (delta ** 2) / 3.0
                ss += contribution

            sigma = max(math.sqrt(ss / n_obs), 0.1)

        self.sigma = sigma
        return sigma

    def reset_period(self) -> None:
        """Clear censored buffer after reallocation (keep Welford rolling)."""
        self.censored_deltas.clear()


# ─── Allocator ────────────────────────────────────────────────────────────────

# House structure type alias
HouseStructure = dict[int, dict[int, list[int]]]  # house → hh → [plug_uid]


class HierEBAllocator:
    """
    Two-stage hierarchical threshold allocator.

    Stage 1: distribute ε_h to households proportional to their total sigma
    Stage 2: distribute ε_hh to plugs proportional to individual sigma (water-filling)
    """

    def __init__(self, epsilon_h: float) -> None:
        if epsilon_h <= 0:
            raise ValueError(f"epsilon_h must be positive, got {epsilon_h}")
        self.epsilon_h = epsilon_h
        self._plug_states: dict[int, PlugVarianceState] = {}
        self._realloc_count = 0

    # ── State management ──────────────────────────────────────────────────

    def get_or_create_plug(self, plug_uid: int) -> PlugVarianceState:
        if plug_uid not in self._plug_states:
            self._plug_states[plug_uid] = PlugVarianceState(plug_uid=plug_uid)
        return self._plug_states[plug_uid]

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
        self._realloc_count += 1
        new_deltas: dict[int, float] = {}

        for house_id, hh_structure in house_structure.items():
            all_plug_uids = [p for hh in hh_structure.values() for p in hh]

            # ── Step 0: refresh sigma estimates for all plugs in this house ──
            for plug_uid in all_plug_uids:
                state = self.get_or_create_plug(plug_uid)
                state.estimate_sigma()

            house_total_sigma = sum(
                self._plug_states[p].sigma for p in all_plug_uids
            )

            if house_total_sigma == 0:
                # No variance data yet: assign uniform small delta
                for plug_uid in all_plug_uids:
                    fallback = 1.0
                    self._plug_states[plug_uid].delta = fallback
                    new_deltas[plug_uid] = fallback
                continue

            # ── Stage 1: house → household ────────────────────────────────
            for hh_id, plug_uids in hh_structure.items():
                hh_sigma = sum(self._plug_states[p].sigma for p in plug_uids)
                epsilon_hh = eps_h * (hh_sigma / house_total_sigma)

                # ── Stage 2: household → plug (water-filling) ────────────
                hh_deltas = self._water_fill(plug_uids, epsilon_hh)
                for plug_uid, delta in hh_deltas.items():
                    self._plug_states[plug_uid].delta = delta
                    new_deltas[plug_uid] = delta

        # Clear censored buffers for the new period
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

    def _water_fill(
        self,
        plug_uids: list[int],
        epsilon_hh: float,
    ) -> dict[int, float]:
        """
        Stage-2 water-filling: distribute ε_hh among plugs proportional to σ_p.

        δ*_p = ε_hh × σ_p / Σ σ_{p'}

        Minimum delta: 0.1W (numerical floor to avoid zero suppression threshold).
        """
        if not plug_uids:
            return {}

        total_sigma = sum(self._plug_states[p].sigma for p in plug_uids)

        if total_sigma == 0:
            # Equal allocation if no sigma info
            equal = max(epsilon_hh / len(plug_uids), 0.1)
            return {p: equal for p in plug_uids}

        result: dict[int, float] = {}
        for plug_uid in plug_uids:
            sigma = self._plug_states[plug_uid].sigma
            delta = epsilon_hh * (sigma / total_sigma)
            result[plug_uid] = max(delta, 0.1)  # floor at 0.1W

        return result

    # ── Diagnostics ───────────────────────────────────────────────────────

    def sigma_summary(self) -> dict[int, float]:
        """Return current sigma estimates for all plugs. Used for logging."""
        return {uid: s.sigma for uid, s in self._plug_states.items()}

    def delta_summary(self) -> dict[int, float]:
        """Return current delta assignments for all plugs."""
        return {uid: s.delta for uid, s in self._plug_states.items()}