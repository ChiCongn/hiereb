from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

FILE_PATH = Path("outputs/debs-house0-smoke/comparison.csv")

OUTPUT_DIR = Path("plots")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def add_bar_labels(ax, decimals: int = 3) -> None:
    for container in ax.containers:
        ax.bar_label(
            container,
            fmt=f"%.{decimals}f",
            padding=3,
            fontsize=8,
            rotation=90,
        )


def save_plot(fig, filename: str) -> None:
    fig.tight_layout()
    output_path = OUTPUT_DIR / filename
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.show()
    plt.close(fig)
    print(f"Saved: {output_path}")


if not FILE_PATH.is_file():
    raise FileNotFoundError(f"Không tìm thấy file: {FILE_PATH}")

df = pd.read_csv(FILE_PATH)

required_columns = {
    "mode",
    "transmitted",
    "suppressed",
    "transmission_ratio",
    "reduction",
    "mae",
    "rmse",
    "p95",
    "p99",
    "max",
    "cvar95",
    "cvar99",
    "max_bound_utilization",
    "mean_budget_utilization",
    "cap_hit_count",
    "redistribution_rounds",
    "maximum_consecutive_suppression",
}

missing_columns = required_columns - set(df.columns)

if missing_columns:
    raise ValueError("File CSV thiếu các cột bắt buộc: " + ", ".join(sorted(missing_columns)))

df = df.set_index("mode")

display_names = {
    "full_tx": "Full TX",
    "uniform": "Uniform",
    "flat_variance": "Flat variance",
    "legacy_two_stage": "Legacy 2-stage",
    "true_hierarchical": "True hierarchical",
    "true_hierarchical_cap": "Hierarchical + cap",
}

df.index = [display_names.get(name, name) for name in df.index]

allocation_df = df.drop(index="Full TX", errors="ignore")


# 1. Transmitted và suppressed
fig, ax = plt.subplots(figsize=(12, 6))

df[["transmitted", "suppressed"]].plot(
    kind="bar",
    ax=ax,
)

ax.set_title("Transmitted vs Suppressed Events")
ax.set_xlabel("Allocation mode")
ax.set_ylabel("Number of events")
ax.tick_params(axis="x", rotation=25)
ax.grid(axis="y", alpha=0.3)
ax.legend(title="Event status")

add_bar_labels(ax, decimals=0)
save_plot(fig, "01_transmitted_vs_suppressed.png")


# 2. Transmission ratio và reduction
fig, ax = plt.subplots(figsize=(12, 6))

ratio_df = df[["transmission_ratio", "reduction"]] * 100

ratio_df.plot(
    kind="bar",
    ax=ax,
)

ax.set_title("Transmission Ratio and Reduction")
ax.set_xlabel("Allocation mode")
ax.set_ylabel("Percentage (%)")
ax.set_ylim(0, 110)
ax.tick_params(axis="x", rotation=25)
ax.grid(axis="y", alpha=0.3)
ax.legend(title="Metric")

for container in ax.containers:
    ax.bar_label(
        container,
        fmt="%.1f%%",
        padding=3,
        fontsize=8,
        rotation=90,
    )

save_plot(fig, "02_transmission_ratio_and_reduction.png")


# 3. MAE và RMSE
fig, ax = plt.subplots(figsize=(12, 6))

allocation_df[["mae", "rmse"]].plot(
    kind="bar",
    ax=ax,
)

ax.set_title("Average Reconstruction Error")
ax.set_xlabel("Allocation mode")
ax.set_ylabel("Error")
ax.tick_params(axis="x", rotation=25)
ax.grid(axis="y", alpha=0.3)
ax.legend(title="Metric")

add_bar_labels(ax, decimals=4)
save_plot(fig, "03_mae_rmse.png")


# 4. P95, P99 và maximum
fig, ax = plt.subplots(figsize=(12, 6))

allocation_df[["p95", "p99", "max"]].plot(
    kind="bar",
    ax=ax,
)

ax.set_title("Tail Reconstruction Error")
ax.set_xlabel("Allocation mode")
ax.set_ylabel("Error")
ax.tick_params(axis="x", rotation=25)
ax.grid(axis="y", alpha=0.3)
ax.legend(title="Metric")

add_bar_labels(ax, decimals=3)
save_plot(fig, "04_tail_error.png")


# 5. CVaR95 và CVaR99
fig, ax = plt.subplots(figsize=(12, 6))

allocation_df[["cvar95", "cvar99"]].plot(
    kind="bar",
    ax=ax,
)

ax.set_title("Tail Risk: CVaR95 and CVaR99")
ax.set_xlabel("Allocation mode")
ax.set_ylabel("CVaR")
ax.tick_params(axis="x", rotation=25)
ax.grid(axis="y", alpha=0.3)
ax.legend(title="Metric")

add_bar_labels(ax, decimals=3)
save_plot(fig, "05_cvar.png")


# 6. Budget utilization
fig, ax = plt.subplots(figsize=(12, 6))

allocation_df[
    [
        "max_bound_utilization",
        "mean_budget_utilization",
    ]
].plot(
    kind="bar",
    ax=ax,
)

ax.set_title("Bound and Budget Utilization")
ax.set_xlabel("Allocation mode")
ax.set_ylabel("Utilization ratio")
ax.set_ylim(0, 1.15)
ax.tick_params(axis="x", rotation=25)
ax.grid(axis="y", alpha=0.3)
ax.legend(title="Metric")

add_bar_labels(ax, decimals=3)
save_plot(fig, "06_budget_utilization.png")


# 7. Cap, redistribution và consecutive suppression
fig, ax = plt.subplots(figsize=(12, 6))

allocation_df[
    [
        "cap_hit_count",
        "redistribution_rounds",
        "maximum_consecutive_suppression",
    ]
].plot(
    kind="bar",
    ax=ax,
)

ax.set_title("Cap, Redistribution and Consecutive Suppression")
ax.set_xlabel("Allocation mode")
ax.set_ylabel("Count")
ax.tick_params(axis="x", rotation=25)
ax.grid(axis="y", alpha=0.3)
ax.legend(title="Metric")

add_bar_labels(ax, decimals=0)
save_plot(fig, "07_cap_and_redistribution.png")
