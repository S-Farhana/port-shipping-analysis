"""
Port & Shipping Cargo Analysis — Visualizations
Big Data Mini Project 2 | Farhana
Charts 1–5: matplotlib + seaborn
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter
import seaborn as sns
import numpy as np
import os
import warnings
warnings.filterwarnings("ignore")

os.makedirs("output/charts", exist_ok=True)

# ── Google Fonts fallback stack ───────────────────────────────────
plt.rcParams.update({
    "font.family":      "DejaVu Sans",
    "axes.spines.top":  False,
    "axes.spines.right":False,
    "figure.facecolor": "#0D1117",
    "axes.facecolor":   "#0D1117",
    "text.color":       "#E6EDF3",
    "axes.labelcolor":  "#E6EDF3",
    "xtick.color":      "#8B949E",
    "ytick.color":      "#8B949E",
    "axes.edgecolor":   "#30363D",
    "grid.color":       "#21262D",
    "grid.linewidth":   0.6,
})

# ── Palette ───────────────────────────────────────────────────────
BLUE   = "#58A6FF"
TEAL   = "#39D353"
AMBER  = "#F0883E"
PURPLE = "#BC8CFF"
PINK   = "#FF7B72"
DARK   = "#0D1117"
CARD   = "#161B22"
BORDER = "#30363D"

# ═══════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════
job1 = pd.read_csv("output/hdfs_results/job1/job1/part-r-00000",
                   sep="\t", header=None, names=["port", "volume"], quoting=3)

job2 = pd.read_csv("output/hdfs_results/job2/job2/part-r-00000",
                   sep="\t", header=None, names=["port", "avg_delay"],quoting=3)

job3 = pd.read_csv("output/hdfs_results/job3/job3/part-r-00000",
                   sep="\t", header=None, names=["route", "efficiency"])

# Parse job4 partition files
job4_rows = []
for f in ["part-r-00000", "part-r-00001", "part-r-00002"]:
    fpath = f"output/hdfs_results/job4/job4/{f}"
    if os.path.exists(fpath):
        with open(fpath) as fp:
            for line in fp:
                parts = line.strip().split("\t")
                if len(parts) >= 2:
                    try:
                        region   = parts[0]
                        avg_eff  = float(parts[1].replace("avg_efficiency=", "").split()[0])
                        records  = int(parts[2].replace("total_records=", "")) if len(parts) > 2 else 0
                        job4_rows.append({"region": region,
                                          "avg_efficiency": avg_eff,
                                          "total_records":  records})
                    except Exception:
                        pass
job4 = pd.DataFrame(job4_rows)

# Split route → port + cargo_type
job3[["port", "cargo_type"]] = job3["route"].str.rsplit("_", n=1, expand=True)

print(f"✅ Data loaded  |  job1={len(job1)} ports  |  job2={len(job2)} ports  "
      f"|  job3={len(job3)} routes  |  job4={len(job4)} regions")


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════
def add_title_bar(fig, title, subtitle=""):
    fig.text(0.05, 0.97, title,    fontsize=17, fontweight="bold",
             color="#E6EDF3", va="top")
    fig.text(0.05, 0.93, subtitle, fontsize=10, color="#8B949E", va="top")

def comma_fmt(x, _):
    return f"{x:,.0f}"

def save(name):
    path = f"output/charts/{name}"
    plt.savefig(path, dpi=150, bbox_inches="tight",
                facecolor=DARK, edgecolor="none")
    plt.close()
    print(f"  ✅ {name}")


# ═══════════════════════════════════════════════════════════════════
# CHART 1 — Top 10 Ports by Cargo Volume
# ═══════════════════════════════════════════════════════════════════
print("\n📊 Generating Chart 1 — Top 10 Ports by Cargo Volume")
top10 = job1.nlargest(10, "volume").sort_values("volume")

fig, ax = plt.subplots(figsize=(13, 7))
fig.patch.set_facecolor(DARK)
ax.set_facecolor(CARD)

# Gradient-style bars using alpha steps
n = len(top10)
alphas = np.linspace(0.45, 1.0, n)
for i, (_, row) in enumerate(top10.iterrows()):
    ax.barh(row["port"], row["volume"],
            color=BLUE, alpha=alphas[i], height=0.65,
            linewidth=0)

# Accent bar for #1
ax.barh(top10.iloc[-1]["port"], top10.iloc[-1]["volume"],
        color=TEAL, alpha=1.0, height=0.65, linewidth=0)

# Value labels
for _, row in top10.iterrows():
    ax.text(row["volume"] + top10["volume"].max() * 0.01,
            row["port"], f'{row["volume"]:,.0f}',
            va="center", fontsize=9, color="#8B949E")

ax.xaxis.set_major_formatter(FuncFormatter(comma_fmt))
ax.set_xlabel("Total Cargo Volume", fontsize=11, labelpad=10)
ax.set_title("Top 10 Ports by Cargo Volume",
             fontsize=16, fontweight="bold", pad=18, color="#E6EDF3")
ax.set_facecolor(CARD)
ax.tick_params(axis="y", labelsize=10)
ax.grid(axis="x", linestyle="--", alpha=0.3)

# Legend patch
patch1 = mpatches.Patch(color=TEAL,  label="Highest volume")
patch2 = mpatches.Patch(color=BLUE,  label="Top 10 ports", alpha=0.8)
ax.legend(handles=[patch1, patch2], loc="lower right",
          facecolor=CARD, edgecolor=BORDER,
          labelcolor="#E6EDF3", fontsize=9)

fig.text(0.05, 0.01, "Source: MapReduce Job 1 — port_data_clean.csv",
         fontsize=8, color="#484F58")
plt.tight_layout(rect=[0, 0.03, 1, 1])
save("chart1_top10_ports_cargo.png")


# ═══════════════════════════════════════════════════════════════════
# CHART 2 — Top 15 Ports by Average Vessel Delay (Heatmap)
# ═══════════════════════════════════════════════════════════════════
print("📊 Generating Chart 2 — Vessel Delay Heatmap")
top15 = job2.nlargest(15, "avg_delay").sort_values("avg_delay", ascending=False)

fig, ax = plt.subplots(figsize=(13, 8))
fig.patch.set_facecolor(DARK)
ax.set_facecolor(CARD)

pivot = top15.set_index("port")[["avg_delay"]]
cmap  = sns.color_palette("rocket", as_cmap=True)

sns.heatmap(pivot, annot=True, fmt=".1f", cmap=cmap,
            linewidths=1.5, linecolor=DARK,
            ax=ax, cbar_kws={"label": "Avg Delay (hours)",
                             "shrink": 0.6},
            annot_kws={"size": 11, "color": "white", "weight": "bold"})

ax.set_title("Top 15 Ports — Average Vessel Delay",
             fontsize=16, fontweight="bold", pad=18, color="#E6EDF3")
ax.set_xlabel("", fontsize=0)
ax.set_ylabel("", fontsize=0)
ax.tick_params(axis="y", labelsize=10, colors="#E6EDF3", rotation=0)
ax.tick_params(axis="x", labelsize=10, colors="#8B949E")

cbar = ax.collections[0].colorbar
cbar.ax.yaxis.label.set_color("#8B949E")
cbar.ax.tick_params(colors="#8B949E")

fig.text(0.05, 0.01, "Source: MapReduce Job 2 — vessel_schedule.csv",
         fontsize=8, color="#484F58")
plt.tight_layout(rect=[0, 0.03, 1, 1])
save("chart2_vessel_delay_heatmap.png")


# ═══════════════════════════════════════════════════════════════════
# CHART 3 — Top 10 Routes by Efficiency Score
# ═══════════════════════════════════════════════════════════════════
print("📊 Generating Chart 3 — Top 10 Route Efficiency")
top10r = job3.nlargest(10, "efficiency").sort_values("efficiency")

fig, ax = plt.subplots(figsize=(13, 7))
fig.patch.set_facecolor(DARK)
ax.set_facecolor(CARD)

alphas_r = np.linspace(0.45, 1.0, len(top10r))
for i, (_, row) in enumerate(top10r.iterrows()):
    ax.barh(row["route"], row["efficiency"],
            color=TEAL, alpha=alphas_r[i], height=0.65, linewidth=0)

ax.barh(top10r.iloc[-1]["route"], top10r.iloc[-1]["efficiency"],
        color=AMBER, alpha=1.0, height=0.65, linewidth=0)

for _, row in top10r.iterrows():
    ax.text(row["efficiency"] + top10r["efficiency"].max() * 0.01,
            row["route"], f'{row["efficiency"]:.1f}',
            va="center", fontsize=9, color="#8B949E")

ax.set_xlabel("Avg Efficiency Score  (cargo_tons / dwell_hours)",
              fontsize=11, labelpad=10)
ax.set_title("Top 10 Routes by Efficiency Score",
             fontsize=16, fontweight="bold", pad=18, color="#E6EDF3")
ax.grid(axis="x", linestyle="--", alpha=0.3)
ax.tick_params(axis="y", labelsize=9)

patch_a = mpatches.Patch(color=AMBER, label="Highest efficiency")
patch_g = mpatches.Patch(color=TEAL,  label="Top 10 routes", alpha=0.8)
ax.legend(handles=[patch_a, patch_g], loc="lower right",
          facecolor=CARD, edgecolor=BORDER,
          labelcolor="#E6EDF3", fontsize=9)

fig.text(0.05, 0.01, "Source: MapReduce Job 3 — vessel_schedule.csv",
         fontsize=8, color="#484F58")
plt.tight_layout(rect=[0, 0.03, 1, 1])
save("chart3_top10_route_efficiency.png")


# ═══════════════════════════════════════════════════════════════════
# CHART 4 — Region-wise Efficiency (Job 4 Partitioner)
# ═══════════════════════════════════════════════════════════════════
print("📊 Generating Chart 4 — Region Efficiency")

if not job4.empty:
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.patch.set_facecolor(DARK)

    palette = {"Asia": BLUE, "Europe": TEAL, "Americas": AMBER}
    region_colors = [palette.get(r, PURPLE) for r in job4["region"]]

    # Bar chart
    ax = axes[0]
    ax.set_facecolor(CARD)
    bars = ax.bar(job4["region"], job4["avg_efficiency"],
                  color=region_colors, width=0.5,
                  edgecolor=DARK, linewidth=1.5)
    for bar, val in zip(bars, job4["avg_efficiency"]):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.5,
                f"{val:.2f}", ha="center", fontsize=13,
                fontweight="bold", color="#E6EDF3")
    ax.set_ylabel("Avg Efficiency Score", fontsize=11)
    ax.set_title("Avg Route Efficiency by Region",
                 fontsize=13, fontweight="bold", color="#E6EDF3", pad=12)
    ax.set_ylim(0, job4["avg_efficiency"].max() * 1.2)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    # Records pie
    ax2 = axes[1]
    ax2.set_facecolor(CARD)
    if "total_records" in job4.columns and job4["total_records"].sum() > 0:
        wedges, texts, autotexts = ax2.pie(
            job4["total_records"],
            labels=job4["region"],
            autopct="%1.1f%%",
            colors=region_colors,
            startangle=140,
            pctdistance=0.78,
            wedgeprops={"edgecolor": DARK, "linewidth": 2}
        )
        for t in texts:
            t.set_color("#E6EDF3"); t.set_fontsize(12)
        for at in autotexts:
            at.set_color(DARK); at.set_fontsize(10); at.set_fontweight("bold")
        ax2.set_title("Record Distribution by Region",
                      fontsize=13, fontweight="bold",
                      color="#E6EDF3", pad=12)
    else:
        ax2.text(0.5, 0.5, "No record count\nin output",
                 ha="center", va="center", color="#8B949E",
                 fontsize=12, transform=ax2.transAxes)
        ax2.set_title("Record Distribution by Region",
                      fontsize=13, fontweight="bold",
                      color="#E6EDF3", pad=12)

    fig.suptitle("Job 4 — Custom Partitioner Output",
                 fontsize=15, fontweight="bold",
                 color="#E6EDF3", y=1.01)
    fig.text(0.05, -0.02, "Source: MapReduce Job 4 — RegionPartitioner",
             fontsize=8, color="#484F58")
    plt.tight_layout()
    save("chart4_region_efficiency.png")
else:
    print("  ⚠️  Job4 data empty — skipping Chart 4")


# ═══════════════════════════════════════════════════════════════════
# CHART 5 — Cargo Type Efficiency (Donut + Bar side-by-side)
# ═══════════════════════════════════════════════════════════════════
print("📊 Generating Chart 5 — Cargo Type Efficiency")
cargo_avg = (job3.groupby("cargo_type")["efficiency"]
               .mean()
               .sort_values(ascending=False))

fig, axes = plt.subplots(1, 2, figsize=(15, 7))
fig.patch.set_facecolor(DARK)

colors5 = [BLUE, TEAL, AMBER, PURPLE, PINK,
           "#79C0FF", "#56D364", "#FFA657"][:len(cargo_avg)]

# Donut
ax_d = axes[0]
ax_d.set_facecolor(CARD)
wedges, texts, autotexts = ax_d.pie(
    cargo_avg.values,
    labels=cargo_avg.index,
    autopct="%1.1f%%",
    colors=colors5,
    startangle=140,
    pctdistance=0.78,
    wedgeprops={"edgecolor": DARK, "linewidth": 2, "width": 0.55}
)
for t in texts:
    t.set_color("#E6EDF3"); t.set_fontsize(11)
for at in autotexts:
    at.set_color(DARK); at.set_fontsize(9); at.set_fontweight("bold")
ax_d.set_title("Share of Avg Efficiency\nby Cargo Type",
               fontsize=13, fontweight="bold",
               color="#E6EDF3", pad=12)

# Horizontal bar
ax_b = axes[1]
ax_b.set_facecolor(CARD)
y_pos = range(len(cargo_avg))
h_bars = ax_b.barh(list(cargo_avg.index), cargo_avg.values,
                   color=colors5, height=0.6, linewidth=0)
for bar, val in zip(h_bars, cargo_avg.values):
    ax_b.text(val + cargo_avg.max() * 0.01,
              bar.get_y() + bar.get_height() / 2,
              f"{val:.1f}", va="center", fontsize=9, color="#8B949E")
ax_b.set_xlabel("Avg Efficiency Score", fontsize=11, labelpad=10)
ax_b.set_title("Avg Efficiency Score\nby Cargo Type",
               fontsize=13, fontweight="bold",
               color="#E6EDF3", pad=12)
ax_b.grid(axis="x", linestyle="--", alpha=0.3)
ax_b.invert_yaxis()

fig.suptitle("Cargo Type Efficiency Analysis",
             fontsize=16, fontweight="bold",
             color="#E6EDF3", y=1.01)
fig.text(0.05, -0.02, "Source: MapReduce Job 3 — vessel_schedule.csv",
         fontsize=8, color="#484F58")
plt.tight_layout()
save("chart5_cargo_type_efficiency.png")


# ═══════════════════════════════════════════════════════════════════
# CHART 6 — Summary Dashboard (2×2 mini panels)
# ═══════════════════════════════════════════════════════════════════
print("📊 Generating Chart 6 — Summary Dashboard")
fig = plt.figure(figsize=(16, 10))
fig.patch.set_facecolor(DARK)
gs = gridspec.GridSpec(2, 2, figure=fig,
                       hspace=0.42, wspace=0.35)

# Panel A — Top 5 cargo volume
ax_a = fig.add_subplot(gs[0, 0])
ax_a.set_facecolor(CARD)
t5 = job1.nlargest(5, "volume").sort_values("volume")
ax_a.barh(t5["port"], t5["volume"],
          color=BLUE, alpha=0.85, height=0.6)
ax_a.set_title("Top 5 Ports — Cargo Volume",
               fontsize=11, fontweight="bold", color="#E6EDF3", pad=8)
ax_a.xaxis.set_major_formatter(FuncFormatter(comma_fmt))
ax_a.tick_params(labelsize=8)
ax_a.grid(axis="x", linestyle="--", alpha=0.3)

# Panel B — Top 5 vessel delay
ax_b = fig.add_subplot(gs[0, 1])
ax_b.set_facecolor(CARD)
t5d = job2.nlargest(5, "avg_delay").sort_values("avg_delay")
ax_b.barh(t5d["port"], t5d["avg_delay"],
          color=PINK, alpha=0.85, height=0.6)
ax_b.set_title("Top 5 Ports — Avg Vessel Delay (hrs)",
               fontsize=11, fontweight="bold", color="#E6EDF3", pad=8)
ax_b.tick_params(labelsize=8)
ax_b.grid(axis="x", linestyle="--", alpha=0.3)

# Panel C — Cargo type bar
ax_c = fig.add_subplot(gs[1, 0])
ax_c.set_facecolor(CARD)
ax_c.bar(cargo_avg.index, cargo_avg.values,
         color=colors5[:len(cargo_avg)], width=0.6)
ax_c.set_title("Avg Efficiency by Cargo Type",
               fontsize=11, fontweight="bold", color="#E6EDF3", pad=8)
ax_c.tick_params(axis="x", rotation=20, labelsize=8)
ax_c.tick_params(axis="y", labelsize=8)
ax_c.grid(axis="y", linestyle="--", alpha=0.3)

# Panel D — Region efficiency
ax_d2 = fig.add_subplot(gs[1, 1])
ax_d2.set_facecolor(CARD)
if not job4.empty:
    rc = [palette.get(r, PURPLE) for r in job4["region"]]
    ax_d2.bar(job4["region"], job4["avg_efficiency"],
              color=rc, width=0.5)
    ax_d2.set_title("Region Efficiency (Job 4)",
                    fontsize=11, fontweight="bold", color="#E6EDF3", pad=8)
    ax_d2.grid(axis="y", linestyle="--", alpha=0.3)
    ax_d2.tick_params(labelsize=9)
else:
    ax_d2.text(0.5, 0.5, "Job 4 data\nnot available",
               ha="center", va="center", color="#8B949E",
               fontsize=11, transform=ax_d2.transAxes)
    ax_d2.set_title("Region Efficiency (Job 4)",
                    fontsize=11, fontweight="bold", color="#E6EDF3", pad=8)

fig.suptitle("Port & Shipping Cargo Analysis — Summary Dashboard",
             fontsize=17, fontweight="bold",
             color="#E6EDF3", y=1.01)
fig.text(0.5, -0.01,
         "Big Data Mini Project 2  |  Hadoop MapReduce  |  S. Farhana  |  2026",
         ha="center", fontsize=9, color="#484F58")

plt.savefig("output/charts/chart6_summary_dashboard.png",
            dpi=150, bbox_inches="tight",
            facecolor=DARK, edgecolor="none")
plt.close()
print("  ✅ chart6_summary_dashboard.png")

print("\n🎉 All charts saved to output/charts/")
print("   chart1_top10_ports_cargo.png")
print("   chart2_vessel_delay_heatmap.png")
print("   chart3_top10_route_efficiency.png")
print("   chart4_region_efficiency.png")
print("   chart5_cargo_type_efficiency.png")
print("   chart6_summary_dashboard.png")