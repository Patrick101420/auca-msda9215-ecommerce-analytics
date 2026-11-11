"""
Static figures for the report (PNG, 200 dpi) -> viz/figures/.

Inputs: cleaned Parquet (small aggregates computed by Spark here) + the CSV/JSON outputs
of the Spark and integration jobs. Run after jobs 01-07.

Style rules: one y-axis per chart, fixed categorical colour order (blue, orange, aqua), thin
marks, recessive grid, direct labels only where they carry the message.
"""
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mt
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "spark"))
from common import CLEAN, OUT, spark_session  # noqa: E402
from pyspark.sql import functions as F         # noqa: E402

FIG = os.path.join(ROOT, "viz", "figures")
os.makedirs(FIG, exist_ok=True)
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
plt.rcParams.update({
    "figure.facecolor": SURF, "axes.facecolor": SURF, "savefig.facecolor": SURF,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 9.5,
    "axes.titlesize": 11, "axes.titleweight": "bold", "axes.titlecolor": INK,
    "axes.titlelocation": "left", "legend.frameon": False,
})
money = mt.FuncFormatter(lambda v, _: f"${v / 1e6:.1f}M" if abs(v) >= 1e6 else f"${v / 1e3:.0f}k")


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, name), dpi=200)
    plt.close(fig)
    print("wrote", name)


spark = spark_session("08-figures")
rd = lambda n: spark.read.parquet(os.path.join(CLEAN, n))

# ---------------------------------------------------------------- 1. daily revenue by channel
daily = (rd("transactions").groupBy("txn_date", "channel").agg(F.sum("total").alias("rev"))
         .toPandas().pivot(index="txn_date", columns="channel", values="rev").fillna(0).sort_index())
daily = daily.iloc[1:-1]   # drop partial first/last day of the window
fig, ax = plt.subplots(figsize=(8, 3.6))
for col, c, lab in [("web_session", BLUE, "Session-linked orders"), ("unlinked", ORANGE, "Orders with no session")]:
    s = daily[col]
    ax.plot(s.index, s.values, color=c, lw=1, alpha=0.35)
    ax.plot(s.index, s.rolling(7, center=True).mean(), color=c, lw=2, label=f"{lab} (7-day mean)")
ax.yaxis.set_major_formatter(money)
ax.set_ylim(0, daily.max().max() * 1.12)
ax.set_title("Daily revenue by order channel: most revenue has no session behind it")
ax.set_ylabel("Revenue per day")
ax.legend(loc="lower left", ncol=2)
save(fig, "fig1_daily_revenue.png")

# ---------------------------------------------------------------- 2. conversion funnel
s = rd("sessions")
tx_linked = rd("transactions").filter("session_id IS NOT NULL").select("session_id", "user_id").distinct()
f = s.agg(F.count("*").alias("Sessions"),
          F.sum(F.col("reached_product").cast("int")).alias("Viewed a product"),
          F.sum((F.col("n_cart_lines") > 0).cast("int")).alias("Added to cart"),
          F.sum(F.col("reached_checkout").cast("int")).alias("Reached checkout"),
          F.sum((F.col("conversion_status") == "converted").cast("int")).alias("Labelled converted")).first().asDict()
f["Transaction recorded"] = s.join(tx_linked, ["session_id", "user_id"]).count()
stages = list(f.keys())
vals = np.array([f[k] for k in stages], dtype=float)
fig, ax = plt.subplots(figsize=(8, 3.8))
y = np.arange(len(stages))[::-1]
ax.barh(y, vals, color=[BLUE] * 5 + [AQUA], height=0.62)
for yi, v, k in zip(y, vals, stages):
    ax.text(v + vals[0] * 0.01, yi, f"{v / 1e6:.2f}M  ({100 * v / vals[0]:.1f}%)", va="center", color=INK, fontsize=9)
ax.set_yticks(y, stages)
ax.set_xlim(0, vals[0] * 1.25)
ax.xaxis.set_major_formatter(mt.FuncFormatter(lambda v, _: f"{v / 1e6:.1f}M"))
ax.set_title("Session funnel: 2M sessions to recorded transactions")
ax.grid(axis="y", visible=False)
save(fig, "fig2_funnel.png")
json.dump(f, open(os.path.join(OUT, "funnel_counts.json"), "w"), indent=2)

# ---------------------------------------------------------------- 3. revenue + est. profit by category
cat = pd.read_csv(os.path.join(OUT, "sql_category_weekly_revenue.csv"))
cat = cat.groupby("category_name")[["gross_revenue", "est_gross_profit"]].sum().sort_values("gross_revenue")
fig, ax = plt.subplots(figsize=(8, 6))
yy = np.arange(len(cat))
ax.barh(yy, cat["gross_revenue"], color=BLUE, height=0.7, label="Gross revenue")
ax.barh(yy, cat["est_gross_profit"], color=ORANGE, height=0.7, label="Estimated gross profit (category mean margin)")
ax.set_yticks(yy, cat.index, fontsize=8)
ax.xaxis.set_major_formatter(money)
ax.set_title("Revenue and estimated gross profit by category (90 days)")
ax.legend(loc="lower right")
ax.grid(axis="y", visible=False)
save(fig, "fig3_category_revenue.png")

# ---------------------------------------------------------------- 4. views vs units by stock state
pl = pd.read_csv(os.path.join(OUT, "product_leakage.csv"))
fig, ax = plt.subplots(figsize=(7.5, 5))
for state, c, lab, m, sz in [("in_stock", BLUE, "Still in stock", "o", 9), ("sold_out", ORANGE, "Sold out by day 90", "o", 9),
                             ("inactive", INK2, "Inactive listing", "x", 14)]:
    d = pl[pl.state == state]
    ax.scatter(d.views, d.units, s=sz, color=c, alpha=0.45 if m == "o" else 0.8, lw=0 if m == "o" else 0.8,
               marker=m, label=f"{lab} (n={len(d):,})")
ins, so_ = pl[pl.state == "in_stock"], pl[pl.state == "sold_out"]
ax.annotate(f"In stock: median {ins.views.median():,.0f} views,\n{ins.units.median():,.0f} units ({ins.upv.median():.2f} units/view)",
            xy=(ins.views.median() - 120, ins.units.median()), xytext=(1450, 230), fontsize=8.5, color=INK2,
            arrowprops=dict(arrowstyle="-", color=INK2, lw=0.6))
ax.annotate(f"Sold out: median {so_.views.median():,.0f} views,\n{so_.units.median():,.0f} units ({so_.upv.median():.2f} units/view)\n"
            "- hidden from shoppers once stock hit 0",
            xy=(so_.views.median(), so_.units.median()), xytext=(820, 90), fontsize=8.5, color=INK2,
            arrowprops=dict(arrowstyle="-", color=INK2, lw=0.6))
ax.set_xlabel("Product-detail views (90 days)")
ax.set_ylabel("Units sold (90 days)")
ax.set_title("Sold-out products lost ~73% of their exposure")
ax.legend(loc="upper center", fontsize=8, ncol=3, bbox_to_anchor=(0.5, -0.12))
save(fig, "fig4_views_vs_units.png")

# ---------------------------------------------------------------- 4b. lost revenue: stock-outs vs conversion
ls_ = json.load(open(os.path.join(OUT, "leakage_summary.json")))
fig, ax = plt.subplots(figsize=(7.5, 2.6))
labels = ["Lost to stock-outs\n(upper-bound estimate)", "Lost to weak conversion\n(in-stock products)",
          "Naive v1 estimate\n(rejected: confounded)"]
vals = [ls_["stockout_est_lost_revenue"], ls_["instock_conversion_leakage"], ls_["v1_naive_leakage_rejected"]]
cols = [ORANGE, BLUE, "#c9c8c2"]
yy = np.arange(3)[::-1]
ax.barh(yy, vals, color=cols, height=0.6)
for y_, v in zip(yy, vals):
    ax.text(v + 3e6, y_, f"${v / 1e6:,.1f}M  ({100 * v / ls_['total_revenue']:.1f}% of revenue)", va="center", fontsize=9, color=INK)
ax.set_yticks(yy, labels, fontsize=8.5)
ax.set_xlim(0, max(vals) * 1.45)
ax.xaxis.set_major_formatter(money)
ax.grid(axis="y", visible=False)
ax.set_title("Where revenue was lost (90 days)")
save(fig, "fig4b_lost_revenue.png")

# ---------------------------------------------------------------- 5. cohort: orders per member per day
co = pd.read_csv(os.path.join(OUT, "cohort_matrix.csv"), parse_dates=["cohort", "activity_month"])
hm = co.pivot(index="cohort", columns="activity_month", values="orders_per_member_per_day").sort_index()
fig, ax = plt.subplots(figsize=(7.5, 3.8))
# scale anchored at 0: equal colours ARE the finding (no cohort differs)
im = ax.imshow(hm.values, cmap=matplotlib.colors.LinearSegmentedColormap.from_list(
    "b", ["#f0efec", "#86b6ef", "#2a78d6", "#0d366b"]), vmin=0, vmax=0.8, aspect="auto")
ax.set_xticks(range(hm.shape[1]), [d.strftime("%b %Y") for d in hm.columns])
ax.set_yticks(range(hm.shape[0]), [d.strftime("%b %Y") for d in hm.index])
for i in range(hm.shape[0]):
    for j in range(hm.shape[1]):
        v = hm.values[i, j]
        if not np.isnan(v):
            ax.text(j, i, f"{v:.3f}", ha="center", va="center", fontsize=8, color="white")
ax.grid(False)
ax.set_xlabel("Activity month (Aug and Nov are partial: normalised per observed day)")
ax.set_ylabel("Registration cohort")
ax.set_title("Orders per member per day: identical across cohorts")
fig.colorbar(im, ax=ax, label="orders / member / day", shrink=0.85)
save(fig, "fig5_cohorts.png")

# ---------------------------------------------------------------- 6. lift: observed vs shuffled null
ob = pd.read_csv(os.path.join(OUT, "coview_lift_observed.csv")).lift
nu = pd.read_csv(os.path.join(OUT, "coview_lift_null.csv")).lift
bins = np.linspace(0, 8, 65)   # >99% of pairs; the long tail is pairs seen only 3 times
fig, ax = plt.subplots(figsize=(7.5, 3.6))
ax.hist(nu, bins=bins, color=ORANGE, alpha=0.55, label=f"Shuffled baskets (null), n={len(nu):,}")
ax.hist(ob, bins=bins, histtype="step", color=BLUE, lw=2, label=f"Observed co-viewed pairs, n={len(ob):,}")
ax.set_xlabel("Lift of product pair (1 = no affinity); x-axis clipped at 8")
ax.set_ylabel("Number of pairs")
ax.yaxis.set_major_formatter(mt.FuncFormatter(lambda v, _: f"{v / 1e3:.0f}k"))
ax.set_title("Co-view lift is indistinguishable from random baskets")
ax.legend()
save(fig, "fig6_lift_vs_null.png")

# ---------------------------------------------------------------- 7. CLV deciles: predicted vs actual
dec = pd.read_csv(os.path.join(OUT, "clv_deciles.csv"))
clv = pd.read_csv(os.path.join(OUT, "clv_by_user.csv"))
pred_by_dec = clv.groupby("clv_decile").pred_30d_spend.mean()
act_by_dec = clv.groupby("clv_decile").hold_spend.mean()
fig, ax = plt.subplots(figsize=(7.5, 3.6))
x = np.arange(1, 11)
ax.bar(x - 0.2, pred_by_dec.values, width=0.38, color=BLUE, label="Predicted 30-day spend")
ax.bar(x + 0.2, act_by_dec.values, width=0.38, color=AQUA, label="Actual holdout spend")
ax.set_xticks(x, [f"D{i}" for i in x])
ax.set_xlabel("Customer decile by predicted CLV (D1 = highest)")
ax.set_ylabel("Mean spend per customer")
ax.yaxis.set_major_formatter(mt.FuncFormatter(lambda v, _: f"${v:,.0f}"))
ax.set_title("CLV model: predicted vs realised spend in the holdout month")
ax.legend(loc="upper center", ncol=2, bbox_to_anchor=(0.5, -0.2))
ax.grid(axis="x", visible=False)
save(fig, "fig7_clv_deciles.png")

# ---------------------------------------------------------------- 8. engine benchmark (small multiples, log scale)
bm = json.load(open(os.path.join(OUT, "benchmark.json")))
fig, axes = plt.subplots(1, 2, figsize=(8, 2.8))
for ax, q, title in [(axes[0], "QA", "A: one user's last 5 sessions"), (axes[1], "QB", "B: top-20 products, last 30 days")]:
    eng = ["MongoDB", "HBase", "Spark/Parquet"]
    v = [bm[f"{q}_mongo_ms"], bm[f"{q}_hbase_ms"], bm[f"{q}_spark_ms"]]
    best = int(np.argmin(v))
    ax.barh(np.arange(3)[::-1], v, color=[AQUA if i == best else "#c9c8c2" for i in range(3)], height=0.6)
    ax.set_xscale("log")
    ax.set_yticks(np.arange(3)[::-1], eng)
    for y_, x_ in zip(np.arange(3)[::-1], v):
        ax.text(x_ * 1.15, y_, f"{x_:,.1f} ms" if x_ < 1000 else f"{x_ / 1000:,.1f} s", va="center", fontsize=8.5)
    ax.set_xlim(min(v) / 3, max(v) * 12)
    ax.set_title(title, fontsize=10)
    ax.grid(axis="y", visible=False)
fig.suptitle("Same question, three engines (best warm run, log scale)", x=0.01, ha="left", fontweight="bold", fontsize=11)
save(fig, "fig8_benchmark.png")

spark.stop()
