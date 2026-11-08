import os, glob, json, sys
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import quarterwatt as qw
qw.USE_GOOGLE_DRIVE = False
def read(f): return open(f, "rb").read().decode("latin-1")
# R4: BG in BGN until 2021-05-11, uncoupled 25 % cheaper than GR
qw._ECB_CACHE.clear()
idx = pd.date_range("2015-01-01", "2026-11-01", freq="1h", tz="Europe/Berlin", inclusive="left")
rng = np.random.default_rng(7)
gr = 60 + 15*np.sin(np.asarray(idx.hour)/24*2*np.pi) + rng.normal(0, 6, len(idx))
qw.configure("GR"); out = os.path.join(qw.resolve_base_dir(), "output"); os.makedirs(out, exist_ok=True)
pd.DataFrame({"P_DA": gr}, index=idx).to_parquet(os.path.join(out, f"{qw.DATASET_NAME}_1h.parquet"))
sw = pd.Timestamp("2021-05-11", tz="Europe/Berlin")
truth = gr * np.where(idx < sw, 0.75, 1.0) * rng.uniform(0.9, 1.1, len(idx))
bg = np.where(idx < sw, truth * 1.95583, truth)
qw.configure("BG"); lg = {}; df = qw.harmonise_currency(pd.DataFrame({"P_DA": bg}, index=idx), lg)
ratio = (df["P_DA"] / truth)
print("R4 hours still in BGN:", int((ratio > 1.5).sum()), "| hours wrongly divided:", int((ratio < 0.7).sum()), "| to:", lg["currency_conversions"][0]["to"])
# R5: missing reference -> logged + audit WARN
qw.configure("CZ"); lg = {}; qw.harmonise_currency(pd.DataFrame({"P_DA": gr}, index=idx), lg); print("R5 skipped logged:", lg.get("currency_check_skipped"))
# R6: months without rate -> NaN + logged; stale warning
qw._ECB_CACHE["PLN"] = pd.Series(4.3, index=pd.period_range("2018-01", "2026-12", freq="M"))
qw.configure("PL"); lg = {}
x = qw.harmonise_currency(pd.DataFrame({"P_DA": 50.0, "P_DA_DE": 50.0, "P_Imb_Long": 215.0}, index=idx), lg)
print("R6 imb 2017 NaN:", bool(x.loc["2017", "P_Imb_Long"].isna().all()), "| 2019 value:", float(x.loc["2019-03", "P_Imb_Long"].iloc[0]),
      "| logged:", [c.get("months_without_rate") for c in lg["currency_conversions"]])
# R7: yearly aggregates from hourly data
qw.configure("DE"); hh = pd.DataFrame({"P_DA": np.r_[np.full(8760, 10.0)], "Load_Actual": 1.0}, index=pd.date_range("2023-01-01", periods=8760, freq="1h", tz="Europe/Berlin"))
hh.iloc[100, 0] = 999.0; hh.iloc[5000:5100, 1] = 100.0; hh.iloc[5000:5100, 0] = 50.0
y = qw._monthly(hh, "DE", "Y").iloc[0]
print("R7 yearly max:", y["P_DA_max"], "| loadweighted:", round(y["P_DA_loadweighted"], 3),
      "expected", round(((hh.P_DA*hh.Load_Actual).sum()/hh.Load_Actual.sum()), 3), "| col:", "year" in qw._monthly(hh, "DE", "Y").columns)
# R9: zone restored after project-level calls; offline rebuild of a never-downloaded zone
qw.configure("BE"); qw.status(["DE", "TR"]); print("R9 zone after status:", qw.COUNTRY, qw.TZ)
qw.OFFLINE = True; qw.configure("LT"); r = qw.main(); qw.OFFLINE = False
print("R9 offline undownloaded:", r, "| folder created:", os.path.exists("Quarterwatt/data/zones/LT_Lithuania/output"))
# R10: genuine EUR spike after the switch must not move the switch day
qw._ECB_CACHE.pop("PLN", None)
bg2 = bg.copy(); spike = (idx >= pd.Timestamp("2021-05-28", tz="Europe/Berlin")) & (idx < pd.Timestamp("2021-05-29", tz="Europe/Berlin"))
truth2 = truth.copy(); truth2[spike] *= 2.0; bg2[spike] = truth2[spike]
qw.configure("BG"); lg = {}; d2 = qw.harmonise_currency(pd.DataFrame({"P_DA": bg2}, index=idx), lg)
r2 = d2["P_DA"] / truth2
print("R10 wrong hours with spike:", int(((r2 > 1.5) | (r2 < 0.7)).sum()), "| to:", lg["currency_conversions"][0]["to"])
# R11: fx_check with fixed-rate currency
qw._ECB_CACHE.clear(); print("R11 BGN check:", qw.fx_check(("BGN",)))
# R12: months without rate counted + audit WARN key
print("R12 months_without_rate:", [c["months_without_rate"] for c in lg_pl] if False else None)
qw._ECB_CACHE["PLN"] = pd.Series(4.3, index=pd.period_range("2018-01", "2026-12", freq="M"))
qw.configure("PL"); lgp = {}
qw.harmonise_currency(pd.DataFrame({"P_DA": 50.0, "P_DA_DE": 50.0, "P_Imb_Long": 215.0}, index=idx), lgp)
print("R12 months without rate:", [c.get("months_without_rate") for c in lgp["currency_conversions"]])
# R13: FAILURES survive status()
qw.FAILURES[:] = [{"dataset": "X", "segment": "s", "errors": ["e"]}]; qw.status(["DE"]); print("R13 failures kept:", len(qw.FAILURES))
