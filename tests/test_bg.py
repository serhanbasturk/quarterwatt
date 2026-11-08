import sys, os
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import quarterwatt as sb
sb.USE_GOOGLE_DRIVE = False
idx_all = pd.period_range("2010-01", "2026-12", freq="M")
sb._ECB_CACHE["RON"] = pd.Series(4.8, index=idx_all)
rng = np.random.default_rng(5)
idx = pd.date_range("2015-01-01", "2026-11-01", freq="1h", tz="Europe/Berlin", inclusive="left")
gr = 55 + 15*np.sin(np.asarray(idx.hour)/24*2*np.pi) + rng.normal(0, 8, len(idx))
sb.configure("GR"); out = os.path.join(sb.resolve_base_dir(), "output"); os.makedirs(out, exist_ok=True)
pd.DataFrame({"P_DA": gr}, index=idx).to_parquet(os.path.join(out, f"{sb.DATASET_NAME}_1h.parquet"))
sw = pd.Timestamp("2022-01-21", tz="Europe/Berlin")
lvl = np.where(idx < pd.Timestamp("2021-05-11", tz="Europe/Berlin"), 0.65 + 0.15*np.sin(np.asarray(idx.month)), 1.0)  # uncoupled: cheaper, varying
bg = gr * lvl * rng.uniform(0.85, 1.15, len(idx)); bg = np.where(idx < sw, bg * 1.95583, bg)
bg[idx < pd.Timestamp("2016-10-31", tz="Europe/Berlin")] = np.nan
truth = gr * lvl
df = pd.DataFrame({"P_DA": bg}, index=idx); sb.configure("BG"); lg = {}; df = sb.harmonise_currency(df, lg)
err = (df["P_DA"] / (truth * 1.0)).resample("ME").median()
print("BG months still ~1.96x too high:", int((err > 1.5).sum()), "| converted log:", lg["currency_conversions"][0]["from"], lg["currency_conversions"][0]["to"], lg["currency_conversions"][0]["days"])
print("switch days:", df["P_DA"].loc["2022-01-18":"2022-01-24"].resample("D").median().round(0).tolist(), "truth ~", round(np.median(truth[(idx>="2022-01-18")&(idx<"2022-01-25")]),0))
