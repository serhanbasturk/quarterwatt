import sys, os, json
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import quarterwatt as sb
sb.USE_GOOGLE_DRIVE = False
idx_all = pd.period_range("2010-01", "2026-12", freq="M")
for cur, v in {"RON": 4.8, "BGN": 1.95583, "CZK": 25.0, "PLN": 4.3, "HUF": 380, "TRY": 30.0}.items():
    sb._ECB_CACHE[cur] = pd.Series(v, index=idx_all)
rng = np.random.default_rng(1)
def table(code, eur_until=None, factor=1, imb_factor=None, ref=None):
    sb.configure(code)
    idx = pd.date_range("2015-01-01", "2026-11-01", freq="15min", tz=sb.TZ, inclusive="left")
    df = pd.DataFrame(index=idx)
    base = 40 + 20*np.sin(idx.hour/24*2*np.pi) + rng.normal(0, 8, len(idx))
    df["P_DA"] = base
    if eur_until:
        m = idx < pd.Timestamp(eur_until, tz=sb.TZ)
        df.loc[m, "P_DA"] *= factor
    if imb_factor:
        df["P_Imb_Long"] = base * imb_factor + rng.normal(0, 30, len(idx))
    df["Load_Actual"] = 5000.0
    return df
# HU reference must exist as a 1h output
hu = table("HU"); out = os.path.join(sb.resolve_base_dir(), "output"); os.makedirs(out, exist_ok=True)
hu.resample("1h").mean().to_parquet(os.path.join(out, "SB_Energy_Dataset_Hungary_1h.parquet"))
ro = table("RO", eur_until="2021-06-17 00:00", factor=4.8, imb_factor=4.8)
log = {}; sb.configure("RO"); ro = sb.harmonise_currency(ro, log)
print(json.dumps(log, indent=1))
print("RO monthly median after:", ro["P_DA"].resample("ME").median().loc["2021-04":"2021-08"].round(1).tolist())
print("RO 2021-06 daily medians around switch:", ro["P_DA"].loc["2021-06-14":"2021-06-19"].resample("D").median().round(1).tolist())
# BG without reference -> note
log = {}; sb.configure("BG"); bg = sb.harmonise_currency(table("BG", eur_until="2021-01-01", factor=1.95583), log); print(log)
# PL with own P_DA_DE column, EUR all the time -> no conversion expected
pl = table("PL"); pl["P_DA_DE"] = pl["P_DA"] * 0.8; log = {}; sb.configure("PL"); sb.harmonise_currency(pl, log); print("PL clean:", log)
# DE file with neighbour P_DA_PL in PLN 2017-2019
de = table("DE"); de["P_DA_PL"] = de["P_DA"]; m = (de.index >= "2017-03-01") & (de.index < "2019-12-01"); de.loc[m, "P_DA_PL"] *= 4.3
log = {}; sb.configure("DE"); sb.harmonise_currency(de, log); print(log)
