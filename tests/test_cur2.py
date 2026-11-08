import sys, os, json
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import quarterwatt as sb
sb.USE_GOOGLE_DRIVE = False
idx_all = pd.period_range("2010-01", "2026-12", freq="M")
for cur, v in {"RON": 4.8, "BGN": 1.95583, "CZK": 25.0, "PLN": 4.3, "HUF": 380, "HRK": 7.5, "NOK": 11.5}.items():
    sb._ECB_CACHE[cur] = pd.Series(v, index=idx_all)
rng = np.random.default_rng(1)
idx = pd.date_range("2015-01-01", "2026-11-01", freq="1h", tz="Europe/Berlin", inclusive="left")
base = 45 + 20*np.sin(idx.hour/24*2*np.pi) + rng.normal(0, 8, len(idx))
def save(code, s):
    sb.configure(code); out = os.path.join(sb.resolve_base_dir(), "output"); os.makedirs(out, exist_ok=True)
    pd.DataFrame({"P_DA": s}, index=idx).to_parquet(os.path.join(out, f"{sb.DATASET_NAME}_1h.parquet"))
save("GR", base); save("SK", base); save("FI", np.where(idx >= "2023-01-01", 3.0, base))
# BG: EUR level 0.75 x GR, in BGN until 2022-01-21 12:00 ; low GR month in 2020-05
bg = base * 0.75; bgn = idx < pd.Timestamp("2022-01-21", tz="Europe/Berlin"); bg = np.where(bgn, bg * 1.95583, bg)
df = pd.DataFrame({"P_DA": bg}, index=idx); sb.configure("BG"); lg = {}; df = sb.harmonise_currency(df, lg)
m = df["P_DA"].resample("YE").median().round(1); print("BG yearly median:", m.tolist())
print("BG days around switch:", df["P_DA"].loc["2022-01-19":"2022-01-23"].resample("D").median().round(1).tolist())
# NO_1 -> not touched
no = pd.DataFrame({"P_DA": base, "P_Imb_Long": base}, index=idx); sb.configure("NO_1"); lg = {}; sb.harmonise_currency(no, lg); print("NO_1:", lg)
# HU imbalance in HUF only in some months, negative values present
hu = pd.DataFrame({"P_DA": base, "P_Imb_Long": base + rng.normal(0, 40, len(idx))}, index=idx)
hufm = (idx.month % 3 == 0) & (idx.year < 2026); hu.loc[hufm, "P_Imb_Long"] *= 380
sb.configure("HU"); lg = {}; hu = sb.harmonise_currency(hu, lg)
print("HU imb max ratio month:", round((hu["P_Imb_Long"].abs().resample("ME").median() / hu["P_DA"].resample("ME").median()).max(), 2))
# HR forced until 2023
hr = pd.DataFrame({"P_DA": base, "P_Imb_Long": np.where(idx < pd.Timestamp("2023-01-01", tz="Europe/Berlin"), base*7.5, base)}, index=idx)
sb.configure("HR"); sb._ECB_CACHE.pop("EUR", None); lg = {}; hr = sb.harmonise_currency(hr, lg)
print("HR imb/DA by year:", (hr["P_Imb_Long"].resample("YE").median() / hr["P_DA"].resample("YE").median()).round(2).tolist())
# ID-AEP csv ISO
txt = "Datum von;(Uhrzeit) von;Zeitzone;(Uhrzeit) bis;Zeitzone;ID AEP in €/MWh\n2020-06-30;22:00;UTC;22:15;UTC;45.67\n2020-06-30;22:15;UTC;22:30;UTC;-12.5\n"
print(sb.parse_ntp_csv(txt, "P_ID_AEP_"))
txt2 = "Datum;von;Zeitzone;bis;reBAP unterdeckt\n01.07.2020;00:00;UTC;00:15;1.234,56\n"
print(sb.parse_ntp_csv(txt2, "P_"))
