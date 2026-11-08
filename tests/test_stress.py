import sys, os, json, warnings
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import quarterwatt as sb
warnings.simplefilter("error")          # every warning = failure
sb.USE_GOOGLE_DRIVE = False
idx_all = pd.period_range("2010-01", "2026-12", freq="M")
for cur, v in {"RON": 4.8, "CZK": 25.0, "PLN": 4.3, "HUF": 380, "HRK": 7.5, "TRY": 30.0}.items():
    sb._ECB_CACHE[cur] = pd.Series(v, index=idx_all)
rng = np.random.default_rng(3)
def zone(code, start="2015-01-01", nan_share=0.03, extra=None, nosolar=False):
    sb.configure(code)
    out = os.path.join(sb.resolve_base_dir(), "output"); os.makedirs(out, exist_ok=True)
    idx = pd.date_range(pd.Timestamp("2015-01-01", tz=sb.TZ), pd.Timestamp("2026-11-03", tz=sb.TZ), freq="1h", inclusive="left")
    n = len(idx); h = np.asarray(idx.hour)
    df = pd.DataFrame(index=idx)
    p = 50 + 20*np.sin(h/24*2*np.pi) + rng.normal(0, 15, n); p[rng.random(n) < nan_share] = np.nan
    p[idx < pd.Timestamp(start, tz=sb.TZ)] = np.nan
    df["P_DA"] = p
    L = np.asarray(30000 + 5000*np.sin((h-6)/24*2*np.pi)); L[rng.random(n) < nan_share] = np.nan; df["Load_Actual"] = L
    if not nosolar: df["Gen_Solar"] = np.clip(np.sin((h - 6)/12*np.pi), 0, None) * 5000
    df["Gen_Fossil_Gas"] = np.where(rng.random(n) < 0.02, np.nan, 15000.0)
    df["Gen_Wind_Onshore"] = rng.uniform(0, 9000, n)
    df["WX_Temp_C"] = 10 + rng.normal(0, 6, n); df["WX_GHI_Wm2"] = np.clip(np.sin((h - 6)/12*np.pi), 0, None)*600
    df["P_Imb_Long"] = df["P_DA"] + rng.normal(0, 40, n)
    if extra: extra(df)
    df = sb.add_derived(df)
    df.to_parquet(os.path.join(out, f"{sb.DATASET_NAME}_1h.parquet"))
    json.dump({"failures": [], "datasets": {}}, open(os.path.join(out, "manifest.json"), "w")) if False else open(os.path.join(out, "manifest.json"), "w").write(json.dumps({"failures": [], "datasets": {}}))
    return df
for z in sb.ALL_ZONES:
    zone(z, start="2016-10-31" if z == "BG" else ("2017-10-18" if z == "HR" else "2015-01-01"),
         nosolar=z in ("NO_3", "IE_SEM"))
summ, iss = sb.audit()
panel, mon = sb.build_panel()
print(panel.dtypes.value_counts().to_dict(), panel.shape)
print(mon.isna().mean().round(2).sort_values().tail(5).to_dict())
y = pd.read_csv("Quarterwatt/data/panel/panel_yearly.csv"); print(y.shape, y.columns.tolist()[:8])
print(sb.load_panel(["P_DA"], zones=["DE", "TR"]).groupby("zone", observed=True).size().to_dict())
print(len(pd.read_csv("Quarterwatt/data/panel/data_dictionary.csv")), "dictionary rows;",
      pd.read_csv("Quarterwatt/data/panel/zones.csv").shape)
print("---- finalize")
r = sb.finalize()
