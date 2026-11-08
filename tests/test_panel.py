import sys, os, json, warnings
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import quarterwatt as sb
sb.USE_GOOGLE_DRIVE = False
rng = np.random.default_rng(0)
def fake(code, shift=0, pln=False, identical_to=None, nosolar=False):
    sb.configure(code)
    out = os.path.join(sb.resolve_base_dir(), "output"); os.makedirs(out, exist_ok=True)
    idx = pd.date_range(pd.Timestamp("2015-01-01", tz=sb.TZ), pd.Timestamp.now(tz=sb.TZ).normalize() - pd.Timedelta(days=2), freq="1h", inclusive="left")
    n = len(idx); h = idx.hour
    df = pd.DataFrame(index=idx)
    df["P_DA"] = 50 + 20*np.sin(h/24*2*np.pi) + rng.normal(0, 10, n)
    if identical_to is not None: df["P_DA"] = identical_to.reindex(idx).values
    df["Load_Actual"] = 50000 + 10000*np.sin((h-6)/24*2*np.pi) + rng.normal(0, 500, n)
    sol = np.clip(np.sin((((h + shift) % 24) - 6)/12*np.pi), 0, None) * 10000
    if not nosolar: df["Gen_Solar"] = sol
    df["Gen_Wind_Onshore"] = rng.uniform(0, 20000, n)
    df["Gen_Fossil_Gas"] = 25000.0
    df["WX_Temp_C"] = 10 + rng.normal(0, 5, n); df["WX_GHI_Wm2"] = sol/20
    df["P_Imbalance"] = df["P_DA"] + rng.normal(0, 30, n)
    df.loc[df.index < "2016-01-01", "P_Imbalance"] = np.nan
    if pln:
        df["P_DA_DE"] = df["P_DA"]; df.loc["2017-03":"2017-05", "P_DA"] *= 4.3
    df.loc["2020-05-03":"2020-05-07", "Load_Actual"] = np.nan  # small gap
    df = sb.add_derived(df)
    df.to_parquet(os.path.join(out, f"{sb.DATASET_NAME}_1h.parquet"))
    json.dump({"failures": [1, 2] if code == "BE" else [], "gap_months_remaining": 0, "datasets": {}}, open(os.path.join(out, "manifest.json"), "w"))
    return df
de = fake("DE"); fake("BE", shift=3); fake("PL", pln=True); fake("SE_1", identical_to=de["P_DA"].tz_convert("UTC").tz_convert(sb.TZ)); fake("TR")
sb._ECB_CACHE["TRY"]=None
warnings.simplefilter("error", FutureWarning)
summ, iss = sb.audit(["DE", "BE", "PL", "SE_1", "TR", "FI"])
panel, mon = sb.build_panel(["DE", "BE", "PL", "SE_1", "TR", "FI"])
print(panel.head(3)); print(mon.iloc[60].to_string())
print(pd.read_csv("Quarterwatt/data/panel/panel_yearly.csv").head(3).T)
print(pd.read_csv("Quarterwatt/data/panel/data_dictionary.csv").to_string())
print(sb.load_panel(["P_DA"], zones=["TR"]).tail(2))
print(sb.load_zone("BE", ["P_DA", "nope"]).shape)
