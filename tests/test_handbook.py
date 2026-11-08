"""Runs the Python recipes of docs/HANDBOOK.md section 9 on synthetic data,
so the handbook can never drift away from the code."""
import sys, os, re, json
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import quarterwatt as qw
qw.USE_GOOGLE_DRIVE = False
rng = np.random.default_rng(1)
for code in ["DE", "BE", "FR", "PL"]:
    qw.configure(code)
    out = os.path.join(qw.resolve_base_dir(), "output"); os.makedirs(out, exist_ok=True)
    idx = pd.date_range(pd.Timestamp("2023-01-01", tz=qw.TZ), pd.Timestamp("2026-01-01", tz=qw.TZ), freq="15min", inclusive="left")
    h = idx.hour + idx.minute / 60
    m = pd.DataFrame(index=idx)
    m["P_DA"] = 80 + 30 * np.sin((h - 6) / 24 * 2 * np.pi) + rng.normal(0, 15, len(idx))
    m["Load_Actual"] = 50000 + 8000 * np.sin((h - 6) / 24 * 2 * np.pi)
    m["Gen_Solar"] = np.clip(np.sin((h - 6) / 12 * np.pi), 0, None) * 20000
    m = qw.add_derived(m)
    m.to_parquet(os.path.join(out, f"{qw.DATASET_NAME}_15min.parquet"))
    m.resample("1h").mean().to_parquet(os.path.join(out, f"{qw.DATASET_NAME}_1h.parquet"))
    json.dump({"failures": [], "gap_months_remaining": 0, "datasets": {}}, open(os.path.join(out, "manifest.json"), "w"))
qw.build_panel(["DE", "BE", "FR", "PL"])
ref = qw.reference_dir(); os.makedirs(os.path.join(ref, "load_profiles"), exist_ok=True); os.makedirs(os.path.join(ref, "tariffs"), exist_ok=True)
t = pd.date_range("2024-01-01", periods=200, freq="15min", tz="Europe/Berlin")
pd.DataFrame({"timestamp": t, "plant": "2016_IP1", "kW": 100.0}).to_parquet(os.path.join(ref, "load_profiles", "braeuer2020.parquet"), index=False)
pd.DataFrame({"h0": 1.0}, index=pd.Index(t, name="timestamp")).to_parquet(os.path.join(ref, "load_profiles", "bdew_slp.parquet"))
pd.DataFrame({"component": ["EEG"], "category": ["A"], "year": [2024], "ct_per_kWh": [0.0], "source": ["x"], "note": [""],
              "valid_from": ["2024-01-01"], "valid_to": ["2024-12-31"]}).to_csv(os.path.join(ref, "tariffs", "DE_levies.csv"), index=False)
hb = open("HANDBOOK.md").read()
sec = hb[hb.index("## 9. Working with the data"):hb.index("## 10. Reference data")]
blocks = re.findall(r"```python\n(.*?)```", sec, re.S)
p15 = 'os.path.join(qw.zone_location("DE")[0], "output", "Quarterwatt_DE_15min.parquet")'
ns = {}
ok = 0
for i, b in enumerate(blocks):
    b = b.replace('loc["2025"]', 'loc["2025"]').replace("pd.read_parquet(...15min...)", f"pd.read_parquet({p15})")
    try:
        exec(b, ns); ok += 1
    except Exception as ex:
        print(f"recipe {i} failed: {type(ex).__name__}: {ex}\n{b}")
print(f"handbook recipes run: {ok}/{len(blocks)}")
print("lwaec plausible:", 60 < ns["lwaec"] < 110, "| profile factor:", round(ns["lwaec"] / ns["baseload"], 3) > 1,
      "| capture years:", list(ns["capture"].index), "| joined rows:", len(ns["both"]), "| wide cols:", sorted(ns["wide"].columns))
