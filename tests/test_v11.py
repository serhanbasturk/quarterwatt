"""v1.1: regelleistung.net harmonisation, quarter-hour panel indicators,
data dictionary units/licences, raw-table dictionary."""
import sys, os, json
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import quarterwatt as qw
qw.USE_GOOGLE_DRIVE = False
TZ = "Europe/Berlin"

# ---- H1: aFRR old (EUR/MW per 4-h product) and new (EUR/MW/h) schema ----------
old_t = pd.date_range(pd.Timestamp("2021-11-29", tz=TZ), periods=6 * 3, freq="4h")
new_t = pd.date_range(pd.Timestamp("2021-12-02", tz=TZ), periods=6 * 3, freq="4h")
a = pd.DataFrame(index=old_t.append(new_t))
a.index.name = "timestamp"
a["RL_aFRR_Up_GERMANY_MIN_CAPACITY_PRICE_EUR_MW"] = np.where(a.index < new_t[0], 1.0, np.nan)
a["RL_aFRR_Up_GERMANY_AVERAGE_CAPACITY_PRICE_EUR_MW"] = np.where(a.index < new_t[0], 40.0, np.nan)
a["RL_aFRR_Up_GERMANY_MARGINAL_CAPACITY_PRICE_EUR_MW"] = np.where(a.index < new_t[0], 99.0, np.nan)
a["RL_aFRR_Up_GERMANY_AVERAGE_CAPACITY_PRICE_EUR_MW_H"] = np.where(a.index >= new_t[0], 12.0, np.nan)
a["RL_aFRR_Up_GERMANY_SUM_OF_OFFERED_CAPACITY_MW"] = 1000.0
h = qw.harmonise_rl(a.copy(), "aFRR")
v = h["RL_CapPrice_aFRR_Up"]
print("H1 old per hour:", sorted(v[v.index < new_t[0]].unique()), "| new:", sorted(v[v.index >= new_t[0]].unique()),
      "| raw kept:", "RL_aFRR_Up_GERMANY_AVERAGE_CAPACITY_PRICE_EUR_MW" in h.columns)

# ---- H2: FCR German column ends, cross-border continues; DST day 3-h product ----
t = pd.date_range("2022-03-26", "2022-03-29", freq="4h", inclusive="left").tz_localize(TZ)  # local 00/04/08..., DST day: 00-04 = 3 h
f = pd.DataFrame(index=t)
f.index.name = "timestamp"
f["RL_FCR_Sym_GERMANY_SETTLEMENTCAPACITY_PRICE_EUR_MW"] = np.where(t < pd.Timestamp("2022-03-28", tz=TZ), 60.0, np.nan)
f["RL_FCR_Sym_CROSSBORDER_SETTLEMENTCAPACITY_PRICE_EUR_MW"] = 80.0
g = qw.harmonise_rl(f.copy(), "FCR")["RL_CapPrice_FCR_Sym"]
dst0 = g.loc[pd.Timestamp("2022-03-27 00:00", tz=TZ)]
print("H2 before:", g.iloc[0], "| DST 00-04 (3 h):", dst0, "| after German end:", g.iloc[-1])

# ---- H3: dictionary units and licences --------------------------------------
cases = ["Elia_netregulationvolume", "Elia_systemimbalance", "Elia_ace", "Elia_imbalanceprice", "Elia_alpha",
         "P_SMF_TRY", "P_SMF", "P_DA_TRY", "P_DA_USD", "P_DA_FR", "NRV_Deutschland", "aFRR_Act_Amprion_Positiv",
         "aFRR_Act_Operational", "FCR_NTP_Deutschland_Negativ", "NetPosition_DA", "Imb_Volume_Total",
         "RL_CapPrice_aFRR_Up", "Solar_Forecast_Error"]
print("H3", {c: qw._describe(c)[1] for c in cases})
real = ["Elia_ace", "FCR_NTP_50Hertz_Negativ", "NRV_AEP_Knappheitskomponente", "NRV_Mrl_Mol_Abweichung",
        "NetPosition_DA", "P_IDM_WAP_TRY", "aFRR_Act_MOL_Abweichung", "mFRR_Act_TransnetBW_Positiv",
        "Imb_Volume_Total", "TR_YAL_net", "TR_FCR_Capacity_Price_EUR", "P_DA_USD"]
print("H3 unknown licences:", [c for c in real if qw.licence_of(c)[0].startswith("unknown")],
      "| empty units:", [c for c in real if not qw._describe(c)[1]])

# ---- H4: quarter-hour indicators ------------------------------------------------
idx = pd.date_range(pd.Timestamp("2025-10-01", tz=TZ), pd.Timestamp("2025-12-01", tz=TZ), freq="15min", inclusive="left")
p15 = pd.Series(50.0, index=idx)
p15[(idx.hour == 13) & (idx.minute == 0)] = -20.0          # one negative quarter per day
p15[(idx.hour == 19) & (idx.minute == 45)] = 200.0          # one spike quarter per day
hourly = pd.DataFrame({"P_DA": p15.resample("1h").mean()})
mon = qw._monthly(hourly, "DE", "M", p15)
print("H4 Oct neg hours hourly / qh:", mon.loc[0, "neg_price_hours"], mon.loc[0, "neg_price_qh_hours"],
      "| spread hourly / qh:", round(mon.loc[0, "daily_spread_mean"], 2), round(mon.loc[0, "daily_spread_qh_mean"], 2))
yr = qw._monthly(hourly, "DE", "Y")
print("H4 without p15 no qh cols:", "neg_price_qh_hours" not in yr.columns)

# ---- H5: build_panel writes the raw dictionary, no timestamp row, qh columns -----
for code in ["DE", "BE"]:
    qw.configure(code)
    out = os.path.join(qw.resolve_base_dir(), "output"); os.makedirs(out, exist_ok=True)
    m = pd.DataFrame({"P_DA": p15.values, "Load_Actual": 50000.0}, index=idx)
    m.index.name = "timestamp"
    m = qw.add_derived(m)
    m.to_parquet(os.path.join(out, f"{qw.DATASET_NAME}_15min.parquet"))
    qw.add_derived(m.resample("1h").mean()).to_parquet(os.path.join(out, f"{qw.DATASET_NAME}_1h.parquet"))
    ds = {"P_DA": {"rows": 10, "merged_into_master": True, "columns": ["P_DA"]}}
    if code == "DE":
        ds["RL_aFRR"] = {"rows": 36, "merged_into_master": False, "group": "experimental", "unit": "EUR/MW, MW",
                         "description": "aFRR capacity auction results", "first": "2020-07-01", "last": "2026-11-06",
                         "columns": ["RL_aFRR_Up_GERMANY_AVERAGE_CAPACITY_PRICE_EUR_MW", "RL_CapPrice_aFRR_Up"]}
        ds["Eurostat_Prices"] = {"rows": 0, "merged_into_master": False, "columns": []}
    json.dump({"failures": [], "gap_months_remaining": 0, "datasets": ds}, open(os.path.join(out, "manifest.json"), "w"))
qw.build_panel(["DE", "BE"])
pdir = qw.panel_dir()
raw = pd.read_csv(os.path.join(pdir, "data_dictionary_raw.csv"))
dd = pd.read_csv(os.path.join(pdir, "data_dictionary.csv"))
pm = pd.read_csv(os.path.join(pdir, "panel_monthly.csv"))
print("H5 raw tables:", raw[["zone", "table", "licence"]].values.tolist())
print("H5 timestamp listed:", "timestamp" in set(dd["column"]), "| qh cols:",
      {"neg_price_qh_hours", "daily_spread_qh_mean"} <= set(pm.columns),
      "| DE Oct qh neg:", pm[(pm.zone == "DE") & (pm.month == "2025-10")]["neg_price_qh_hours"].iloc[0])
print("H6 version:", qw.CODE_VERSION)
