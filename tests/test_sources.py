import os, sys, json
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
os.environ["ENTSOE_API_KEY"] = "k"; os.environ.pop("NTP_CLIENT_ID", None)
import quarterwatt as qw
qw.USE_GOOGLE_DRIVE = False
# S1 secrets: env var read, nothing hard-coded (pattern of real keys, no real values here)
import re
_SECRET_RE = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|ntp_[A-Za-z0-9]{8,}|cm_app_ntp_id_[0-9a-f]{8,}"
src = open("quarterwatt.py").read()
print("S1 key from env:", qw.ENTSOE_API_KEY == "k", "| no credentials in code:",
      not re.search(_SECRET_RE, src) and "PASSWORD = \"" not in src)
print("S1 check:", qw.credentials_check())
try:
    qw.ntp_token(); print("S1 ntp: no error")
except RuntimeError as ex: print("S1 ntp error:", "credentials missing" in str(ex))
qw.configure("DE"); print("S1 NTP datasets still registered:", any(d["name"].startswith("NTP_") for d in qw.DATASETS))
qw.configure("TR"); print("S1 EPIAS datasets registered:", any(d["name"].startswith("EPIAS_") for d in qw.DATASETS))
# S2 balancing energy price fn: 1-min data, two series per direction
idx = pd.date_range("2024-03-01", periods=120, freq="1min", tz="Europe/Brussels")
class C:
    def query_activated_balancing_energy_prices(self, area, start, end, business_type):
        a = pd.DataFrame({"Price": np.r_[np.full(60, 100.0), np.full(60, 200.0)], "Direction": "Up", "ReserveType": "aFRR"}, index=idx)
        b = pd.DataFrame({"Price": 50.0, "Direction": "Down", "ReserveType": "aFRR"}, index=idx)
        a2 = a.copy(); a2["Price"] = a2["Price"] + 20
        return pd.concat([a, a2, b]).sort_index()
    def query_activated_balancing_energy(self, area, start, end, business_type):
        cols = pd.MultiIndex.from_tuples([("Automatic frequency restoration reserve", "Up"), ("Automatic frequency restoration reserve", "Down")])
        return pd.DataFrame([[10.0, 5.0]] * 8, index=pd.date_range("2024-03-01", periods=8, freq="15min", tz="Europe/Brussels"), columns=cols)
    def query_intraday_prices(self, area, start, end, sequence):
        return pd.Series(float(sequence), index=pd.date_range(start, periods=4, freq="15min"))
    def query_generation_forecast(self, area, start, end):
        if (end - start) > pd.Timedelta(days=1): raise AttributeError("'numpy.float64' object has no attribute 'tz_convert'")
        if start.day == 3: raise AttributeError("bad day")
        return pd.Series(1.0, index=pd.date_range(start, end, freq="1h", inclusive="left"))
c = C()
pr = qw.make_bal_price_fn("aFRR")(c, "BE", None, None)
print("S2 cols:", list(pr.columns), "| rows:", len(pr), "| first up:", pr.iloc[0]["P_BalE_aFRR_Up"], "| 4th up:", pr.iloc[3]["P_BalE_aFRR_Up"])
print("S2 IDA2:", qw.make_ida_fn(2)(c, "BE", pd.Timestamp("2024-07-01", tz="Europe/Berlin"), None).columns.tolist())
qw.configure("DE"); ida = [d for d in qw.DATASETS if d["name"] == "P_IDA2"][0]
print("S2 IDA areas before/after start:", ida["areas"](pd.Timestamp("2024-01-01", tz="Europe/Berlin")), ida["areas"](pd.Timestamp("2024-07-01", tz="Europe/Berlin")))
# S3 gen forecast fallback
g = qw.f_gen_forecast(c, "BG", pd.Timestamp("2018-10-01", tz="Europe/Berlin"), pd.Timestamp("2018-10-06", tz="Europe/Berlin"))
print("S3 fallback hours:", len(g), "(4 good days x 24 = 96)")
# S4 licence tags
print("S4 licences:", qw.licence_of("P_DA")[1], qw.licence_of("P_DA_FR")[1], qw.licence_of("Elia_imbalanceprice")[1], qw.licence_of("WX_Temp_C")[1], qw.licence_of("P_reBAP_unterdeckt")[1], qw.licence_of("P_BalE_aFRR_Up")[1], qw.licence_of("Strange")[0][:7])
# S5 reference data: levies + SLP (demandlib) + Braeuer parser (mock CSV, both layouts)
os.makedirs("Quarterwatt/data/reference/load_profiles/braeuer2020_raw", exist_ok=True)
t = pd.date_range("2016-01-01", periods=96*3, freq="15min")
pd.DataFrame({"Time": t.strftime("%d.%m.%Y %H:%M"), "IP1": 10.5, "IP2": 3.25}).to_csv(
    "Quarterwatt/data/reference/load_profiles/braeuer2020_raw/LoadProfile_20IPs_2016.csv", sep=";", decimal=",", index=False)
pd.DataFrame({"P1": np.arange(96*2, dtype=float), "P2": 1.0}).to_csv(
    "Quarterwatt/data/reference/load_profiles/braeuer2020_raw/LoadProfile_30IPs_2017.csv", index=False)
qw.SLP_YEARS = range(2024, 2026)
qw.update_reference()
lev = qw.load_reference("levies"); slp = qw.load_reference("slp"); br = qw.load_reference("braeuer")
print("S5 levies:", len(lev), "| EEG 2022:", lev[(lev.component == "EEG") & (lev.year == 2022)].ct_per_kWh.iloc[0],
      "| Offshore 2026:", lev[(lev.component == "Offshore") & (lev.year == 2026)].ct_per_kWh.iloc[0])
e24 = slp.loc["2024", "g0"].sum() * 0.25
print("S5 SLP:", slp.shape[1], "profiles | 2024 kWh per MWh/a:", round(e24, 1), "| tz:", slp.index.tz, "| quarters 2024:", len(slp.loc["2024"]), "| NaN:", int(slp.isna().sum().sum()))
print("S5 braeuer:", sorted(br.plant.unique()), br.groupby("plant").kW.first().round(2).to_dict(), "| first time:", br.timestamp.min())
# R-fixes
qw.configure("DE"); ida = [d for d in qw.DATASETS if d["name"] == "P_IDA3"][0]; bal = [d for d in qw.DATASETS if d["name"] == "BalE_Price_aFRR"][0]
print("F1 IDA June 2024:", ida["areas"](pd.Timestamp("2024-06-01", tz="Europe/Berlin")), "| BalE 2018/2019:", bal["areas"](pd.Timestamp("2018-12-01", tz="Europe/Berlin")), bal["areas"](pd.Timestamp("2019-01-01", tz="Europe/Berlin")))
lev = qw.load_reference("levies"); e22 = lev[(lev.component == "EEG") & (lev.year == 2022)]
print("F8 EEG 2022 periods:", [(str(a.date()), str(b.date()), v) for a, b, v in zip(e22.valid_from, e22.valid_to, e22.ct_per_kWh)])
print("F7 licences:", qw.licence_of("VRE_Generation")[1], qw.licence_of("Imb_Volume_Total")[0][:7], qw.licence_of("N_Borders_Reporting")[1])
# audit on a zone with realistic balancing prices: no currency WARN
qw.configure("DE"); out = os.path.join(qw.resolve_base_dir(), "output"); os.makedirs(out, exist_ok=True)
i = pd.date_range("2024-01-01", "2025-01-01", freq="1h", tz="Europe/Berlin", inclusive="left")
rng = np.random.default_rng(1)
d = pd.DataFrame({"P_DA": 80 + rng.normal(0, 20, len(i)), "Load_Actual": 50000.0,
                  "P_BalE_aFRR_Up": 400 + rng.normal(0, 100, len(i)), "P_BalE_aFRR_Down": 2 + rng.normal(0, 5, len(i))}, index=i)
d.to_parquet(os.path.join(out, "Quarterwatt_DE_1h.parquet"))
iss, _, _ = qw._audit_zone("DE")
print("F2 BalE currency WARNs:", [x["message"][:40] for x in iss if x["check"] == "currency" and x["level"] == "WARN"])
d["P_BalE_aFRR_Up"] *= 25; d.to_parquet(os.path.join(out, "Quarterwatt_DE_1h.parquet"))
iss, _, _ = qw._audit_zone("DE"); print("F2 BalE in CZK detected:", any("P_BalE_aFRR_Up: median >8x" in x["message"] for x in iss))
