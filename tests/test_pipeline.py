import sys, os, io, json
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd, requests
import quarterwatt as dw
from entsoe.exceptions import NoMatchingDataError
CALLS = {"wx": 0, "es": 0, "fred": 0, "idaep": 0}
class R:
    def __init__(self, code, text="", js=None):
        self.status_code, self.text, self.content, self._js = code, text, text.encode(), js
    def json(self): return self._js
    def raise_for_status(self):
        if self.status_code >= 400:
            e = requests.HTTPError(str(self.status_code)); e.response = self; raise e
def fake_get(url, headers=None, timeout=None, params=None):
    if "open-meteo" in url:
        CALLS["wx"] += 1
        n = len(params["latitude"].split(","))
        t = pd.date_range(params["start_date"], pd.Timestamp(params["end_date"]) + pd.Timedelta(hours=23), freq="1h", tz="UTC")
        locs = [{"hourly": {"time": [int(x.timestamp()) for x in t], "temperature_2m": [10.0 * (i + 1)] * len(t),
                 "shortwave_radiation": [100.0] * len(t), "wind_speed_100m": [20.0] * len(t), "cloud_cover": [50.0] * len(t)}} for i in range(n)]
        return R(200, js=locs if n > 1 else locs[0])
    if "eurostat" in url:
        CALLS["es"] += 1
        if "205_c" in url:
            rows = "DATAFLOW,LAST UPDATE,freq,product,nrg_cons,nrg_prc,unit,currency,geo,TIME_PERIOD,OBS_VALUE\n" + \
                   "\n".join(f"E,x,A,6000,MWH2000-19999,{c},KWH,EUR,{g},{y},{v}" for g in ["DE","TR","SE"] for y in [2017,2018] for c,v in [("NRG_SUP",0.05),("NETC",0.03)])
        else:
            rows = "DATAFLOW,LAST UPDATE,freq,product,nrg_cons,unit,tax,currency,geo,TIME_PERIOD,OBS_VALUE,OBS_FLAG\n" + \
                   "\n".join(f"E,x,S,6000,{b},KWH,{tx},{cur},{g},{p},{v}," for g in ["DE","TR","SE","EL"] for p in ["2015-S1","2015-S2"]
                             for b in ["MWH20-499","MWH2000-19999"] for tx,v in [("X_TAX",0.10),("I_TAX",0.15)] for cur in ["EUR","NAC"])
        return R(200, rows)
    if "fred" in url:
        CALLS["fred"] += 1
        d = pd.date_range("2014-01-01", "2026-09-01", freq="MS")
        txt = "observation_date,PNGASEUUSDM,PCOALAUUSDM,POILBREUSDM,EXUSEU\n" + "\n".join(f"{x:%Y-%m-%d},10.0,100.0,80.0,1.10" for x in d)
        return R(200, txt)
    if "netztransparenz" in url:
        a, b = url.rsplit("/", 2)[-2:]
        if "IdAep" in url:
            CALLS["idaep"] += 1
            t = pd.date_range(pd.Timestamp(a, tz="UTC"), pd.Timestamp(b, tz="UTC"), freq="15min", inclusive="left")
            lines = ["Datum von;(Uhrzeit) von;Zeitzone;(Uhrzeit) bis;Zeitzone;ID AEP in €/MWh"] + \
                    [f"{x:%d.%m.%Y};{x:%H:%M};UTC;{x+pd.Timedelta('15min'):%H:%M};UTC;1.234,56" for x in t]
            return R(200, "\n".join(lines))
        return R(204)
    if "regelleistung" in url: return R(204)
    raise AssertionError(url)
dw.requests.get = fake_get
dw.requests.post = lambda url, data=None, timeout=None: R(200, js={"access_token": "t", "expires_in": 3600})
class EPTR2Mock:
    def call(self, key, start_date, end_date):
        t = pd.date_range(start_date, pd.Timestamp(end_date) + pd.Timedelta(hours=23), freq="1h", tz="Europe/Istanbul")
        d = pd.DataFrame({"date": [x.isoformat() for x in t]})
        if key == "mcp": d["price"] = 3000.0; d["priceUsd"] = 100.0; d["priceEur"] = 90.0; d["hour"] = "00:00"
        if key == "smp": d["systemMarginalPrice"] = 3300.0; d["smpDirection"] = "UP"
        if key == "wap": d["wap"] = 3100.0
        if key == "rt-cons": d["consumption"] = 40000.0
        if key == "anc-pfk": d["price"] = 300.0; d["hour"] = "00:00"
        if key == "anc-sfk": d["price"] = 600.0
        if key == "anc-pf-qty": d["amount"] = 500.0
        if key == "anc-sf-qty": d["amount"] = 900.0
        if key in ("bpm-up", "bpm-down"): d["net"] = 1200.0; d["upRegulationZeroCoded"] = 5.0
        if key == "imb-qty": d["positiveImbalance"] = 100.0; d["negativeImbalance"] = -80.0
        if key == "ren-unit-cost":
            d = pd.DataFrame({"period": [pd.Timestamp(start_date, tz="Europe/Istanbul").isoformat()], "unitCost": [250.0]})
        if key == "rt-gen":
            for k, v in [("total", 40000), ("naturalGas", 10000), ("lng", 1000), ("lignite", 8000), ("importCoal", 6000),
                         ("wind", 5000), ("sun", 3000), ("dammedHydro", 4000), ("river", 2000), ("importExport", 500), ("newThing", 7)]:
                d[k] = float(v)
        return d
dw.epias_client = lambda: EPTR2Mock()
class Mock:
    def query_day_ahead_prices(self, area, start, end):
        return pd.Series(50.0, index=pd.date_range(start, end, freq="60min", inclusive="left"))
    def __getattr__(self, n):
        def f(*a, **k): raise NoMatchingDataError
        return f
MOCK = Mock()
dw.get_client = lambda: MOCK
dw.get_api_key = lambda: "x"
dw.NTP_CLIENT_ID = dw.NTP_CLIENT_SECRET = "x"; dw.EPIAS_USERNAME = dw.EPIAS_PASSWORD = "x"
dw.USE_GOOGLE_DRIVE = False
for L in ["LIMITER", "NTP_LIMITER", "ELIA_LIMITER", "RL_LIMITER", "WX_LIMITER", "EPIAS_LIMITER"]: setattr(dw, L, dw.RateLimiter(10**6))
dw.time.sleep = lambda x: None
dw.VOLATILE_DAYS = 0
out = {}
PER = {"TR": ("2015-01-01", "2015-03-01"), "DE": ("2024-03-01", "2024-05-01"), "SE_3": ("2015-01-01", "2015-03-01")}
for c in ["TR", "DE", "SE_3"]:
    dw.configure(c)
    a, b = PER[c]
    dw.START = pd.Timestamp(a, tz=dw.TZ); dw.END = pd.Timestamp(b, tz=dw.TZ)
    out[c] = dw.main()
print("\n#### CHECKS", CALLS)
tr, trh = out["TR"]
print("TR tz:", tr.index.tz, "| rows:", len(tr), "| P_DA:", tr["P_DA"].dropna().unique()[:2], "| P_DA_TRY:", tr["P_DA_TRY"].dropna().unique()[:1])
print("TR P_SMF (3300*90/3000=99):", tr["P_SMF"].dropna().unique()[:2], "| P_Imbalance:", tr["P_Imbalance"].dropna().unique()[:1])
print("TR Gen cols:", sorted(c for c in tr.columns if c.startswith("Gen_")))
print("TR Gas (10000+1000):", tr["Gen_Fossil_Gas"].dropna().unique()[:1], "| Load:", tr["Load_Actual"].dropna().unique()[:1])
print("TR LCA CO2:", round(tr["CO2_Intensity_LCA_AR5_g_per_kWh"].dropna().iloc[0], 1), "| direct:", round(tr["CO2_Intensity_approx_g_per_kWh"].dropna().iloc[0], 1))
print("TR hour of first row (local):", tr.index[0], tr["Hour"].iloc[0], "| holiday col:", tr["Is_Public_Holiday"].sum())
print("TR weather temp (weighted):", round(tr["WX_Temp_C"].dropna().iloc[0], 2))
es = pd.read_parquet("Quarterwatt/data/zones/TR_Turkey/output/raw_combined/Eurostat_Prices.parquet")
print("TR Eurostat cols:", list(es.columns), "rows", len(es), list(es.index[:2]))
de, deh = out["DE"]
print("DE P_ID_AEP:", de["P_ID_AEP"].dropna().unique()[:2], "NaN", de["P_ID_AEP"].isna().sum())
f = pd.read_parquet("Quarterwatt/data/zones/DE_Germany/output/raw_combined/FRED_Fuels.parquet")
print("FRED cols:", list(f.columns), "| gas EUR/MWh (10/1.1*3.412=31.02):", round(f["Gas_EU_EUR_MWh"].iloc[0], 2), "| first:", f.index[0])
esc = pd.read_parquet("Quarterwatt/data/zones/TR_Turkey/output/raw_combined/Eurostat_Components.parquet")
print("DE Eurostat comps:", list(esc.columns), len(esc))
se, _ = out["SE_3"]
print("SE_3 weather:", round(se["WX_Temp_C"].dropna().iloc[0], 2), "| Eurostat geo SE rows:", len(pd.read_parquet("Quarterwatt/data/zones/SE_3_Sweden/output/raw_combined/Eurostat_Prices.parquet")))

tr = out["TR"][0]
print("TR reserve cols:", [c for c in tr.columns if c.startswith("TR_")])
print("TR FCR price EUR (300*0.03=9):", tr["TR_FCR_Capacity_Price_EUR"].dropna().unique()[:1])
print("YEKDEM raw:", pd.read_parquet("Quarterwatt/data/zones/TR_Turkey/output/raw_combined/EPIAS_YEKDEM_Unit_Cost.parquet").to_dict())
print("STATUS:"); st = dw.status(["TR", "DE", "SE_3", "FI"])
print("\n#### OFFLINE REBUILD")
import json as _j
mp = "Quarterwatt/data/zones/DE_Germany/output/manifest.json"
m = _j.load(open(mp)); m["failures"] = [{"dataset": "X", "segment": "s", "errors": []}]; _j.dump(m, open(mp, "w"))
CALLS0 = dict(CALLS)
dw.OFFLINE = True; dw.configure("DE"); dw.START = pd.Timestamp("2024-03-01", tz=dw.TZ); dw.END = pd.Timestamp("2024-05-01", tz=dw.TZ)
dw.get_client = lambda: (_ for _ in ()).throw(AssertionError("no client offline"))
de2, _ = dw.main(); dw.OFFLINE = False
print("calls unchanged:", CALLS == CALLS0, "| rows equal:", len(de2) == len(de), "| failures kept:", _j.load(open(mp))["failures"])
