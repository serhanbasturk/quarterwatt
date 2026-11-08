exec(open("harness.py").read())
import glob, json, requests
from entsoe import EntsoePandasClient
idx_all = pd.period_range("2010-01", "2026-12", freq="M")
dw._ECB_CACHE["CZK"] = pd.Series(25.0, index=idx_all); dw._ECB_CACHE["RON"] = pd.Series(5.0, index=idx_all)

def ts_xml(direction, cur, price, s, e, bt="A96"):
    n = int((e - s) / pd.Timedelta("15min"))
    pts = "".join(f"<Point><position>{i+1}</position><activation_Price.amount>{price}</activation_Price.amount></Point>" for i in range(n))
    return (f"<TimeSeries><businessType>{bt}</businessType><flowDirection.direction>{direction}</flowDirection.direction>"
            f"<currency_Unit.name>{cur}</currency_Unit.name><Period><timeInterval><start>{s.tz_convert('UTC'):%Y-%m-%dT%H:%MZ}</start>"
            f"<end>{e.tz_convert('UTC'):%Y-%m-%dT%H:%MZ}</end></timeInterval><resolution>PT15M</resolution>{pts}</Period></TimeSeries>")
# X1 parser: currency per series, minority currency dropped
s0 = pd.Timestamp("2024-03-01", tz="Europe/Berlin"); e0 = s0 + pd.Timedelta("2h")
xml = "<Doc>" + ts_xml("A01", "RON", 650, s0, e0) + ts_xml("A02", "RON", 0.1, s0, e0) + ts_xml("A01", "RON", 750, s0, e0) + ts_xml("A01", "EUR", 99, s0, e0) + "</Doc>"
f = dw._bal_price_frame(xml.encode(), "aFRR")
print("X1 cols:", list(f.columns), "| up:", f["P_BalE_aFRR_Up"].iloc[0], "| cur:", f["Cur_BalE_aFRR"].iloc[0], "| rows:", len(f))

CALLS = {"a84": 0, "np": 0}
class RC(EntsoePandasClient):
    def __init__(self): super().__init__(api_key="x")
    def _base_request(self, params, start, end):
        if params.get("documentType") == "A84":
            CALLS["a84"] += 1
            cur = "CZK" if params["controlArea_Domain"] == "10YCZ-CEPS-----N" else "EUR"
            p = 2000 if cur == "CZK" else 80
            body = "<Doc>" + ts_xml("A01", cur, p, start, end, params["businessType"]) + ts_xml("A02", cur, p / 4, start, end, params["businessType"]) + "</Doc>"
            return types.SimpleNamespace(content=body.encode(), text=body)
        raise NoMatchingDataError
    def query_day_ahead_prices(self, area, start, end, **k):
        return pd.Series(50.0, index=pd.date_range(start, end, freq="60min", inclusive="left"))
    def query_net_position(self, area, start, end, dayahead=True):
        CALLS["np"] += 1
        r = requests.Response(); r.status_code = 400
        r._content = b"<Acknowledgement_MarketDocument><Reason><text>The combination is not valid</text></Reason></Acknowledgement_MarketDocument>"
        raise requests.HTTPError("400 Client Error: Bad Request", response=r)
    def __getattr__(self, n):
        if n.startswith("query_"):
            def f(*a, **k): raise NoMatchingDataError
            return f
        raise AttributeError(n)
import types
RCI = RC(); dw.get_client = lambda: RCI
def zone(c, start="2024-03-01", end="2024-05-01"):
    dw.configure(c); dw.START = pd.Timestamp(start, tz=dw.TZ); dw.END = pd.Timestamp(end, tz=dw.TZ)
    return dw.main()
# X2 CZ end-to-end: CZK from the XML -> EUR, no Cur column in the outputs
m, h = zone("CZ")
man = json.load(open("Quarterwatt/data/zones/CZ_Czechia/output/manifest.json"))
conv = [x for x in man["currency_conversions"] if x["column"].startswith("P_BalE")]
print("X2 CZ up:", h["P_BalE_aFRR_Up"].median(), "| down:", h["P_BalE_mFRR_Down"].median(),
      "| Cur cols:", [c for c in m.columns if c.startswith("Cur_")], "| how:", sorted({x["reference"] for x in conv}))
# X3 Net_Position HTTP 400 in old months -> .rejected, not asked again, reported
rej = glob.glob("Quarterwatt/data/zones/CZ_Czechia/raw/Net_Position/*.rejected")
print("X3 rejected files:", len(rej), "| reason kept:", "not valid" in open(rej[0]).read(),
      "| failures:", len(man["failures"]), "| manifest:", len(man["rejected_by_source"]))
n0 = CALLS["np"]; zone("CZ"); print("X3 second run Net_Position calls:", CALLS["np"] - n0)
# X4 migration: CZ raw file without currency column is fetched once more
fold = "Quarterwatt/data/zones/CZ_Czechia/raw/BalE_Price_aFRR"
f0 = sorted(glob.glob(f"{fold}/2024-03-01_*.parquet"))[0]
old = pd.read_parquet(f0).drop(columns=["Cur_BalE_aFRR"]); old["P_BalE_aFRR_Up"] = 2000.0; old.to_parquet(f0)
a0 = CALLS["a84"]; zone("CZ")
import pyarrow.parquet as pq
print("X4 legacy refetched:", CALLS["a84"] - a0 == 1, "| has Cur now:", "Cur_BalE_aFRR" in pq.read_schema(f0).names)
a0 = CALLS["a84"]; zone("CZ"); print("X4 third run calls:", CALLS["a84"] - a0)
# X5 RO legacy files (OFFLINE rebuild) -> checked fallback RON
dw.OFFLINE = False
mr, hr = zone("RO")
for fp in glob.glob("Quarterwatt/data/zones/RO_Romania/raw/BalE_Price_*/*.parquet"):
    d = pd.read_parquet(fp); d = d[[c for c in d.columns if not c.startswith("Cur_")]] * 5.0; d.to_parquet(fp)
dw.OFFLINE = True; mr, hr = zone("RO"); dw.OFFLINE = False
print("X5 RO legacy up (400 RON/5):", hr["P_BalE_aFRR_Up"].median())
# X6 single-price systems: zero P_Imb_Short emptied, real values kept
i = pd.date_range("2024-01-01", "2024-04-01", freq="15min", inclusive="left", tz="Europe/Rome")
d = pd.DataFrame({"P_Imb_Long": 100.0, "P_Imb_Short": 0.0}, index=i); d.loc["2024-03", "P_Imb_Short"] = 120.0
log = {}; d = dw.drop_empty_short_price(d, log)
print("X6 empty months:", sorted(set(d.index[d.P_Imb_Short.isna()].strftime("%Y-%m"))), "| March kept:", d.loc["2024-03", "P_Imb_Short"].median(), "|", log)
# X7 CH has no balancing price dataset, DE has; no volume dataset / probe left
dw.configure("CH"); ch = [x["name"] for x in dw.DATASETS]; dw.configure("DE"); de = [x["name"] for x in dw.DATASETS]
print("X7 CH BalE:", any(n.startswith("BalE") for n in ch), "| DE BalE:", sorted(n for n in de if n.startswith("BalE")),
      "| probe:", hasattr(dw, "probe_bal_volume"), "| volume fn:", hasattr(dw, "make_bal_volume_fn"))
