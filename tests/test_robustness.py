"""Robustness: rebuilds, key masking, resume, ordering, API bans, parser, weather."""
exec(open("harness.py").read())
import glob, json, types, requests
from entsoe import EntsoePandasClient
# R1 offline rebuild on a later day keeps the open month (file key ends at the download day)
dw.configure("FR"); dw.START = pd.Timestamp("2026-09-01", tz=dw.TZ); dw.END = pd.Timestamp("2026-11-07", tz=dw.TZ)
m, h = dw.main()
def nov_hours(end):
    dw.OFFLINE = True; dw.configure("FR"); dw.START = pd.Timestamp("2026-09-01", tz=dw.TZ); dw.END = pd.Timestamp(end, tz=dw.TZ)
    mm, hh = dw.main(); dw.OFFLINE = False
    return int(hh.P_DA.loc["2026-11"].notna().sum())
print("R1 Nov hours after download / next day / next month:", int(h.P_DA.loc["2026-11"].notna().sum()), nov_hours("2026-11-08"), nov_hours("2026-12-02"))
# R10 two files for one month: the newest is used
fold = "Quarterwatt/data/zones/FR_France/raw/P_DA"
f = sorted(glob.glob(f"{fold}/2026-10-01_2026-11-01__*.parquet"))[0]
d = pd.read_parquet(f); d["P_DA"] = 999.0; old = f.replace("__", "__OLD", 1); d.to_parquet(old); os.utime(old, (1, 1))
d["P_DA"] = 77.0; d.to_parquet(f)
dw.configure("FR"); dw.START = pd.Timestamp("2026-09-01", tz=dw.TZ); dw.END = pd.Timestamp("2026-11-08", tz=dw.TZ)
print("R10 newest file used:", float(dw.load_dataset([x for x in dw.DATASETS if x["name"] == "P_DA"][0])[0].P_DA.loc["2026-10"].median()))
os.remove(old)
# R2 API key never stored: FetchError text and old files are scrubbed
r = requests.Response(); r.status_code = 400; r.url = "https://web-api.tp.entsoe.eu/api?documentType=A84&securityToken=SECRET-KEY-123&periodStart=1"
r._content = b"<Reason><text>bad</text></Reason>"
def boom(c, a, s, e): raise requests.HTTPError(f"400 Client Error: Bad Request for url: {r.url}", response=r)
try:
    dw.call_with_retry(boom, "FR", None, None)
except dw.FetchError as ex:
    print("R2 key in error:", "SECRET-KEY-123" in str(ex), "| masked:", "securityToken=***" in str(ex))
os.makedirs("Quarterwatt/data/zones/XX/output", exist_ok=True)
json.dump({"failures": [{"errors": [f"for url {r.url}"]}]}, open("Quarterwatt/data/zones/XX/output/manifest.json", "w"))
print("R2 files fixed:", dw.scrub_files(), "| key left:", "SECRET-KEY-123" in open("Quarterwatt/data/zones/XX/output/manifest.json").read(),
      "| second pass:", dw.scrub_files())
# R3 zone with failed months is not marked done -> retried at the next run()
class FailSept(Mock):
    def query_day_ahead_prices(self, area, start, end):
        if start.month == 9:
            rr = requests.Response(); rr.status_code = 503; raise requests.HTTPError("503", response=rr)
        return pd.Series(50.0, index=pd.date_range(start, end, freq="60min", inclusive="left"))
dw.get_client = lambda: FailSept(); dw.START_DATE = "2026-08-01"
dw.run(["NL"]); st = dw._run_state()
print("R3 NL marked done despite failure:", "NL" in st)
dw.get_client = lambda: MOCK
# R7 reference zones first (SK before CZ, GR before BG)
order = []
_main = dw.main
dw.main = lambda: (order.append(dw.COUNTRY), (None, None))[1]
dw.run(["BG", "CZ", "SK", "GR"], force=True); dw.main = _main
print("R7 order:", order)
# R9 ENTSO-E 403 during the download (smoke test passed) stops the zone at once
class Ban(Mock):
    n = 0
    def query_day_ahead_prices(self, area, start, end):
        Ban.n += 1
        if Ban.n == 1:
            return pd.Series(50.0, index=pd.date_range(start, end, freq="60min", inclusive="left"))
        rr = requests.Response(); rr.status_code = 403; rr.url = "https://web-api.tp.entsoe.eu/api?x"
        raise requests.HTTPError("403 Forbidden", response=rr)
dw.get_client = lambda: Ban(); dw.START_DATE = "2016-01-01"
dw.run(["AT"], force=True)
print("R9 stopped early:", Ban.n < 300, "| AT marked done:", "AT" in dw._run_state())
dw.get_client = lambda: MOCK
# R4 PL balancing prices without currency information are never passed on as EUR
dw.configure("PL")
i15 = pd.date_range("2024-03-01", periods=8, freq="15min", tz=dw.TZ)
dfp = pd.DataFrame({"P_BalE_aFRR_Up": 400.0, "P_BalE_aFRR_Down": 100.0, "P_DA": 80.0}, index=i15)
log = {}; out = dw.harmonise_currency(dfp, log)
print("R4 PL legacy up:", out["P_BalE_aFRR_Up"].isna().all(), "| logged:", "P_BalE_aFRR_Up" in log.get("currency_unconverted", []))
# R5 tolerant parser: missing point, A03 direction, A03 curve, two periods
def per(start, end, pts):
    return (f"<Period><timeInterval><start>{start}</start><end>{end}</end></timeInterval><resolution>PT15M</resolution>"
            + "".join(f"<Point><position>{p}</position>" + (f"<activation_Price.amount>{v}</activation_Price.amount>" if v is not None else "")
                      + "</Point>" for p, v in pts) + "</Period>")
def tsx(d, periods, curve="A01"):
    return (f"<TimeSeries><businessType>A96</businessType><curveType>{curve}</curveType><flowDirection.direction>{d}</flowDirection.direction>"
            f"<currency_Unit.name>PLN</currency_Unit.name>{periods}</TimeSeries>")
xml = "<Doc>" + tsx("A02", per("2024-03-01T00:00Z", "2024-03-01T01:00Z", [(1, 100), (2, 110), (3, None), (4, 130)])) \
      + tsx("A03", per("2024-03-01T00:00Z", "2024-03-01T01:00Z", [(1, 50)]), curve="A03") \
      + tsx("A01", per("2024-03-01T00:00Z", "2024-03-01T01:00Z", [(1, 1), (2, 2), (3, 3), (4, 4)])
            + per("2024-03-02T00:00Z", "2024-03-02T01:00Z", [(1, 5), (2, 6), (3, 7), (4, 8)])) + "</Doc>"
fr = dw._bal_price_frame(xml, "aFRR")
print("R5 down points:", int(fr["P_BalE_aFRR_Down"].notna().sum()), "| sym (A03 curve, 1 point -> 4):", int(fr["P_BalE_aFRR_Sym"].notna().sum()),
      "| up over two periods:", int(fr["P_BalE_aFRR_Up"].notna().sum()), "| cur:", fr["Cur_BalE_aFRR"].iloc[0])
# R6 weather: no hour lost at local midnight on 1 January (CET and Türkiye)
def fake_wx(url, timeout=None, params=None):
    t = pd.date_range(params["start_date"], pd.Timestamp(params["end_date"]) + pd.Timedelta("23h"), freq="1h", tz="UTC")
    n = len(params["latitude"].split(","))
    loc = {"hourly": {"time": [int(x.timestamp()) for x in t], **{v: [1.0] * len(t) for v in dw.WX_VARS}}}
    return types.SimpleNamespace(raise_for_status=lambda: None, json=lambda: [loc] * n if n > 1 else loc)
_get = dw.requests.get; dw.requests.get = fake_wx
miss = {}
for z in ["TR", "FR"]:
    dw.configure(z); s0 = pd.Timestamp("2025-01-01", tz=dw.TZ); e0 = pd.Timestamp("2025-02-01", tz=dw.TZ)
    w = dw.f_weather(None, None, s0, e0); w = w[(w.index >= s0) & (w.index < e0)]
    miss[z] = len(pd.date_range(s0, e0, freq="1h", inclusive="left").difference(w.index))
dw.requests.get = _get
print("R6 missing weather hours:", miss)
# R11 single-price month with only ~60 % zeros is emptied too; a normal month is kept
i = pd.date_range("2024-01-01", "2024-03-01", freq="15min", inclusive="left", tz="Europe/Rome")
d = pd.DataFrame({"P_Imb_Long": 100.0, "P_Imb_Short": 90.0}, index=i)
jan = d.index.month == 1
d.loc[jan, "P_Imb_Short"] = np.where(np.arange(jan.sum()) % 10 < 6, 0.0, 95.0)
d = dw.drop_empty_short_price(d, {})
print("R11 Jan emptied:", d.loc["2024-01", "P_Imb_Short"].isna().all(), "| Feb kept:", d.loc["2024-02", "P_Imb_Short"].notna().all())
