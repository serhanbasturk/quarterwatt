exec(open("harness.py").read())
import warnings
TZ = "Europe/Berlin"
PLN_A, PLN_B = pd.Timestamp("2017-03-01", tz=TZ), pd.Timestamp("2017-06-01", tz=TZ)
def price(area, start, end):
    idx = pd.date_range(start, end, freq="60min", inclusive="left")
    base = 40 + 10*np.sin(np.asarray(idx.hour)/24*2*np.pi)
    s = pd.Series(base, index=idx)
    if "PL" in str(area):
        s = s * 1.1
        s[(idx >= PLN_A) & (idx < PLN_B)] *= 4.3                 # PLN period
        s = s[~((np.asarray(idx.hour) >= 1) & (np.asarray(idx.hour) <= 4) & (np.asarray(idx.day) % 2 == 0))]  # A03 holes
    return s
def imb(area, start, end):
    idx = pd.date_range(start, end, freq="15min", inclusive="left")
    v = 45 + 30*np.sin(np.asarray(idx.hour)/24*2*np.pi)
    if "PL" in str(area): v = v * 4.3                              # PLN always
    return pd.DataFrame({"Long": v, "Short": v + 5}, index=idx)
class M2(Mock):
    def query_day_ahead_prices(self, area, start, end): return price(area, start, end)
    def query_imbalance_prices(self, area, start, end, **k): return imb(area, start, end)
    def query_load(self, area, start, end, **k):
        idx = pd.date_range(start, end, freq="15min", inclusive="left")
        return pd.DataFrame({"Actual Load": 20000.0}, index=idx)
M = M2(); dw.get_client = lambda: M
# ECB down, frankfurter down, FRED up?  -> first: all external FX fail except cache
idx_all = pd.period_range("2010-01", "2026-12", freq="M")
dw._ECB_CACHE["PLN"] = pd.Series(4.3, index=idx_all)
warnings.simplefilter("error", FutureWarning)
res = {}
for c in ["DE", "PL"]:
    dw.configure(c); dw.START = pd.Timestamp("2017-01-01", tz=dw.TZ); dw.END = pd.Timestamp("2017-08-01", tz=dw.TZ)
    res[c] = dw.main()
pl = res["PL"][1]; de = res["DE"][1]
mm = lambda s: s.resample("ME").median().round(1).tolist()
print("PL P_DA monthly median :", mm(pl["P_DA"]))
print("PL P_DA_DE              :", mm(pl["P_DA_DE"]))
print("PL imbalance (Long)     :", mm(pl["P_Imb_Long"]), "| P_Imbalance:", mm(pl["P_Imbalance"])[:3])
print("PL coverage P_DA        :", round(pl["P_DA"].notna().mean(), 3), "| flagged filled share:", round(pl["Flag_P_DA_Filled"].mean(), 3))
print("DE P_DA_PL monthly      :", mm(de["P_DA_PL"]))
man = json.load(open("Quarterwatt/data/zones/PL_Poland/output/manifest.json"))
print("manifest conversions    :", [(x["column"], x["currency"], x["from"], x["to"]) for x in man.get("currency_conversions", [])])
print("unconverted             :", man.get("currency_unconverted"))
# offline rebuild must give identical results
dw.OFFLINE = True; dw.configure("PL"); dw.START = pd.Timestamp("2017-01-01", tz=dw.TZ); dw.END = pd.Timestamp("2017-08-01", tz=dw.TZ)
pl2 = dw.main()[1]; dw.OFFLINE = False
print("rebuild identical       :", np.allclose(pl2["P_DA"].fillna(-1), pl["P_DA"].fillna(-1)), np.allclose(pl2["P_Imb_Long"].fillna(-1), pl["P_Imb_Long"].fillna(-1)))
