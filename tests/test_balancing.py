exec(open("harness.py").read())
idx_all = pd.period_range("2010-01", "2026-12", freq="M")
dw._ECB_CACHE["HUF"] = pd.Series(380.0, index=idx_all)
class M3(Mock):
    def query_day_ahead_prices(self, area, start, end):
        return pd.Series(50.0, index=pd.date_range(start, end, freq="60min", inclusive="left"))
    def query_activated_balancing_energy_prices(self, area, start, end, business_type, **k):
        i = pd.date_range(start, end, freq="15min", inclusive="left")
        f = 380.0 if "HU" in str(area) or "10YHU" in str(area) else 1.0
        up = pd.DataFrame({"Price": 80.0 * f, "Direction": "Up", "ReserveType": "x"}, index=i)
        dn = pd.DataFrame({"Price": 20.0 * f, "Direction": "Down", "ReserveType": "x"}, index=i)
        return pd.concat([up, dn]).sort_index()
    def query_activated_balancing_energy(self, area, start, end, business_type, **k):
        i = pd.date_range(start, end, freq="15min", inclusive="left")
        return pd.DataFrame({("X", "Up"): 30.0, ("X", "Down"): 10.0}, index=i)
M = M3(); dw.get_client = lambda: M
out = {}
for c in ["DE", "HU"]:
    dw.configure(c); dw.START = pd.Timestamp("2024-03-01", tz=dw.TZ); dw.END = pd.Timestamp("2024-05-01", tz=dw.TZ)
    out[c] = dw.main()[1]
for c, h in out.items():
    cols = sorted(x for x in h.columns if "BalE" in x)
    print(f"E {c} cols:", cols, "| up price:", round(h["P_BalE_aFRR_Up"].median(), 2))
man = json.load(open("Quarterwatt/data/zones/HU_Hungary/output/manifest.json"))
print("E HU conversions:", sorted({x["column"] for x in man.get("currency_conversions", [])}))
