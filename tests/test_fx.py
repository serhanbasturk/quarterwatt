import sys, os
sys.path.insert(0, os.getcwd())
import pandas as pd, requests
import quarterwatt as sb
sb.USE_GOOGLE_DRIVE = False; sb.time.sleep = lambda x: None
class R:
    def __init__(s, code, js=None, text=""): s.status_code, s._js, s.text = code, js, text
    def json(s): return s._js
    def raise_for_status(s):
        if s.status_code >= 400: raise requests.HTTPError(str(s.status_code))
mode = {"fr": True}
def fake(url, timeout=None, **k):
    if "ecb" in url: return R(500)
    if "frankfurter" in url and mode["fr"]:
        return R(200, js={"rates": {"2017-03-01": {"PLN": 4.3}, "2017-03-15": {"PLN": 4.5}, "2019-01-02": {"PLN": 4.28}}})
    return R(503)
sb.requests.get = fake
r = sb.ecb_rates("PLN"); print("frankfurter:", r["2017-03"], len(r), os.path.exists("Quarterwatt/fx_rates/EUR_PLN_monthly.csv"))
sb._ECB_CACHE.clear(); mode["fr"] = False
r = sb.ecb_rates("PLN"); print("saved copy:", r["2017-03"])
sb._ECB_CACHE.clear(); print("none:", sb.ecb_rates("CZK"))
import numpy as np
sb.configure("PL"); idx = pd.date_range("2018-01-01", periods=2000, freq="1h", tz=sb.TZ)
df = pd.DataFrame({"P_DA": 50.0, "P_Imb_Long": 200.0}, index=idx)
sb.FORCED_IMBALANCE_CURRENCY["PL"] = ("CZK", None); lg = {}
df = sb.harmonise_currency(df, lg); print(lg, "values kept:", df["P_Imb_Long"].iloc[0])
