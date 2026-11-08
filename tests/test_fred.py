import sys, os
sys.path.insert(0, os.getcwd())
import pandas as pd, requests
import quarterwatt as sb
sb.USE_GOOGLE_DRIVE = False; sb.time.sleep = lambda x: None
class R:
    def __init__(s, code, text=""): s.status_code, s.text = code, text
    def raise_for_status(s):
        if s.status_code >= 400: raise requests.HTTPError(str(s.status_code))
def fake(url, timeout=None, headers=None, **k):
    if "fred" in url:
        sid = url.split("id=")[1]
        if sid == "EXUSEU": return R(200, "observation_date,EXUSEU\n2017-03-01,1.07\n2017-04-01,1.10\n")
        if sid == "CCUSMA02PLM618N": return R(200, "observation_date,CCUSMA02PLM618N\n2017-03-01,4.0\n2017-04-01,3.9\n")
        return R(404)
    return R(500)
sb.requests.get = fake
r = sb.ecb_rates("PLN"); print(r.loc["2017-03"], r.loc["2017-04"], r.index.max())
sb._ECB_CACHE.clear(); print("RON:", sb.ecb_rates("RON"))
