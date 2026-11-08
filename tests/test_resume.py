exec(open("harness.py").read())
import glob, json
N = {"n": 0}
class CM(Mock):
    def query_day_ahead_prices(self, area, start, end):
        N["n"] += 1
        return pd.Series(50.0, index=pd.date_range(start, end, freq="60min", inclusive="left"))
    def __getattr__(self, n):
        def f(*a, **k):
            N["n"] += 1; raise NoMatchingDataError
        return f
M = CM(); dw.get_client = lambda: M
dw.START_DATE = "2026-08-01"; dw.VERBOSE = False; dw.VOLATILE_DAYS = 60; dw.USE_ELIA = False   # no unmocked source -> no failures
def calls(fn):
    N["n"] = 0; fn(); return N["n"]
c1 = calls(lambda: dw.run(["BE", "NL"]))
st = json.load(open("Quarterwatt/audit/run_state.json"))
print("T1 first run calls > 50:", c1 > 50, "| state:", sorted(st), st["BE"]["code_version"] == dw.CODE_VERSION)
c2 = calls(lambda: dw.run(["BE", "NL"]))
print("T2 second run calls:", c2)
del st["NL"]; json.dump(st, open("Quarterwatt/audit/run_state.json", "w"))      # NL interrupted
c3 = calls(lambda: dw.run(["BE", "NL"]))
print("T3 resume NL calls (smoke test only):", c3, "| NL back in state:", "NL" in json.load(open("Quarterwatt/audit/run_state.json")))
print("T4 .empty for recent months:", len(glob.glob("Quarterwatt/data/zones/*/raw/*/2026-1*.empty")),
      "| .checked:", len(glob.glob("Quarterwatt/data/zones/*/raw/*/*.checked")) > 0)
c5 = calls(lambda: dw.run(["BE"], force=True))
print("T5 force refetches (smoke only, files fresh):", c5)
dw.RESUME_HOURS = 0
c6 = calls(lambda: dw.run(["BE"], force=True))
print("T6 stale -> full refetch:", c6 > 20, c6, c1)
df = dw.load_zone("BE"); print("T7 data kept:", df["P_DA"].notna().sum() > 0)
dw.OFFLINE = True
c8 = calls(lambda: dw.rebuild(["BE"])); print("T8 rebuild ignores state, no calls:", c8)
