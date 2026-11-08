"""Contract test: every ENTSO-E call of every dataset in every zone uses a method and
keyword arguments that exist in the installed entsoe-py (no network needed)."""
import inspect, sys, os, collections, tempfile
import pandas as pd, requests
_HERE = os.getcwd(); os.chdir(tempfile.mkdtemp())
sys.path.insert(0, _HERE)
import quarterwatt as qw
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
CALLS = collections.Counter(); BAD = []
class SigClient(EntsoePandasClient):
    def __init__(self): super().__init__(api_key="x")
    def _base_request(self, params, start, end):
        CALLS["raw:" + params.get("documentType", "?")] += 1
        raise NoMatchingDataError
for name, fn in inspect.getmembers(EntsoePandasClient, inspect.isfunction):
    if not name.startswith("query_"):
        continue
    def make(name, fn):
        sig = inspect.signature(fn)
        def w(self, *a, **k):
            try:
                sig.bind(self, *a, **k)
            except TypeError as ex:
                BAD.append((name, str(ex)))
            CALLS[name] += 1
            raise NoMatchingDataError
        return w
    setattr(SigClient, name, make(name, fn))
def offline(*a, **k): raise requests.ConnectionError("offline test")
qw.requests.get = qw.requests.post = offline
qw.time.sleep = lambda s: None
qw.MAX_TRIES = 1
c = SigClient()
problems = []
cover = {}
for zone in qw.PROFILES:
    qw.configure(zone)
    cover[zone] = [d["name"] for d in qw.DATASETS]
    for month in ("2016-03-01", "2024-03-01", "2026-03-01"):
        s = pd.Timestamp(month, tz=qw.TZ); e = s + pd.DateOffset(months=1)
        for d in qw.DATASETS:
            try:
                areas = list(d["areas"](s))
            except Exception as ex:
                problems.append((zone, d["name"], "areas", repr(ex))); continue
            for a in areas:
                try:
                    d["fn"](c, a, s, e)
                except (NoMatchingDataError, requests.ConnectionError, qw.FetchError):
                    pass
                except Exception as ex:
                    if type(ex).__name__ in ("TypeError", "AttributeError", "NameError", "KeyError",
                                             "UnboundLocalError", "ValueError", "IndexError"):
                        problems.append((zone, d["name"], a if isinstance(a, str) else str(a), f"{type(ex).__name__}: {ex}"))
print("zones:", len(cover), "| ENTSO-E methods used:", len([k for k in CALLS if not k.startswith("raw:")]),
      "| raw document types:", sorted(k for k in CALLS if k.startswith("raw:")))
print("signature errors:", BAD[:5], len(BAD))
uniq = sorted({(p[1], p[3][:120]) for p in problems})
print("other errors:", len(problems)); [print("  ", u) for u in uniq[:30]]
