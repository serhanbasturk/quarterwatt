exec(open("harness.py").read())
import re
import warnings
qw = dw
# K1 secrets: blanks / quotes stripped
os.environ["QW_T1"] = '  "abc-123" \n'; os.environ["QW_T2"] = "'xyz'"
print("K1 secrets:", repr(qw._secret("QW_T1")), repr(qw._secret("QW_T2")), repr(qw._secret("QW_NONE")))
# K2 balancing volumes off by default, prices still on
qw.configure("DE"); names = [d["name"] for d in qw.DATASETS]
print("K2 volume registered:", any(n.startswith("BalE_Volume") for n in names), "| price registered:", "BalE_Price_aFRR" in names)
# K3 Braeuer 2016 layout: interval END, no DST, full leap year
raw = "Quarterwatt/data/reference/load_profiles/braeuer2020_raw"; os.makedirs(raw, exist_ok=True)
t = pd.date_range("2016-01-01 00:15", periods=35136, freq="15min")
pd.DataFrame({"Time": t.strftime("%d.%m.%Y %H:%M"), "IP1": np.arange(35136) % 96 * 1.0, "IP2": "3,5"}).to_csv(
    f"{raw}/LoadProfile_20IPs_2016.csv", sep=";", decimal=",", index=False)
# K4 2017 layout: local time WITH DST (spring hour missing, autumn hour twice), interval start, mixed types
loc = pd.date_range("2017-01-01", "2017-12-31 23:45", freq="15min", tz="Europe/Berlin")
p1 = pd.Series(1.0, index=range(len(loc))).astype(object); p1.iloc[5] = "x"
pd.DataFrame({"Date": loc.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"), "P1": p1, "P2": 2.0}).to_csv(
    f"{raw}/LoadProfile_30IPs_2017.csv", index=False)
with warnings.catch_warnings():
    warnings.simplefilter("error")
    a = qw._parse_load_csv(f"{raw}/LoadProfile_20IPs_2016.csv")
    b = qw._parse_load_csv(f"{raw}/LoadProfile_30IPs_2017.csv")
print("K3 2016 first:", a.timestamp.min(), "| last:", a.timestamp.max(), "| n per plant:", a.groupby("plant").size().tolist())
u = a[a.plant == "IP1"].sort_values("timestamp")
print("K3 utc steps:", u.timestamp.dt.tz_convert("UTC").diff().dropna().unique().tolist(),
      "| midnight value:", u.set_index("timestamp").kW.loc["2016-01-02 00:00"].item(), "| IP2:", a[a.plant == "IP2"].kW.iloc[0])
print("summer 00:00 local = row of 23:00 UTC+1:", u.set_index("timestamp").kW.loc["2016-06-01 00:00"].item())
print("K4 2017 n:", b.groupby("plant").size().to_dict(), "| unique:", not b.duplicated(["plant", "timestamp"]).any(),
      "| tz:", b.timestamp.dt.tz, "| utc steps:", b[b.plant == "P2"].sort_values("timestamp").timestamp.dt.tz_convert("UTC").diff().dropna().unique().tolist())
# K5 an older table with naive time stamps is rebuilt automatically
lp = "Quarterwatt/data/reference/load_profiles"
pd.DataFrame({"timestamp": pd.date_range("2016-01-01", periods=3, freq="15min"), "plant": "x", "kW": 1.0}).to_parquet(f"{lp}/braeuer2020.parquet")
pd.DataFrame({"x": [1]}).to_parquet(f"{lp}/bdew_slp.parquet")
qw.update_reference()
br = qw.load_reference("braeuer")
print("K5 rebuilt:", br.timestamp.dt.tz is not None, br.plant.nunique())
print("K7 version:", qw.CODE_VERSION, "| no credentials:", not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", open(qw.__file__).read()))
