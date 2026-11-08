import sys, os
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import quarterwatt as sb
sb.USE_GOOGLE_DRIVE = False; sb.configure("PL")
idx = pd.date_range("2016-01-01", "2016-01-05", freq="15min", tz="Europe/Berlin", inclusive="left")
p = pd.Series(np.repeat(np.arange(len(idx)//4, dtype=float), 4), index=idx)
p[(idx.hour >= 0) & (idx.hour <= 4)] = np.nan          # omitted night points
p["2016-01-03"] = np.nan                               # full missing day
p["2016-01-04 20:00":] = np.nan                        # series end
df = sb.fill_a03_price_gaps(pd.DataFrame({"P_DA": p}))
h = df["P_DA"].resample("1h").mean()
print("Jan 1 00-05 (no previous value that day -> NaN except 0h? ):", h["2016-01-01 00:00":"2016-01-01 05:00"].tolist())
print("Jan 2 00-05 (CET day starts 00:00, previous = none) :", h["2016-01-02 00:00":"2016-01-02 05:00"].tolist())
print("Jan 3 full NaN stays:", h["2016-01-03"].isna().all(), "| end stays NaN:", h["2016-01-04 20:00":].isna().all())
