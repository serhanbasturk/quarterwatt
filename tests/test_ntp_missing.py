exec(open("harness.py").read())
dw.configure("DE"); dw.START = pd.Timestamp("2024-03-01", tz=dw.TZ); dw.END = pd.Timestamp("2024-04-01", tz=dw.TZ)
h1 = dw.main()[1]
dw.NTP_CLIENT_ID = dw.NTP_CLIENT_SECRET = ""
dw.configure("DE"); dw.START = pd.Timestamp("2024-03-01", tz=dw.TZ); dw.END = pd.Timestamp("2024-04-01", tz=dw.TZ)
h2 = dw.main()[1]
print("N kept:", "P_ID_AEP" in h2.columns, h2["P_ID_AEP"].notna().sum() == h1["P_ID_AEP"].notna().sum(), "| failures:", len(dw.FAILURES))
