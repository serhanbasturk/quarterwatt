import os, glob
import quarterwatt as qw
qw.USE_GOOGLE_DRIVE = False
z = "Quarterwatt/data/zones/DE_Germany"
for d in ["raw/P_DA", "raw/Weather", "raw/Net_Position_old", "output"]: os.makedirs(f"{z}/{d}", exist_ok=True)
open(f"{z}/raw/P_DA/2016-01.parquet", "w").write("a"); open(f"{z}/raw/P_DA/2016-01.parquet.bak", "w").write("old")
open(f"{z}/raw/P_DA/2016-02.parquet.bak", "w").write("only-copy")
open(f"{z}/raw/Weather/2016.parquet", "w").write("w"); open(f"{z}/raw/Net_Position_old/x.parquet", "w").write("x")
open(f"{z}/diagnose_output.txt", "w").write("d"); open(f"{z}/output/Quarterwatt_DE_1h.parquet", "w").write("o")
os.makedirs("Quarterwatt/archive/SB_Energy_Code"); open("Quarterwatt/archive/SB_Energy_Code/x.py", "w").write("#")
os.makedirs("Quarterwatt/analysis/wp1"); open("Quarterwatt/analysis/wp1/key_numbers.txt", "w").write("k")
os.makedirs("Quarterwatt/audit"); open("Quarterwatt/audit/audit_summary.csv", "w").write("a")
os.makedirs("Quarterwatt/data/zones/XX_Nowhere")
qw.USE_WEATHER = False                    # switched off must NOT mean "delete"
before = sorted(glob.glob("Quarterwatt/**", recursive=True))
plan = qw.clean_storage()
print("PLAN:", sorted(os.path.relpath(p, "Quarterwatt") for p, _ in plan))
print("UNCHANGED BEFORE DELETE:", before == sorted(glob.glob("Quarterwatt/**", recursive=True)))
qw.clean_storage(delete=True)
print("AFTER: weather kept", os.path.exists(f"{z}/raw/Weather/2016.parquet"), "| P_DA kept", os.path.exists(f"{z}/raw/P_DA/2016-01.parquet"),
      "| restored", open(f"{z}/raw/P_DA/2016-02.parquet").read() == "only-copy", "| own folders kept", os.path.exists("Quarterwatt/archive/SB_Energy_Code/x.py") and os.path.exists("Quarterwatt/analysis/wp1/key_numbers.txt"),
      "| audit kept", os.path.exists("Quarterwatt/audit/audit_summary.csv"), "| bak removed", not os.path.exists(f"{z}/raw/P_DA/2016-01.parquet.bak"))
print("SWITCH RESTORED:", qw.USE_WEATHER)
