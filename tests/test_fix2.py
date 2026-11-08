import sys, os, json
sys.path.insert(0, os.getcwd())
exec(open("test_cur.py").read())
print("---- RO negative kept:", (ro["P_DA"] < 0).sum() > 0)
# CZ forced imbalance
sb.configure("CZ"); cz = table("CZ", imb_factor=25.0); sb._ECB_CACHE["CZK"] = pd.Series(25.0, index=idx_all)
lg = {}; cz = sb.harmonise_currency(cz, lg); print("CZ imb/DA level:", round(cz["P_Imb_Long"].abs().median() / cz["P_DA"].median(), 2))
# price bounds
sb.configure("BG"); b = table("BG"); b.iloc[1000, 0] = 3120.0; b.loc["2023-01-01 10:00", "P_DA"] = 4500.0; b.iloc[2000, 0] = -600
lg = {}; b = sb.apply_price_bounds(b, lg); print(lg, "| 4500 in 2023 kept:", (b["P_DA"] == 4500).sum())
# odd generation columns
g = pd.DataFrame({"Gen_Fossil_Gas": [1.0, np.nan], "Gen_'Fossil_Gas'_'Actual_Aggregated'": [np.nan, 2.0],
                  "Gen_'Hydro_Pumped_Storage'_'Actual_Consumption'": [np.nan, 3.0], "Gen_'Biomass'_'Actual_Aggregated'": [np.nan, 4.0]})
print(sb.master_columns(g).to_dict("list"))
# tuple columns from entsoe
class C:
    def query_generation(self, area, start, end):
        return pd.DataFrame({("Biomass", "Actual Aggregated"): [1.0], ("Hydro Pumped Storage", "Actual Consumption"): [2.0], "Solar": [3.0]})
print(list(sb.f_generation(C(), "IT", None, None).columns))
# IT CO2 with Other
sb.configure("IT"); x = pd.DataFrame({"Gen_Other": [100.0], "Gen_Solar": [100.0]}, index=pd.date_range("2016-01-01", periods=1, freq="h", tz=sb.TZ))
x = sb.add_derived(x); print("IT CO2 LCA (490*100+48*100)/200 =", x["CO2_Intensity_LCA_AR5_g_per_kWh"].iloc[0])
sb.configure("DE"); y = sb.add_derived(pd.DataFrame({"Gen_Other": [100.0], "Gen_Solar": [100.0]}, index=x.index)); print("DE unchanged:", y["CO2_Intensity_LCA_AR5_g_per_kWh"].iloc[0])
# ntp fallback on 400
calls = []
class Rr:
    def __init__(s, c): s.status_code = c
def fg(url, headers=None, timeout=None, params=None):
    calls.append((url, params)); return Rr(400 if params is None else 200)
sb.requests.get = fg; sb.ntp_token = lambda: "t"
print("ntp:", sb.ntp_get("IdAep", "a", "b").status_code, calls)
