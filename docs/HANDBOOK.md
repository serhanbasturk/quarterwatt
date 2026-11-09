# Quarterwatt Handbook

*For students, thesis writers and researchers who work with Quarterwatt data. Version 1.2 (November 2026).*

---

## Contents

1. [What Quarterwatt is, and what it is not](#1-what-quarterwatt-is-and-what-it-is-not)
2. [Accounts and API keys](#2-accounts-and-api-keys)
3. [Installation and first run](#3-installation-and-first-run)
4. [The monthly workflow](#4-the-monthly-workflow)
5. [Where the data are: folder layout](#5-where-the-data-are-folder-layout)
6. [Data model: time, units, currencies](#6-data-model-time-units-currencies)
7. [Variable catalogue](#7-variable-catalogue)
8. [Data quality: cleaning rules, audit and known limits](#8-data-quality-cleaning-rules-audit-and-known-limits)
9. [Working with the data: recipes](#9-working-with-the-data-recipes)
10. [Reference data](#10-reference-data)
11. [Licences and citation](#11-licences-and-citation)
12. [Troubleshooting](#12-troubleshooting)
13. [Guidance for theses](#13-guidance-for-theses)
14. [For developers: how the code works and how to extend it](#14-for-developers-how-the-code-works-and-how-to-extend-it)
15. [Function reference](#15-function-reference)

---

## 1. What Quarterwatt is, and what it is not

Quarterwatt is **one Python file** (`quarterwatt.py`). It builds and maintains your own copy of European electricity-market data:

* **Sources:**
  * ENTSO-E Transparency Platform (all European zones)
  * netztransparenz.de and regelleistung.net (Germany)
  * Elia Open Data (Belgium)
  * EPİAŞ Transparency Platform (Türkiye)
  * ECB, frankfurter.app and FRED (exchange rates, fuels)
  * Eurostat (end-user prices)
  * Open-Meteo / ERA5 (weather)
* **Zones:** 34 European bidding zones and Türkiye (35 in total), from 2015-01-01 to today.
* **Raw "data lake":** Every dataset is stored month by month at its native resolution. A re-run only fetches what is missing or recent, so the monthly update is cheap.
* **Analysis tables:** One 15-minute and one hourly table per zone, with **identical column names in every zone**. Prices are in EUR/MWh and time stamps are time-zone aware.
* **Audit:** An automatic quality check marks every zone PASS, WARN or FAIL.
* **Panel:** A combined dataset of all zones (hourly, monthly, yearly) with a data dictionary and a version stamp.

**It is not a data portal.** The repository contains code only. You download the data yourself, with your own key. Most sources allow research use but restrict republishing raw values; see [§ 11](#11-licences-and-citation).

---

## 2. Accounts and API keys

The code never contains credentials. They are read, in this order, from:
1. an **environment variable**;
2. a **Colab Secret** with the same name.

| Name | Needed for | How to get it |
|---|---|---|
| `ENTSOE_API_KEY` | everything European (**required**) | Register at *transparency.entsoe.eu*. Then send an e-mail to the ENTSO-E Transparency helpdesk asking for "Restful API access". The token then appears under *My Account Settings → Web API Security Token*. It is free. |
| `NTP_CLIENT_ID`, `NTP_CLIENT_SECRET` | Germany: reBAP, ID-AEP, activated aFRR/mFRR (optional) | Register at *api-portal.netztransparenz.de* and create a client ("client credentials"). |
| `EPIAS_USERNAME`, `EPIAS_PASSWORD` | Türkiye (optional) | Use the free registration form of the EPİAŞ Transparency Platform (the link is in the code header). |

**In Colab:**
1. Click the key symbol in the left bar.
2. Choose *Add new secret* and enter the name and value.
3. Switch on *Notebook access*.

**On your own computer:** `export ENTSOE_API_KEY=...` (Linux/macOS) or `set ENTSOE_API_KEY=...` (Windows).

`qw.credentials_check()` shows which credentials are found. It never prints the values. A missing optional credential only skips that source, and data already stored are kept.

> **Never** write a key into a notebook cell, an e-mail or a Git commit. A key that has been pushed to GitHub is public, even if you delete it later. Quarterwatt masks keys in all messages and stored files (`securityToken=***`). If you think your key has leaked, request a new one from ENTSO-E.

---

## 3. Installation and first run

### Google Colab (recommended for students)

1. Open `notebooks/Quarterwatt_Colab.ipynb` in Colab (*File → Open notebook → GitHub*).
2. In cell 2, set `REPO_RAW` to the raw URL of `quarterwatt.py` in your copy of the repository. Use a tag such as `v1.0` instead of `main` to pin a release.
3. Run cell **1** (mount Drive), cell **2** (get the code) and cell **3** (load, check credentials, status).
4. Start small: `qw.run(["DE"])`, then `qw.finalize()`.

The data go to `MyDrive/Quarterwatt/`, so they stay between Colab sessions.

### Own computer

```bash
python -m venv .venv && source .venv/bin/activate      # optional
pip install git+https://github.com/serhanbasturk/quarterwatt.git
export ENTSOE_API_KEY=...
python
>>> import quarterwatt as qw
>>> qw.run(["BE"])
>>> qw.finalize()
```

Without Google Drive, the data go to `./Quarterwatt/` in the current folder.

### How long does it take?

| Task | Duration |
|---|---|
| Quick test: one zone, last ~13 months (`qw.START_DATE = "2025-10-01"`) | about 10 min |
| First download of one zone, 2015 to today | about 1–2 h. It depends on the zone and on how busy the ENTSO-E API is. |
| First download of all 35 zones | many hours. Run it in batches (`qw.run(qw.CORE)`, `qw.run(qw.NORDIC)`, …) or overnight. |
| Monthly update of all zones | typically 1–3 h |
| `qw.finalize()` (audit and panel, no downloads) | about 10–20 min |

**Interruptions are harmless.** If Colab disconnects, run cells 1 → 2 → 3 and the same `qw.run(...)` again:
* zones finished in the last 20 h are skipped;
* the interrupted zone continues where it stopped;
* a zone that ended with failed months is not counted as finished, so it is retried.

---

## 4. The monthly workflow

```
1 mount  →  2 code  →  3 load + status  →  qw.run()  →  qw.finalize()  →  sync (cell 9)
```

* **`qw.run()`** downloads what is missing plus the last 60 days. The last 60 days are fetched again because sources publish late and revise data. It then rebuilds the tables of each zone.
* **`qw.finalize()`** loads exchange rates, audits all zones and, if no zone fails, rebuilds the combined panel, the README and `VERSION.json`.
* **Sync:** `drive.flush_and_unmount()` makes sure that Colab has written everything to Drive.

After a change of cleaning or currency rules (a new code version), `qw.finalize(["PL", "DE"])` first rebuilds these zones **from the raw files, without downloading**.

---

## 5. Where the data are: folder layout

```
Quarterwatt/
├── README.md                      overview, regenerated by finalize()
├── code/quarterwatt.py            (Colab) the code
├── data/
│   ├── zones/<CODE>_<Country>/    one folder per zone, e.g. DE_Germany, SE_1_Sweden
│   │   ├── raw/<dataset>/         monthly raw files  YYYY-MM-DD_YYYY-MM-DD__<area>.parquet
│   │   └── output/
│   │       ├── Quarterwatt_<CODE>_15min.parquet   master table, 15-min grid
│   │       ├── Quarterwatt_<CODE>_1h.parquet      hourly means (also .csv)
│   │       ├── raw_combined/<dataset>.parquet     native resolution, all columns
│   │       ├── manifest.json, quality_report.csv, gap_report.csv
│   ├── panel/                     ← start here
│   │   ├── panel_core_1h.parquet  all zones, hourly, core columns, long format (UTC)
│   │   ├── panel_monthly.csv, panel_yearly.csv
│   │   ├── zones.csv, data_dictionary.csv (meaning, unit, licence of every column),
│   │   │   data_dictionary_raw.csv (the raw-only tables of output/raw_combined)
│   │   └── VERSION.json           data version (YYYY.MM), code version, data_until
│   └── reference/                 levies, standard load profiles, SME profiles
├── audit/                         audit_summary.csv, audit_issues.csv, run_state.json
├── fx_rates/                      exchange rates used (copy, for reproducibility)
└── website/                       derived, publishable indicators (planned)
```

**Marker files in `raw/<dataset>/`:**

| Marker | Meaning |
|---|---|
| `.empty` | The source has no data for this month. It is not asked again (`RETRY_EMPTY = True` asks again). |
| `.rejected` | The source refuses the request (HTTP 400). The file holds the server's reason. It is not asked again (`RETRY_REJECTED = True` asks again). |
| `.checked` | A recent month was checked but had no data yet. It is asked again at the next monthly run. |

Do not edit files in `raw/` by hand. If something looks wrong, run `qw.clean_storage()`. It only *lists* unused items until you call it with `delete=True`. Your own folders in `Quarterwatt/` (for example `analysis/`) are never deleted.

---

## 6. Data model: time, units, currencies

### Time

* **Index:** time-zone aware, **interval start**. The 13:00 row covers 13:00–13:15 in the 15-min table and 13:00–14:00 in the hourly table.
* **Time zone:** all European zones are stored in **`Europe/Berlin` (CET/CEST)**, the market time of the European day-ahead coupling. This includes PT, IE, GR, FI and the Baltics. Türkiye uses `Europe/Istanbul`. If you need the local clock time of a zone, for example "hour of day" in Portugal, which is one hour behind CET, convert the index first: `df.tz_convert("Europe/Lisbon")`.
* **Daylight-saving days** have 92 or 100 quarter-hours. Nothing is duplicated or dropped.
* **The panel is in UTC** (`timestamp_utc`), so zones can be joined without time-zone errors. `qw.load_zone(code, utc=True)` also returns UTC.
* **Day-ahead prices:**
  * hourly market time unit until 30.09.2025;
  * 15 minutes since 01.10.2025 (SDAC change).
  * In the 15-min table, hourly prices are repeated for each quarter. In the hourly table, 15-min prices are averaged.

### Units

| Quantity | Unit | Note |
|---|---|---|
| Power (load, generation, flows) | **MW**, average over the interval | Energy per 15-min interval = MW × 0.25 MWh |
| Prices | **EUR/MWh** | Original currency columns keep a suffix, e.g. `P_DA_TRY` |
| Capacity prices | EUR/MW (per product) | see the column description |
| CO₂ intensity | g/kWh | see [§ 7](#co2) |

### Currencies

ENTSO-E publishes some series in the national currency. Quarterwatt converts everything to EUR with the **ECB monthly average rate**. If the ECB is not reachable, it uses frankfurter.app, then FRED, then the saved copy in `fx_rates/`. The rules, in order:

1. **Activated balancing energy prices:** the currency is read from the XML of every series (`currency_Unit.name`) and converted exactly.
2. **Imbalance prices:** fixed rules, checked on the data:
   * CZ: always CZK;
   * RO: always RON;
   * PL: always PLN;
   * HR: HRK until 01.01.2023.
3. **Day-ahead prices:** detection for PL, RO, BG, HR, CZ and HU. Each day is compared with a coupled euro zone. A value far above the euro level (geometric midpoint of the exchange rate) is treated as local currency.

Every conversion is logged in `manifest.json` → `currency_conversions` and reported as INFO by the audit.

---

## 7. Variable catalogue

Every column of every zone is documented in `data/panel/data_dictionary.csv`, with meaning, unit and licence. The raw-only tables (`output/raw_combined/`: regelleistung.net, ENTSO-E reserves, Eurostat, FRED, installed capacity) are listed in `data_dictionary_raw.csv`. The most important families:

### Prices (EUR/MWh)

| Column | Meaning |
|---|---|
| `P_DA` | Day-ahead price of the zone (SDAC; for TR the EPİAŞ PTF). **IT = IT_NORD**, not the national PUN. **AT** until 30.09.2018 = the common DE-AT-LU price (see § 8, comparability). |
| `P_DA_<ZONE>` | Day-ahead price of a neighbouring zone, e.g. `P_DA_FR` in the DE table |
| `P_IDA1`, `P_IDA2`, `P_IDA3` | Intraday auction prices (IDA1 from mid-2024; IDA2/3 from 13.06.2024). ENTSO-E serves them only for some zones (currently ES). Continuous intraday prices are not included; for Germany use `P_ID_AEP`. |
| `P_Imbalance` | **One continuous imbalance price series.** DE: reBAP (netztransparenz), BE: Elia, TR: SMF, otherwise ENTSO-E |
| `P_Imb_Long`, `P_Imb_Short` | ENTSO-E imbalance prices for surplus and deficit. In single-price systems only `P_Imb_Long` is filled. |
| `P_BalE_<aFRR\|mFRR>_<Up\|Down>` | Price of **activated** balancing energy (ENTSO-E 17.1.F), from 2019. It exists only in intervals with activation. A missing value means "no activation", **not zero**. |
| `P_reBAP_*`, `P_ID_AEP` | Germany: official reBAP and ID-AEP (netztransparenz) |
| `Res_*`, `RL_*` | Reserve capacity prices and volumes (raw tables; ENTSO-E until 2023-06; DE also regelleistung.net). Capacity prices are per MW **and product** (EUR/MW) or per MW **and hour** (EUR/MW/h): the unit is in the column name. |
| `RL_CapPrice_<FCR\|aFRR\|mFRR>_<Sym\|Up\|Down>` | **Use this for analyses.** regelleistung.net capacity price in EUR/MW/h, harmonised over the format changes (aFRR/mFRR: EUR/MW per product until 2021, EUR/MW/h after; FCR: German settlement column empty since 2022, cross-border value used). aFRR/mFRR = average price (pay-as-bid), FCR = settlement price. Annual revenue of 1 MW sold only in that product = mean × 8,760 h. |

### Load and generation (MW)

| Column | Meaning |
|---|---|
| `Load_Actual`, `Load_Forecast_DA`, `Load_Forecast_Error` | Actual load, day-ahead forecast, actual minus forecast |
| `Gen_<Type>` | Actual generation per production type, e.g. `Gen_Solar`, `Gen_Wind_Onshore`, `Gen_Fossil_Gas`, `Gen_Nuclear`, `Gen_Hydro_Pumped_Storage`, `Gen_Hydro_Pumped_Storage_Consumption` |
| `Gen_Total`, `Gen_Types_Reporting` | Sum of all types; number of types reported in that month (watch for jumps) |
| `Solar_Forecast_DA`, `Wind_*_Forecast_DA`, `*_Forecast_Error` | Day-ahead and intraday wind and solar forecasts, and their errors |
| `VRE_Generation`, `VRE_Share_of_Load`, `Residual_Load` | Solar + wind; VRE ÷ load; load − solar − wind |
| `Installed_Capacity` (raw) | Installed capacity per type, yearly |

### Exchange and balance (MW)

| Column | Meaning |
|---|---|
| `Flow_Import_<ZONE>`, `Flow_Export_<ZONE>` | Physical cross-border flows |
| `Net_Import_Physical`, `N_Borders_Reporting` | Imports − exports over all borders; number of borders with data |
| `NetPosition_DA` | Day-ahead net position (positive = export) |
| `Imb_Volume_Total` | Signed system imbalance |

### Context

| Column | Meaning |
|---|---|
| `WX_Temp_C`, `WX_GHI_Wm2`, `WX_Wind100m_kmh`, `WX_Cloud_pct` | Population-weighted ERA5 weather (weighted by city population, **not** by the PV or wind fleet: for renewable output use `Gen_Solar`, `Gen_Wind_*`) |
| `ES_*`, `ESC_*` | Eurostat industrial prices by consumption band, and price components (half-yearly) |
| `Gas_*`, `Coal_*`, `Brent`, `USD_per_EUR` | Monthly fuel prices (FRED/IMF) |
| `TR_*` | Türkiye: balancing instructions (YAL/YAT), imbalance quantities, FCR/aFRR, YEKDEM |

### Flags and calendar

| Column | Meaning |
|---|---|
| `Flag_Negative_DA` | 1 if the day-ahead price is below 0. In the hourly table this is the **hourly mean** price; since the 15-min day-ahead MTU (10/2025) use the 15-min table for quarter-hour detail. |
| `Flag_P_DA_Filled` | Share of the interval whose price was filled from the last published value (short gaps of ≤ 8 h, ENTSO-E curve type A03) |
| `Is_Cheapest_Energy_Window` | 1 if the interval is among the cheapest 15 % of its day (hourly table: of the hourly means) |
| `Is_Weekend`, `Is_Public_Holiday`, `Hour`, `Weekday`, `Month` | Calendar (local time) |

<a id="co2"></a>
### CO₂ intensity

* `CO2_Intensity_LCA_AR5_g_per_kWh`: life-cycle factors from IPCC AR5 (Schlömer et al. 2014), medians.
* `CO2_Intensity_approx_g_per_kWh`: an **approximate** direct-combustion intensity. The direct factors in the code are illustrative. For a thesis, use the factors of the source you cite, for example UBA, and recompute the intensity.

### The combined panel

`panel_core_1h.parquet` holds the core columns for all zones in long format (`timestamp_utc`, `zone`, …):
* `P_DA`, `P_IDA1`, `P_ID_AEP`, `P_Imbalance`
* `Load_Actual`, `Load_Forecast_DA`
* `Gen_Total`, `Gen_Solar`, `Gen_Wind_*`, `VRE_*`, `Residual_Load`
* `Net_Import_Physical`, `Net_Position`
* `CO2_*`
* weather and flags

`panel_monthly.csv` and `panel_yearly.csv` contain averages, sums and load-weighted prices per zone.

---

## 8. Data quality: cleaning rules, audit and known limits

### Cleaning rules (applied when the tables are built; raw files stay untouched)

| Rule | What it does |
|---|---|
| Price bounds | Day-ahead prices outside the SDAC limits are set empty. The limits are −500 and 3000 EUR/MWh, and from 10.05.2022 the upper limit is 5000. The rule is not applied to IE_SEM and TR. |
| A03 gaps | ENTSO-E sends repeated prices only once (curve type A03). Interior gaps of up to 8 h are filled with the last price and marked in `Flag_P_DA_Filled`. |
| Load | Values ≤ 0 and spikes more than 40 % of typical load away from the 5-hour median are set empty. |
| Single-price systems | `P_Imb_Short` exactly 0 in more than half of a month while `P_Imb_Long` has values → `P_Imb_Short` is set empty for that month. |
| Gaps | Real gaps stay **empty (NaN)**. Nothing is interpolated except the A03 rule above. |

### The audit (`qw.audit()`, part of `finalize()`)

For each zone the audit checks:
* coverage and freshness;
* plausible ranges and stuck (constant) values;
* currency levels (each price compared with `P_DA` and with the continental median);
* the TR exchange rate compared with the ECB rate;
* time zone (the solar peak must be between 9 and 15 h local time);
* the energy balance;
* structural breaks.

| Result | Meaning |
|---|---|
| **PASS** | no issue |
| **WARN** | something to look at; the zone is still in the panel |
| **FAIL** | a serious problem; the panel is not rebuilt until it is fixed |
| INFO | for the record, e.g. currency conversions and documented source limits |

The results are in `audit/audit_summary.csv` and `audit/audit_issues.csv`.

### Known source limits (November 2026)

* **Nordic zones:** imbalance prices only from 2021-11. Swedish generation only from 2021-12.
* **IE_SEM:** load on ENTSO-E has gaps and ends 2025-10.
* **GR:** imbalance prices only about 74 % complete.
* **RO:** dual imbalance pricing until 2021-02, so the surplus price is close to zero. Until 2024-06, upward aFRR prices sit at the regulatory cap (2,000–3,500 RON/MWh).
* **IT, SK:** single-price imbalance systems (IT from 2022-04, SK from 2024-07), so `P_Imb_Short` is empty from then on.
* **CH:** balancing energy prices not used (implausible on ENTSO-E).
* **Activated balancing volumes (ENTSO-E 17.1.E):** no longer offered by the API.
* **DE:** ENTSO-E has no German imbalance prices after 2022-06. Use `P_Imbalance` (reBAP, netztransparenz).
* **Balancing energy prices:** some months are refused by ENTSO-E (HTTP 400). They are listed in `manifest.json` → `rejected_by_source`.
* **ENTSO-E reserve prices (`Res_*`):** end 2023-06. For Germany use the regelleistung.net tables.
* **BG:** no imbalance prices on ENTSO-E.

### Comparability: structural breaks and definitions

The audit checks each zone for data errors. It does not make zones or years comparable with each other; for that, take the following into account.

| Topic | What to do |
|---|---|
| **DE-AT-LU** | Until 30.09.2018 Germany, Austria and Luxembourg formed one price zone. The AT price is identical to DE in that period: do not count AT as a separate observation in cross-zone regressions before 10/2018. |
| **Net imports** | `Net_Import_Physical` sums the borders that report data. The set changes: DE-AT is an internal border before 10/2018, NO_2-DE (NordLink) exists from 2020. Check `N_Borders_Reporting` before using trends; early DE net exports look too low (2015: −26.7 TWh). |
| **IT** | `P_DA` is IT_NORD, not the national PUN. |
| **Imbalance prices** | `P_Imbalance` is one series per zone, but the definitions differ: DE reBAP, BE Elia (column change at MARI, 22.05.2024), TR SMF, otherwise ENTSO-E (single or dual price, see the limits above). Compare levels across zones only with care. |
| **15-min prices** | Since 01.10.2025 the hourly table averages four quarter-hour prices; spreads and negative-price counts are smaller than in the 15-min table (DE: daily spread about −15 %). The panel therefore has both `neg_price_hours` / `daily_spread_mean` (hourly) and `neg_price_qh_hours` / `daily_spread_qh_mean` (15-min). Battery and 15-min flexibility analyses need the 15-min table. |
| **CO₂** | Average intensity of **domestic generation** with fixed factors per technology; imports and `Gen_Other` are not included. It is not a marginal emission factor. |
| **Partial years** | The current year ends at the last download. Label it "YTD" in tables. |

### Provenance

`data/panel/VERSION.json` records the data version and the code version that built the panel. Cite both, together with the GitHub release of that code version (see § 11). After a code update, run `qw.finalize()` once so that the stamp matches the code.

---

## 9. Working with the data: recipes

```python
import quarterwatt as qw
import pandas as pd
```

**One zone, chosen columns (hourly, local time):**
```python
de = qw.load_zone("DE", ["P_DA", "Load_Actual", "Gen_Solar"])
de.loc["2025"].describe()
```

**Several zones, UTC, long format → wide:**
```python
p = qw.load_panel(["P_DA"], zones=["DE", "FR", "PL"])
wide = p.pivot(index="timestamp_utc", columns="zone", values="P_DA")
```

**15-minute data:**
```python
import os
m = pd.read_parquet(os.path.join(qw.zone_location("DE")[0], "output", "Quarterwatt_DE_15min.parquet"))
```

**Load-weighted average price (LWAEC) of a load profile:**
```python
de = qw.load_zone("DE", ["P_DA", "Load_Actual"]).loc["2024"]
lwaec = (de.P_DA * de.Load_Actual).sum() / de.Load_Actual.sum()
baseload = de.P_DA.mean()
print(f"LWAEC {lwaec:.1f} EUR/MWh, baseload {baseload:.1f}, profile factor {lwaec / baseload:.3f}")
```

**Solar capture price and negative-price hours per year:**
```python
d = qw.load_zone("DE", ["P_DA", "Gen_Solar", "Flag_Negative_DA"])
yr = d.groupby(d.index.year)
capture = yr.apply(lambda x: (x.P_DA * x.Gen_Solar).sum() / x.Gen_Solar.sum())
neg_hours = yr.Flag_Negative_DA.sum()
```

**Daily price spread (simple BESS indicator, 2 h battery):**
```python
p = qw.load_zone("BE", ["P_DA"]).P_DA
spread = p.groupby(p.index.date).apply(lambda x: x.nlargest(2).mean() - x.nsmallest(2).mean())
```

**Reference profiles:**
```python
slp = qw.load_reference("slp")          # BDEW profiles, kW per MWh/a, 15 min
sme = qw.load_reference("braeuer")      # long: timestamp, plant, kW
lev = qw.load_reference("levies")       # German levies, ct/kWh, with validity dates
```

**Joining a 15-min profile to prices:**
```python
prof = sme[sme.plant == "2016_IP1"].set_index("timestamp").kW
px = pd.read_parquet(...15min...).P_DA                 # see above
both = pd.concat([prof.rename("kW"), px], axis=1, join="inner")   # same tz-aware index
```

> **Tip:** Always state the time zone, the period and the data version in a figure caption or table note.

---

## 10. Reference data

`qw.update_reference()` builds these once (`force=True` rebuilds them):

| Name | Content | Source / licence |
|---|---|---|
| `levies` | German levies and taxes 2015–2026 (EEG, KWKG, § 19 StromNEV, offshore, AbLaV, electricity tax) by category, ct/kWh, with `valid_from`/`valid_to` (EEG 2022 split at 01.07.) | official publications; category A complete, B/C partial. **Check before use.** |
| `slp` | BDEW standard load profiles (h0, g0–g6, l0–l2), 15 min, 2015–2027, kW per 1 MWh/a | demandlib (BDEW) |
| `braeuer` | Measured 15-min load of 50 German SME plants, 2016 and 2017, interval start, Europe/Berlin | Braeuer et al. (2020), Zenodo 10.5281/zenodo.3899018, CC BY 4.0. The 2017 file lacks its last 100 quarter-hours. |

---

## 11. Licences and citation

| Source | Use in a thesis | Republishing raw values |
|---|---|---|
| ENTSO-E Transparency Platform | yes, cite ENTSO-E | **No** unless the series is on ENTSO-E's free re-use list. Publish derived indicators instead. |
| netztransparenz.de, regelleistung.net | yes, cite | **No** without consent of the German TSOs |
| EPİAŞ | yes, cite | to be checked |
| Elia Open Data | yes | yes, CC BY 4.0 |
| ERA5 via Open-Meteo | yes | yes, CC BY 4.0 (free API for non-commercial use) |
| Eurostat, FRED, ECB | yes | yes, with attribution |
| Braeuer et al. (Zenodo) | yes | yes, CC BY 4.0 |

The column `licence` in `data_dictionary.csv` gives this per variable.

**Rule of thumb:**
* A table of monthly averages, a figure or a regression result is fine.
* A CSV of hourly ENTSO-E prices attached to your thesis is not.

**Cite three things:**
1. the original data sources;
2. Quarterwatt: Basturk, S. (2026), Zenodo, https://doi.org/10.5281/zenodo.24054659 (see `CITATION.cff`);
3. the **data version** from `data/panel/VERSION.json`, e.g. *"Quarterwatt data version 2026.11, code 1.1"*.

---

## 12. Troubleshooting

| Message / symptom | Cause | What to do |
|---|---|---|
| `NameError: name 'qw' is not defined` | Colab restarted and the memory is empty | run cells 1 → 2 → 3 again |
| `HTTP 401` | key missing or wrong | `qw.credentials_check()`; check the Colab Secret name and *Notebook access* |
| `HTTP 403` from ENTSO-E (Azure gateway page) | too many requests, so your IP is blocked for some hours | wait, or start a new Colab runtime; `qw.api_check()` tests one request |
| `HTTP 400` for some months | the source refuses this request | stored as `.rejected`; see `manifest.json` |
| `HTTP 599` / `503` / connection error | temporary server problem | nothing; retried automatically, and failed months are fetched at the next run |
| Run takes hours | first download | run in batches; interruptions are resumed |
| Panel not rebuilt | a zone has FAIL | read `audit/audit_issues.csv` |
| Old files after a code update | rules changed | `qw.finalize([...zones...])` rebuilds from raw without downloading |
| Drive shows old files | Colab has not synced yet | cell 9: `drive.flush_and_unmount()` |

---

## 13. Guidance for theses

1. **Freeze your data.** Note the data version at the start of your analysis, or set `END_DATE = "2026-10-01"` for a fixed snapshot. Do not re-run `qw.run()` in the middle of your analysis without noting the new version.
2. **Separate download and analysis.** Use the Runner notebook only for downloading. Write your analysis in its own notebook that only *reads* (`load_zone`, `load_panel`).
3. **Read the audit for your zones** before you interpret results. Report known limits in your data section.
4. **Treat empty values honestly.** Do not fill gaps with zeros. For balancing energy prices, a missing value means "no activation".
5. **Time zones:** join zones in UTC (panel). For daily profiles such as "hour of day", remember that every European zone is stored in CET/CEST; convert to the zone's own clock where that matters.
6. **Hourly vs. 15 minutes:** before 10/2025 day-ahead prices are hourly. A 15-min analysis of older periods only repeats hourly prices.
7. **Currencies:** all prices are EUR. For national-currency results use the rates in `fx_rates/`, the same ones the code used.
8. **Do not attach raw data** to the thesis or publish it on GitHub. Share the code and the data version, so others can rebuild the same data.
9. **Reproducibility checklist for the appendix:**
   * code version;
   * data version;
   * zones and period;
   * cleaning rules used;
   * any own filters.

---

## 14. For developers: how the code works and how to extend it

### Structure of `quarterwatt.py`

| Section | Content |
|---|---|
| CONFIG | zone groups, period, switches (`USE_WEATHER`, `REFETCH_GAPS`, `VOLATILE_DAYS`, …) |
| `PROFILES` | one entry per zone: name, time zone, ENTSO-E areas, neighbours, special sources |
| fetch functions | `f_*`, `make_*_fn`: one function per dataset, signature `fn(client, area, start, end) → DataFrame` |
| `build_datasets()` | registers datasets with `D(name, fn, areas, group, unit, description)` |
| `run_task` / `run_tasks` | download state machine per month (cached / ok / empty / failed / rejected), parallel, rate-limited |
| `assemble` | raw files → 15-min master → hourly table; cleaning, currency, derived columns |
| `audit`, `build_panel`, `finalize` | quality checks, combined panel, README |
| reference block | levies, SLP, Braeuer |

### Adding a dataset

1. Write a fetch function that returns a DataFrame with a tz-aware `DatetimeIndex` and clearly named columns:
   ```python
   def f_my_series(c, area, s, e):
       return single(c.query_something(area, start=s, end=e), "My_Column")
   ```
2. Register it in `build_datasets()`:
   ```python
   D("My_Series", f_my_series, AREAS_ZONE, "extended", "MW", "What it is [ENTSO-E article]"),
   ```
3. Add the column to `COLUMN_DOCS` (meaning and unit) and to `LICENCE_RULES`.
4. Write a test in `tests/` with a mocked client. Look at `test_v14.py` as an example, and add it to `tests/run_tests.py`.
5. Run `python tests/run_tests.py`. All tests must pass, including the API contract test.

### Rules for changes

* Do not change raw files or the meaning of an existing column. Add a new column instead.
* Every change of cleaning or currency rules must come with a test, a CHANGELOG entry and a version number.
* Never commit credentials. The tests check that the code contains none.

---

## 15. Function reference

| Function | What it does |
|---|---|
| `run(countries=None, force=False)` | download/update zones; resumable; `force=True` processes zones again |
| `finalize(rebuild_zones=None)` | FX check → (offline rebuild) → audit → panel → README → `VERSION.json` |
| `rebuild(countries=None)` | rebuild tables from raw files, no downloads |
| `audit(zones=None)` | quality checks, writes `audit/` |
| `build_panel(zones=None)` | combined panel |
| `status(countries=None)` | overview of downloaded zones (no downloads) |
| `load_zone(code, columns=None, utc=False)` | hourly table of one zone |
| `load_panel(columns=None, zones=None)` | combined hourly panel, long format, UTC |
| `update_reference(force=False)`, `load_reference(name)` | reference data (`'levies'`, `'slp'`, `'braeuer'`) |
| `credentials_check()`, `api_check()`, `fx_check()` | diagnostics |
| `clean_storage(delete=False)` | list (or remove) files the code no longer uses |
| `configure(code)` | switch the active zone (used internally; useful in your own loops) |

Zone groups: `qw.CORE`, `qw.NORDIC`, `qw.CEE`, `qw.OTHER`, `qw.TURKEY`; all zones: `qw.COUNTRIES`.
