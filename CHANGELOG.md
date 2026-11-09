# Changelog

## 1.1 (2026-11-09)

Fixes from an external review of the data lake. No re-download is needed: run `qw.finalize(["DE"])` once (offline rebuild of Germany, then audit and panel).

* **regelleistung.net:** new harmonised columns `RL_CapPrice_<FCR|aFRR|mFRR>_<dir>` in EUR/MW/h. They bridge the change of the aFRR/mFRR capacity price unit (EUR/MW per product until 2021, EUR/MW/h after) and the empty German FCR settlement column since 2022. Raw columns are unchanged.
* **Panel:** quarter-hour indicators `neg_price_qh_hours` and `daily_spread_qh_mean` in `panel_monthly.csv` and `panel_yearly.csv`, computed from the 15-min table (since the 15-min day-ahead MTU the hourly means understate spreads).
* **Data dictionary:** corrected units (Elia volumes in MW, `P_SMF_TRY` and `P_DA_TRY` in TRY/MWh, `P_DA_USD` in USD/MWh); descriptions and licences for the netztransparenz columns (NRV, activated aFRR/mFRR, FCR), `NetPosition_DA`, `Imb_Volume_Total`, `P_IDM_WAP_TRY` and the EPİAŞ columns; the index column is no longer listed. New `data_dictionary_raw.csv` lists the raw-only tables (regelleistung.net, ENTSO-E reserves, Eurostat, FRED, installed capacity) per zone.
* **Handbook:** new section on comparability (DE-AT-LU until 09/2018, changing border coverage of net imports, IT = IT_NORD, heterogeneous imbalance definitions, 15-min vs hourly, CO₂ definition, partial years) and on provenance.
* `P_DA_USD` (TR) is excluded from the neighbour-price rules.

## 1.0 (2026-11-08) – first public release

* 34 European bidding zones and Türkiye, 2015 onwards, updated monthly.
* Monthly raw data lake (Parquet), 8 parallel requests under a fixed rate cap. It is resumable after interruptions, and months the source refuses are remembered.
* 15-min and hourly tables with identical column names in every zone. Time stamps are time-zone aware and mark the interval start.
* Currency harmonisation to EUR with ECB monthly rates:
  * balancing prices use the currency stated in the source XML;
  * imbalance prices use fixed rules checked on the data;
  * day-ahead prices use detection against a coupled euro zone.
* Cleaning rules: SDAC price bounds, curve type A03 gaps, load spikes, single-price imbalance systems.
* Quality audit per zone (PASS / WARN / FAIL), with documented known source limits.
* Combined panel of all zones (hourly, monthly, yearly), data dictionary with licence column, data version stamp.
* Reference data: German levies 2015–2026, BDEW standard load profiles, measured SME load profiles (Braeuer et al. 2020).
* Installable with `pip install git+https://github.com/serhanbasturk/quarterwatt.git`; Zenodo metadata for a citable DOI.
* Quick-test mode (`START_DATE`) and an *Open in Colab* button.
* API keys are read from environment variables or Colab Secrets and masked in all messages and stored files.
* 21 offline tests: an API contract test against `entsoe-py` 0.8.1, and a test that runs the handbook recipes. Tested on pandas 2.2, 3.0 and 3.1.
