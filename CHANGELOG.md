# Changelog

## 1.0.0 (2026-11-08) – first public release

* 34 European bidding zones and Türkiye, 2015 onwards, updated monthly.
* Monthly raw data lake (Parquet). It is resumable after interruptions, and months the source refuses are remembered.
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
* API keys are read from environment variables or Colab Secrets and masked in all messages and stored files.
* 21 offline tests: an API contract test against `entsoe-py` 0.8.1, and a test that runs the handbook recipes. Tested on pandas 2.2, 3.0 and 3.1.
