# Quarterwatt

[![tests](https://github.com/serhanbasturk/quarterwatt/actions/workflows/tests.yml/badge.svg)](https://github.com/serhanbasturk/quarterwatt/actions/workflows/tests.yml)
[![licence: MIT](https://img.shields.io/badge/licence-MIT-blue.svg)](LICENSE)
[![python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](pyproject.toml)
[![zones](https://img.shields.io/badge/zones-34%20EU%20%2B%20TR-orange.svg)](docs/HANDBOOK.md#7-variable-catalogue)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.24054659.svg)](https://doi.org/10.5281/zenodo.24054659)
[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/serhanbasturk/quarterwatt/blob/main/notebooks/Quarterwatt_Colab.ipynb)

**A reproducible data lake for European and Turkish electricity markets, in quarter-hour detail from 2015.**

Quarterwatt is a single Python module, `quarterwatt.py`. It downloads market data for **34 European bidding zones and Türkiye** and stores every month as a raw file. From these it builds clean, harmonised 15-minute and hourly tables with identical column names in every zone, checks their quality, and combines them into one research panel. It runs in Google Colab or on your own computer.

> **Script, not data.** This repository holds code only. Each user downloads the data with their own free API key into their own storage. Most sources allow research use but restrict redistribution of the raw values; see [Licences](docs/HANDBOOK.md#11-licences-and-citation).

## Welcome

I am a professor of sustainable production based in Germany. My research looks at how manufacturing can become cleaner and more energy-flexible, and at how industrial firms can adapt to electricity markets in which prices change every quarter of an hour.

Quarterwatt is the data lake behind my own research: load-weighted electricity costs of industrial load profiles, the economics of battery storage, and AI-based scheduling of production and energy assets. The students who write their theses with me began to use it too, and they asked for a version they could run themselves. That request is why I decided to put it online.

I hope it saves other researchers and students the weeks it takes to collect, clean and harmonise European electricity-market data, so that they can spend their time on the questions that matter. The code is open, the documentation is written for newcomers, and every data release is versioned so that results can be reproduced.

At **[quarterwatt.com](https://quarterwatt.com)**, these data come together with results from my research: indicators, comparisons across zones and analyses built on the same data lake.

If you use Quarterwatt, find an error or would like to contribute, please open an issue. I would be glad to hear how it helps your work.

Best regards,<br>
**Prof. Dr. Serhan Basturk**<br>
[LinkedIn](https://www.linkedin.com/in/serhanbasturk/) · [University profile](https://www.thm.de/w/serhan-bastuerk)

## What you get

| | |
|---|---|
| **Zones** | DE, BE, FR, NL, AT, PL, ES, IT · SE 1–4, NO 1–5, DK 1–2, FI · CZ, SK, HU, RO, SI, HR, BG · PT, EE, LV, LT, GR, IE (SEM), CH · TR |
| **Period** | 2015-01-01 → today, updated monthly |
| **Prices** | day-ahead (15 min since 10/2025), intraday auctions IDA1–3, imbalance prices, activated aFRR/mFRR balancing energy prices, reserve capacity prices; all converted to EUR/MWh |
| **Physical data** | load and load forecast, generation per technology, wind and solar forecasts, cross-border flows, net positions, installed capacity, system imbalance |
| **Derived data** | residual load, VRE share, CO₂ intensity (direct and life-cycle), cheapest-hour and negative-price flags |
| **Context** | population-weighted weather (ERA5), Eurostat industrial prices and price components, fuel prices (FRED), German grid levies, BDEW standard load profiles, 50 measured German SME load profiles |
| **Germany extra** | netztransparenz.de (reBAP, ID-AEP, activated aFRR/mFRR), regelleistung.net capacity auctions |
| **Türkiye** | EPİAŞ: PTF, SMF, intraday market, generation, balancing instructions, FCR/aFRR, YEKDEM |
| **Quality** | per-zone audit (coverage, freshness, ranges, currency, time zone, consistency), PASS/WARN/FAIL, documented known limits |

## Quick start (Google Colab)

1. Get a free ENTSO-E API key (see [Handbook § 2](docs/HANDBOOK.md#2-accounts-and-api-keys)) and store it as a Colab Secret called `ENTSOE_API_KEY`.
2. Open the notebook in Colab with the **Open in Colab** button above.
3. Run sections 1 → 2 → 3. Then try a **quick test** that takes a few minutes, as described below.

**Quick test.** The first full download of one zone (2015 to today) takes one to two hours, because the ENTSO-E server answers slowly. To check that everything works, start with a short period:

```python
qw.START_DATE = "2025-10-01"      # the last ~13 months, since 15-min day-ahead prices
qw.run(["BE"])                    # about 10 minutes
qw.load_zone("BE", ["P_DA", "Load_Actual"]).tail()
```

Then set `qw.START_DATE = "2015-01-01"` and run `qw.run(["BE"])` again. Months already downloaded are kept.

```python
import quarterwatt as qw
de = qw.load_zone("DE", ["P_DA", "Load_Actual", "Gen_Solar"])            # hourly, local time
panel = qw.load_panel(["P_DA", "Load_Actual"], zones=["DE", "BE", "FR"])  # long format, UTC
```

## Quick start (own computer)

```bash
pip install git+https://github.com/serhanbasturk/quarterwatt.git
export ENTSOE_API_KEY=...            # Windows: set ENTSOE_API_KEY=...
python -c "import quarterwatt as qw; qw.run(['BE']); qw.finalize()"
```

To work on the code or run the tests, clone the repository instead: `git clone https://github.com/serhanbasturk/quarterwatt.git` and `pip install -e .`

The data are written to `./Quarterwatt/`.

## Documentation

* **[Handbook](docs/HANDBOOK.md):** setup, data model, every variable, analysis recipes, quality checks, troubleshooting, guidance for theses, how to extend the code.
* [CHANGELOG](CHANGELOG.md): what changed in each version.
* [CONTRIBUTING](CONTRIBUTING.md): how to report a problem or contribute code.

## Tests

```bash
python tests/run_tests.py
```

There are 21 offline tests, and no API key is needed because every source is simulated. One of them, the API contract test, checks every ENTSO-E request in every zone against the installed `entsoe-py`. Another runs the examples of the handbook, so the documentation stays correct. The suite passes under pandas 2.2, 3.0 and 3.1. GitHub Actions runs the suite on every push.

## Citation

Please cite the software and the **data version** you used, which is stored in `data/panel/VERSION.json`:

> Basturk, S. (2026). *Quarterwatt: a reproducible data lake for European and Turkish electricity markets* [Software]. Zenodo. https://doi.org/10.5281/zenodo.24054659 (data version 2026.11)

The DOI always points to the latest version. GitHub's *Cite this repository* button, which uses `CITATION.cff`, gives the same reference in BibTeX and APA.

## Licence

The code is released under the MIT licence. The data remain subject to the terms of their sources.
