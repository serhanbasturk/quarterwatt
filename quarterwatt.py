# =============================================================================
#  Quarterwatt  v1.0  –  European (+ Türkiye) electricity data lake
# =============================================================================
#  Choose the countries with COUNTRIES = [...] in the CONFIG section
#  (8 core countries, 26 further bidding zones, Türkiye). Processed one after another,
#  each into its own folder Quarterwatt/data/zones/<CODE>_<Country>. All zones
#  produce IDENTICAL column names (P_DA, Gen_Solar, Load_Actual, ...).
#
#  What it does
#  ------------
#  1. Fetches ~40 electricity-market series from the ENTSO-E
#     Transparency Platform, month by month, in parallel (rate-limited).
#  2. Stores every month as its own raw Parquet file at NATIVE resolution
#     ("data lake"). Re-running only fetches what is missing, failed, or
#     recent (last VOLATILE_DAYS days, because ENTSO-E revises/late-publishes).
#  3. Builds analysis tables on top of the raw files:
#        output/Quarterwatt_<CODE>_15min.parquet  (master, 15-min grid)
#        output/Quarterwatt_<CODE>_1h.parquet     (hourly means + .csv)
#        output/raw_combined/<dataset>.parquet (native resolution, all columns)
#        output/manifest.json, output/quality_report.csv
#
#  Credentials are NEVER stored in this file. They are read from Colab
#  Secrets (key symbol in the left Colab bar) or environment variables:
#     ENTSOE_API_KEY                    ENTSO-E Transparency Platform
#     NTP_CLIENT_ID, NTP_CLIENT_SECRET  netztransparenz.de (Germany, reBAP etc.)
#     EPIAS_USERNAME, EPIAS_PASSWORD    EPİAŞ Şeffaflık Platformu (Türkiye)
#  credentials_check() shows which ones are available.
#  Units: power in MW (average over the interval), prices in EUR/MWh.
#         Energy per 15-min interval = MW * 0.25 (MWh).
# =============================================================================

import subprocess
import sys

# install missing packages automatically (Colab / fresh environments)
for _pkg, _mod in [("entsoe-py", "entsoe"), ("pyarrow", "pyarrow"), ("holidays", "holidays"),
                   ("openpyxl", "openpyxl"), ("eptr2", "eptr2")]:
    try:
        __import__(_mod)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", _pkg])

import os
import re
import io
import glob
import warnings
import json
import zipfile
import time
import getpass
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

# regelleistung.net xlsx files have no style sheet -> harmless openpyxl warning
# on every file; silence it so real messages stay visible
warnings.filterwarnings("ignore", message="Workbook contains no default style")
if hasattr(pd.errors, "Pandas4Warning"):         # entsoe-py parsers under pandas 3
    warnings.filterwarnings("ignore", category=pd.errors.Pandas4Warning)
import requests
import bs4
import entsoe
from entsoe import EntsoePandasClient, EntsoeRawClient
from entsoe.exceptions import (NoMatchingDataError,
                               InvalidBusinessParameterError,
                               InvalidPSRTypeError)
from entsoe.mappings import NEIGHBOURS

# =============================================================================
#  CONFIG
# =============================================================================
# Countries to process, one after another. Already downloaded months are
# skipped, so a country that is complete only costs a few minutes.
#   DE Germany, BE Belgium, FR France, NL Netherlands, AT Austria,
#   PL Poland, ES Spain, IT Italy (price = IT_NORD, see profile)
CORE = ["DE", "BE", "FR", "NL", "AT", "PL", "ES", "IT"]
NORDIC = ["SE_1", "SE_2", "SE_3", "SE_4", "NO_1", "NO_2", "NO_3", "NO_4", "NO_5",
          "DK_1", "DK_2", "FI"]
CEE = ["CZ", "SK", "HU", "RO", "SI", "HR", "BG"]
OTHER = ["PT", "EE", "LV", "LT", "GR", "IE_SEM", "CH"]
TURKEY = ["TR"]                          # needs EPIAS_USERNAME / EPIAS_PASSWORD
# Order: new zones first, finished core countries last (only updates).
# First full download of all new zones takes long (~1 h per zone) -> it is
# resumable; shorten the list to run in batches, e.g. COUNTRIES = NORDIC.
COUNTRIES = NORDIC + CEE + OTHER + TURKEY + CORE
COUNTRY = COUNTRIES[0]   # set automatically for each country

TZ = "Europe/Berlin"     # CET countries; Türkiye switches to Europe/Istanbul
START_DATE = "2015-01-01"
END_DATE = None          # None = today (every run extends the data set);
                         # a fixed date, e.g. "2026-10-01", gives a citable snapshot
START = pd.Timestamp(START_DATE, tz=TZ)
# Exclusive end. "today 00:00" means every re-run extends the data set.
# For a frozen, citable snapshot set a fixed date, e.g. "2026-10-01".
END = pd.Timestamp(END_DATE, tz=TZ) if END_DATE else pd.Timestamp.now(tz=TZ).normalize()
ZONE_SWITCH = pd.Timestamp("2018-10-01 00:00", tz=TZ)  # German zone: DE_AT_LU -> DE_LU

# Which dataset groups to fetch: "core", "extended", "experimental"
GROUPS = {"core", "extended", "experimental"}

USE_GOOGLE_DRIVE = True              # Colab: keep checkpoints across sessions

MAX_WORKERS = 4                      # parallel threads
MAX_REQUESTS_PER_MIN = 240           # ENTSO-E allows 400/min; above -> temp ban
MAX_TRIES = 4                        # per segment and area
VOLATILE_DAYS = 60                   # re-fetch segments ending within N days
RESUME_HOURS = 20                    # restart after a disconnect: recent months fetched and
                                     # zones finished within N hours are NOT done again
# netztransparenz.de (German TSOs) – official reBAP and NRV balance.
# ENTSO-E has NO German imbalance prices after 2022-06-21.
# Free registration: https://api-portal.netztransparenz.de -> create a client
# -> paste Client-ID / Client-Secret here. Empty = module is skipped.
CODE_VERSION = "1.0"


def _read_json(path):
    with open(path) as f:
        return json.load(f)


def _secret(name):
    """Environment variable -> Colab Secret -> "" (never stored in the code)."""
    val = os.environ.get(name, "")
    if not val:
        try:
            from google.colab import userdata
            val = userdata.get(name) or ""
        except Exception:
            val = ""
    return str(val).strip().strip('"').strip("'").strip()     # pasted with quotes/blanks


NTP_CLIENT_ID = _secret("NTP_CLIENT_ID")
NTP_CLIENT_SECRET = _secret("NTP_CLIENT_SECRET")

# Additional sources (each can be switched off independently)
USE_ELIA = True              # Belgium: Elia Open Data (imbalance price & system imbalance)
USE_REGELLEISTUNG = True     # Germany: regelleistung.net tender results (EXPERIMENTAL:
                             # undocumented download interface of the datacenter)
REGELLEISTUNG_START = "2020-07-01"   # daily 4h products for FCR/aFRR/mFRR since mid-2020

# EPİAŞ Şeffaflık Platformu (Türkiye) – free registration:
# https://kayit.epias.com.tr/epias-transparency-platform-registration-form
EPIAS_USERNAME = _secret("EPIAS_USERNAME")
EPIAS_PASSWORD = _secret("EPIAS_PASSWORD")

USE_WEATHER = True           # Open-Meteo ERA5 (no key; CC BY 4.0; non-commercial use)
USE_EUROSTAT = True          # Eurostat nrg_pc_205 (+ _c): industrial end-user prices
USE_FRED = True              # FRED/IMF monthly gas (EU), coal, Brent + USD/EUR

RETRY_EMPTY = False          # True: re-query months marked "no data" (.empty)
RETRY_REJECTED = False       # True: re-query months the source answered with HTTP 400 (.rejected)

# Gap audit: after assembling, every series is checked month by month INSIDE
# its own data range. Months with more than GAP_THRESHOLD_PCT missing are
# listed in output/gap_report.csv and (if REFETCH_GAPS) downloaded once more.
# Each month is re-tried only once ever (output/gap_retried.json), so genuine
# gaps at the source do not cause endless downloads.
REFETCH_GAPS = True
GAP_THRESHOLD_PCT = 5.0
SPARSE_SERIES_PCT = 50.0   # series missing more than this inside their own range
                           # are sparse by nature (event prices etc.) -> not audited

WRITE_CSV_1H = True                  # ~100k rows, fine for Excel/R/Stata
WRITE_CSV_15MIN = False              # ~400k rows x 100 cols -> several 100 MB


# Approximate DIRECT combustion emission factors (g CO2 / kWh_el).
# Illustrative placeholders only – replace with the values of the reference
# you cite (e.g. UBA, IPCC). Types not listed count as 0 (nuclear, RES,
# biomass as biogenic). Keys must match generation column names ("Gen_<key>").
# Life-cycle emission factors (g CO2eq/kWh): IPCC AR5 WGIII Annex III,
# Table A.III.2 (Schlömer et al. 2014), medians. Oil: IPCC SRREN Annex II
# median. Lignite, coal gases, peat, oil shale: coal value (assumption, flagged).
# Waste / other / storage: not assigned (0) – document this in a paper.
EMISSION_FACTORS_LCA = {
    "Fossil_Brown_coal_Lignite": 820, "Fossil_Hard_coal": 820,
    "Fossil_Coal_derived_gas": 820, "Fossil_Peat": 820, "Fossil_Oil_shale": 820,
    "Fossil_Gas": 490, "Fossil_Oil": 840,
    "Biomass": 230, "Geothermal": 38, "Nuclear": 12,
    "Hydro_Run_of_river_and_pondage": 24, "Hydro_Water_Reservoir": 24,
    "Solar": 48, "Wind_Onshore": 11, "Wind_Offshore": 12,
}

EMISSION_FACTORS = {
    "Fossil_Brown_coal_Lignite": 1100,
    "Fossil_Hard_coal": 850,
    "Fossil_Coal_derived_gas": 1000,
    "Fossil_Gas": 400,
    "Fossil_Oil": 750,
    "Fossil_Peat": 1100,          # approx., same caveat as above
    "Fossil_Oil_shale": 1000,     # approx. (Estonia), same caveat
}

# =============================================================================
#  SETUP: folders, API key, rate-limited per-thread clients
# =============================================================================

PROJECT_NAME = "Quarterwatt"
DRIVE_ROOT = "/content/drive/MyDrive"


def _on_drive():
    """True in Colab with Google Drive (mounted on first use)."""
    if not USE_GOOGLE_DRIVE:
        return False
    try:
        from google.colab import drive
    except ImportError:
        return False
    if not os.path.isdir(DRIVE_ROOT):
        drive.mount("/content/drive")
    return True


def root_dir():
    """Quarterwatt/ - the ONE main folder of the project."""
    return os.path.join(DRIVE_ROOT, PROJECT_NAME) if _on_drive() else PROJECT_NAME


def zone_folder_name(code):
    return f"{code}_{PROFILES[code]['name'].split('_')[0]}"     # e.g. SE_1_Sweden


def zone_location(code):
    """(folder, file prefix) of one zone:
    Quarterwatt/data/zones/<CODE>_<Country>/ with files Quarterwatt_<CODE>_*."""
    return (os.path.join(root_dir(), "data", "zones", zone_folder_name(code)),
            f"{PROJECT_NAME}_{code}")


def resolve_base_dir():
    if DRIVE_FOLDER is None:
        configure(COUNTRY)
    return DRIVE_FOLDER


ENTSOE_API_KEY = _secret("ENTSOE_API_KEY")


def get_api_key():
    return ENTSOE_API_KEY or _secret("ENTSOE_API_KEY") or getpass.getpass("ENTSO-E API key: ")


def credentials_check():
    """Which credentials are available (values are never printed)."""
    rows = [("ENTSOE_API_KEY", ENTSOE_API_KEY, "all ENTSO-E data (required)"),
            ("NTP_CLIENT_ID", NTP_CLIENT_ID, "Germany: reBAP, NRV, ID-AEP, activated aFRR/mFRR"),
            ("NTP_CLIENT_SECRET", NTP_CLIENT_SECRET, ""),
            ("EPIAS_USERNAME", EPIAS_USERNAME, "Türkiye (EPİAŞ)"),
            ("EPIAS_PASSWORD", EPIAS_PASSWORD, "")]
    ok = True
    for name, val, use in rows:
        print(f"   {name:18s} {'set' if val else 'MISSING':8s} {use}")
        ok &= bool(val)
    if not ok:
        print(">>> add missing ones in Colab: key symbol (Secrets) in the left bar -> "
              "+ Add new secret -> name as above -> enable 'Notebook access'; then re-run cell 3")
    return ok


class RateLimiter:
    """Global, thread-safe spacing of ALL HTTP requests."""

    def __init__(self, per_min):
        self.interval = 60.0 / per_min
        self.lock = threading.Lock()
        self.next_t = 0.0

    def wait(self):
        with self.lock:
            t = max(time.monotonic(), self.next_t)
            self.next_t = t + self.interval
        delay = t - time.monotonic()
        if delay > 0:
            time.sleep(delay)


LIMITER = RateLimiter(MAX_REQUESTS_PER_MIN)


class LimitedSession(requests.Session):
    def request(self, *args, **kwargs):
        LIMITER.wait()
        return super().request(*args, **kwargs)


_local = threading.local()
API_KEY = None


def get_client():
    client = getattr(_local, "client", None)
    if client is None:
        client = EntsoePandasClient(api_key=API_KEY, session=LimitedSession(),
                                    retry_count=2, retry_delay=10, timeout=120)
        _local.client = client
    return client


# =============================================================================
#  HELPERS
# =============================================================================

def german_zone(ts):
    return "DE_AT_LU" if ts < ZONE_SWITCH else "DE_LU"


def const(code):
    return lambda ts: code


# Country profiles. Area lists are candidates per segment start, tried in
# order until one returns data. Neighbours: (column label, area function).
PROFILES = {
    "DE": dict(
        name="Germany",
        zone=german_zone,                            # prices, market data
        physical=lambda ts: ["DE", "DE_LU"],         # load/generation: never
                                                     # DE_AT_LU (would add AT)
        imbalance=lambda ts: ["DE_LU"],      # on ENTSO-E only until 2022-06-21!
                                             # -> official reBAP via netztransparenz
        # imbalance VOLUME is NOT uniform: German total = DE_LU, or the sum
        # of the four control areas. Never a single TSO (structural break).
        imbalance_volume=lambda ts: ["DE_LU", "SUM_DE_TSOS"],
        reserve=lambda ts: ["DE_LU", "DE"],
        # known structural zeros: column -> first day it is 0 (not missing)
        zero_after={"Gen_Nuclear": "2023-04-16"},     # nuclear phase-out
        price_neighbours=[(nb, const(nb)) for nb in
                          ["AT", "BE", "CH", "CZ", "DK_1", "DK_2",
                           "FR", "NL", "NO_2", "PL", "SE_4"]],
        flow_neighbours=[(nb, const(nb)) for nb in NEIGHBOURS["DE_LU"]],
        holidays="Germany",
    ),
    "BE": dict(
        name="Belgium",
        zone=const("BE"),
        physical=lambda ts: ["BE"],
        imbalance=lambda ts: ["BE"],                 # Elia
        imbalance_volume=lambda ts: ["BE"],
        reserve=lambda ts: ["BE"],
        zero_after={},
        # DE column = German bidding zone of that time (DE_AT_LU / DE_LU)
        price_neighbours=[("FR", const("FR")), ("NL", const("NL")),
                          ("DE", german_zone), ("GB", const("GB"))],
        # BE-DE: ALEGrO since late 2020; BE-GB: Nemo Link since 2019
        flow_neighbours=[("FR", const("FR")), ("NL", const("NL")),
                         ("DE", german_zone), ("GB", const("GB"))],
        holidays="Belgium",
    ),
}


def simple_profile(code, name, neighbours, holidays_name, **extra):
    """Country = one bidding zone = one control area (FR, NL, ES, PL ...).
    neighbours: list of labels; 'DE' maps to the German zone of the time."""
    nb = [(n, german_zone if n == "DE" else const(n)) for n in neighbours]
    prof = dict(name=name, zone=const(code), physical=lambda ts: [code],
                imbalance=lambda ts: [code], imbalance_volume=lambda ts: [code],
                reserve=lambda ts: [code], zero_after={},
                price_neighbours=nb, flow_neighbours=nb, holidays=holidays_name)
    prof.update(extra)
    return prof


def austria_zone(ts):
    # until 30.09.2018 Austria was part of the common DE-AT-LU price zone
    return "DE_AT_LU" if ts < ZONE_SWITCH else "AT"


def it_austria(ts):
    # seen from Italy, the Austrian border belonged to DE_AT_LU until 2018-10
    return "DE_AT_LU" if ts < ZONE_SWITCH else "AT"


PROFILES.update({
    "FR": simple_profile("FR", "France", ["BE", "CH", "DE", "ES", "GB", "IT_NORD"], "France"),
    "NL": simple_profile("NL", "Netherlands", ["BE", "DE", "GB", "NO_2", "DK_1"], "Netherlands"),
    "ES": simple_profile("ES", "Spain", ["FR", "PT"], "Spain"),
    # PL: ENTSO-E delivers PL day-ahead prices 2017-2019 in PLN -> harmonise_currency()
    "PL": simple_profile("PL", "Poland", ["CZ", "DE", "LT", "SE_4", "SK"], "Poland"),
    "AT": simple_profile(
        "AT", "Austria", ["CH", "CZ", "DE", "HU", "IT_NORD", "SI"], "Austria",
        zone=austria_zone,                         # price: DE_AT_LU until 2018-10
        flow_home=const("AT"),                     # flows always from Austria itself
        # before 2018-10 the "net position" would be the whole DE-AT-LU zone
        net_position=lambda ts: ["AT"] if ts >= ZONE_SWITCH else [],
        net_position_cache="Net_Position_v2",
    ),
    # Italy has several bidding zones and no national price on ENTSO-E.
    # P_DA = IT_NORD (where most energy-intensive industry is located);
    # all other zones are stored as P_DA_IT_<zone>. Load/generation: whole IT.
    # Flows: northern borders only (FR, CH, AT, SI).
    "IT": dict(
        name="Italy", zone=const("IT_NORD"), physical=lambda ts: ["IT"],
        imbalance=lambda ts: ["IT", "IT_NORD"], imbalance_volume=lambda ts: ["IT", "IT_NORD"],
        reserve=lambda ts: ["IT", "IT_NORD"], zero_after={},
        price_neighbours=[("FR", const("FR")), ("CH", const("CH")), ("AT", it_austria),
                          ("SI", const("SI")), ("GR", const("GR"))]
                         + [(z, const(z)) for z in ["IT_CNOR", "IT_CSUD", "IT_SUD",
                                                    "IT_SICI", "IT_SARD", "IT_CALA"]],
        flow_neighbours=[("FR", const("FR")), ("CH", const("CH")), ("AT", it_austria),
                         ("SI", const("SI"))],
        holidays="Italy",
        # Terna reports a large part of thermal (CHP) generation as "Other"
        # (72-92 TWh/yr until 2018). Assumption: emission factors of gas.
        other_ef=(400, 490),
    ),
})

NEIGHBOUR_SKIP = {"RU", "RU_KGD", "BY", "IT_NORD_CH", "IT_NORD_FR", "IT_NORD_AT",
                  "IT_NORD_SI", "IT_BRNN", "IT_GR"}


def auto_neighbours(code):
    """Neighbour labels from entsoe-py's map; DE_AT_LU/DE_LU -> 'DE'."""
    out = []
    for n in NEIGHBOURS.get(code, []):
        lab = "DE" if n in ("DE_AT_LU", "DE_LU") else n
        if lab not in NEIGHBOUR_SKIP and lab not in out:
            out.append(lab)
    return out


def zone_profile(code, name, holidays_name, **extra):
    # neighbour PRICES are not stored for these zones (every zone is its own
    # data set, so the neighbour's price is already in its own folder)
    return simple_profile(code, name, auto_neighbours(code), holidays_name,
                          neighbour_prices=False, **extra)


for _code, _name, _hol in [
    ("SE_1", "Sweden_SE1", "Sweden"), ("SE_2", "Sweden_SE2", "Sweden"),
    ("SE_3", "Sweden_SE3", "Sweden"), ("SE_4", "Sweden_SE4", "Sweden"),
    ("NO_1", "Norway_NO1", "Norway"), ("NO_2", "Norway_NO2", "Norway"),
    ("NO_3", "Norway_NO3", "Norway"), ("NO_4", "Norway_NO4", "Norway"),
    ("NO_5", "Norway_NO5", "Norway"), ("DK_1", "Denmark_DK1", "Denmark"),
    ("DK_2", "Denmark_DK2", "Denmark"), ("FI", "Finland", "Finland"),
    ("CZ", "Czechia", "Czechia"), ("SK", "Slovakia", "Slovakia"),
    ("HU", "Hungary", "Hungary"), ("RO", "Romania", "Romania"),
    ("SI", "Slovenia", "Slovenia"), ("HR", "Croatia", "Croatia"),
    ("BG", "Bulgaria", "Bulgaria"), ("PT", "Portugal", "Portugal"),
    ("EE", "Estonia", "Estonia"), ("LV", "Latvia", "Latvia"),
    ("LT", "Lithuania", "Lithuania"), ("GR", "Greece", "Greece"),
    ("IE_SEM", "Ireland_SEM", "Ireland"), ("CH", "Switzerland", "Switzerland"),
]:
    PROFILES[_code] = zone_profile(_code, _name, _hol)

# Türkiye: not on ENTSO-E in usable form -> EPİAŞ only, local time Istanbul
PROFILES["TR"] = dict(
    name="Turkey", zone=const("TR"), physical=lambda ts: [], imbalance=lambda ts: [],
    imbalance_volume=lambda ts: [], reserve=lambda ts: [], zero_after={},
    price_neighbours=[], flow_neighbours=[], holidays="Turkey",
    entsoe=False, tz="Europe/Istanbul",
)

P = PROFILES[COUNTRY]                    # (re)set by configure() per country
DATASET_NAME = f"{PROJECT_NAME}_{COUNTRY}"  # file prefix, set by configure()
DRIVE_FOLDER = None                         # zone folder, set by configure()
LOCAL_FOLDER = None


def AREAS_ZONE(ts):
    return [P["zone"](ts)]


def AREAS_PHYSICAL(ts):
    return P["physical"](ts)


def AREAS_IMBALANCE(ts):
    return P["imbalance"](ts)


def clean(name):
    s = str(name)
    for a, b in [(" ", "_"), ("/", "_"), ("-", "_"), ("(", ""), (")", ""),
                 (",", ""), (".", "")]:
        s = s.replace(a, b)
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


def as_frame(obj, name=None):
    if obj is None:
        return None
    if isinstance(obj, pd.Series):
        return obj.to_frame(name or obj.name or "value")
    return obj


def flatten(df, prefix=""):
    df = as_frame(df)
    if df is None:
        return None
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        cols = [" ".join(str(x) for x in c if x not in (None, "")) for c in df.columns]
    else:
        cols = [str(c) for c in df.columns]
    df.columns = [prefix + clean(c) for c in cols]
    return df.loc[:, ~df.columns.duplicated()]


def pick_res(df, suffix):
    """Solar / wind on- / offshore columns -> standard names."""
    if df is None:
        return None
    out = {}
    for kw in ["Solar", "Wind_Onshore", "Wind_Offshore"]:
        match = [c for c in df.columns if kw in c]
        if match:
            out[kw + suffix] = df[match[0]]
    return pd.DataFrame(out) if out else None


def single(series_or_df, col):
    df = as_frame(series_or_df)
    if df is None or df.empty:
        return None
    return df.iloc[:, [0]].set_axis([col], axis=1)


# =============================================================================
#  FETCH FUNCTIONS   fn(client, area, start, end) -> DataFrame | None
# =============================================================================

def make_price_fn(col):
    def fn(c, area, s, e):
        return single(c.query_day_ahead_prices(area, start=s, end=e), col)
    return fn


def f_ida1(c, area, s, e):
    # Pan-European intraday auction IDA1 (exists since mid-2024)
    return single(c.query_intraday_prices(area, start=s, end=e, sequence=1), "P_IDA1")


IDA_START = pd.Timestamp("2024-06-13", tz="Europe/Berlin")   # SIDC intraday auctions go-live
IDA_MONTH = IDA_START.replace(day=1)                         # segments are months
BAL_START = pd.Timestamp("2019-01-01", tz="Europe/Berlin")   # balancing energy data used from


def _areas_from(start):
    return lambda ts: AREAS_ZONE(ts) if ts >= start else []


def make_ida_fn(seq):
    def fn(c, area, s, e):
        return single(c.query_intraday_prices(area, start=s, end=e, sequence=seq), f"P_IDA{seq}")
    return fn


BAL_TYPES = {"aFRR": "A96", "mFRR": "A97"}
# Activated balancing VOLUMES [17.1.E, A83] are no longer served by the API
# ("combination ... is not valid", checked 08.11.2026 for 16 variants) -> not used.
# Balancing energy prices of these zones are not used (implausible on ENTSO-E:
# CH mFRR median ~2700 EUR/MWh in 2021, zeros in 2022, nothing afterwards).
BAL_EXCLUDE = {"CH"}
# ISO 4217 numeric codes: the currency of each balancing price series is read
# from the XML (currency_Unit.name) and stored next to the price.
ISO_NUM = {"EUR": 978, "RON": 946, "HUF": 348, "CZK": 203, "PLN": 985, "BGN": 975, "CHF": 756,
           "SEK": 752, "NOK": 578, "DKK": 208, "HRK": 191, "GBP": 826, "TRY": 949}
NUM_ISO = {v: k for k, v in ISO_NUM.items()}
# raw files without a currency column (older downloads): checked on the XML
# (08.11.2026, 2019-2026): RO always RON, HU always HUF
BALE_CURRENCY_FALLBACK = {"RO": "RON", "HU": "HUF"}
# zones whose balancing price files without currency column are fetched ONCE more to read the
# currency (unclear on the data); Nordic zones report EUR, RO/HU are checked above
BALE_CURRENCY_MIGRATE = {"PL", "BG", "HR", "CZ"}


def _to_quarter(df):
    """Finer than 15 min (some TSOs publish per minute) -> 15-min means."""
    if df is None or len(df) < 3:
        return df
    step = pd.Series(df.index).diff().median()
    return df.resample("15min").mean() if step < pd.Timedelta("15min") else df


_DIR = {"A01": "Up", "A02": "Down", "A03": "Sym"}


def _iso_step(res):
    m = re.fullmatch(r"PT(\d+)M", res or "") or re.fullmatch(r"PT(\d+)H", res or "")
    if m:
        return pd.Timedelta(minutes=int(m.group(1))) if res.endswith("M") else pd.Timedelta(hours=int(m.group(1)))
    return pd.Timedelta("1D") if res == "P1D" else None


def _tag(block, name):
    m = re.search(rf"<{name}>\s*([^<]*?)\s*</{name}>", block)
    return m.group(1) if m else None


def _bal_price_frame(xml, product):
    """XML of 17.1.F -> P_BalE_<product>_<Up|Down|Sym> + Cur_BalE_<product> (ISO numeric).
    Own tolerant parser: every <Period> of every TimeSeries, points without a price
    are skipped (not the whole series), curveType A03 repeats a price until the next
    point. Series in another currency than the majority of the document are dropped."""
    xml = xml.decode("utf-8", "replace") if isinstance(xml, bytes) else xml
    rows, skipped = [], 0
    for block in re.findall(r"<TimeSeries>(.*?)</TimeSeries>", xml, re.S):
        direction = _DIR.get(_tag(block, "flowDirection.direction"))
        cur = _tag(block, "currency_Unit.name") or "EUR"
        a03 = _tag(block, "curveType") == "A03"
        if direction is None:
            skipped += 1
            continue
        for period in re.findall(r"<Period>(.*?)</Period>", block, re.S):
            start, end = _tag(period, "start"), _tag(period, "end")
            step = _iso_step(_tag(period, "resolution"))
            if not (start and end and step):
                skipped += 1
                continue
            grid = pd.date_range(pd.Timestamp(start), pd.Timestamp(end), freq=step, inclusive="left")
            vals = pd.Series(np.nan, index=grid)
            for pt in re.findall(r"<Point>(.*?)</Point>", period, re.S):
                pos, price = _tag(pt, "position"), _tag(pt, "activation_Price.amount")
                try:
                    vals.iloc[int(pos) - 1] = float(price)
                except (TypeError, ValueError, IndexError):
                    continue
            if a03:
                vals = vals.ffill()
            vals = vals.dropna()
            if len(vals):
                rows.append(pd.DataFrame({"Price": vals.values, "Direction": direction, "Currency": cur},
                                         index=vals.index))
    if skipped:
        print(f"   note: {skipped} balancing price series/periods without direction or time axis skipped")
    if not rows:
        return None
    df = pd.concat(rows)
    main = df["Currency"].value_counts().idxmax()
    df = df[df["Currency"] == main]
    out = df.pivot_table(index=df.index, columns="Direction", values="Price", aggfunc="mean")
    out.columns = [f"P_BalE_{product}_{d}" for d in out.columns]
    out[f"Cur_BalE_{product}"] = float(ISO_NUM.get(main, np.nan))
    out.index.name = "timestamp"
    return _to_quarter(out)


def make_bal_price_fn(product):
    """Prices of activated balancing energy [17.1.F], up/down, in the currency
    the TSO reports (read from the XML, stored as Cur_BalE_<product>); converted
    to EUR/MWh when the tables are built. Several series per direction are averaged."""
    def fn(c, area, s, e):
        if isinstance(c, EntsoeRawClient):
            xml = EntsoeRawClient.query_activated_balancing_energy_prices(
                c, area, start=s, end=e, business_type=BAL_TYPES[product])
            return _bal_price_frame(xml, product)
        df = c.query_activated_balancing_energy_prices(area, start=s, end=e,   # test doubles
                                                       business_type=BAL_TYPES[product])
        if df is None or df.empty:
            return None
        df = df.copy()
        df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
        out = df.pivot_table(index=df.index, columns="Direction", values="Price", aggfunc="mean")
        out.columns = [f"P_BalE_{product}_{d}" for d in out.columns]
        out.index.name = "timestamp"
        return _to_quarter(out)
    return fn


def f_generation(c, area, s, e):
    g = c.query_generation(area, start=s, end=e)
    if g is None or g.empty:
        return None
    if not isinstance(g.columns, pd.MultiIndex) and any(isinstance(c, tuple) for c in g.columns):
        g = g.copy()
        g.columns = pd.MultiIndex.from_tuples([c if isinstance(c, tuple) else (c, "Actual Aggregated")
                                               for c in g.columns])
    if isinstance(g.columns, pd.MultiIndex):
        cols = []
        for col in g.columns:
            suffix = "_Consumption" if "Consumption" in str(col[-1]) else ""
            cols.append("Gen_" + clean(col[0]) + suffix)
        g = g.copy()
        g.columns = cols
        return g.loc[:, ~g.columns.duplicated()]
    return flatten(g, "Gen_")


def f_gen_forecast(c, area, s, e):
    try:
        return single(c.query_generation_forecast(area, start=s, end=e), "Gen_Forecast_DA_Total")
    except AttributeError:
        # entsoe-py crashes on some months (single-value periods); fetch day by
        # day and skip only the days that cannot be parsed
        parts = []
        for d in pd.date_range(s, e, freq="D", inclusive="left"):
            try:
                parts.append(single(c.query_generation_forecast(area, start=d, end=min(d + pd.Timedelta(days=1), e)),
                                    "Gen_Forecast_DA_Total"))
            except (AttributeError, NoMatchingDataError):
                continue
        parts = [x for x in parts if x is not None and len(x)]
        if not parts:
            raise NoMatchingDataError
        out = pd.concat(parts)
        return out[~out.index.duplicated(keep="first")].sort_index()


def f_load(c, area, s, e):
    return single(c.query_load(area, start=s, end=e), "Load_Actual")


def f_load_forecast(c, area, s, e):
    return single(c.query_load_forecast(area, start=s, end=e), "Load_Forecast_DA")


def f_res_forecast_da(c, area, s, e):
    return pick_res(flatten(c.query_wind_and_solar_forecast(area, start=s, end=e)), "_Forecast_DA")


def f_res_forecast_id(c, area, s, e):
    return pick_res(flatten(c.query_intraday_wind_and_solar_forecast(area, start=s, end=e)), "_Forecast_ID")


def f_imb_prices(c, area, s, e):
    # columns: Long / Short (identical in DE: single reBAP), sometimes
    # "Price for Consumption"
    return flatten(c.query_imbalance_prices(area, start=s, end=e), "P_Imb_")


DE_TSOS = ["DE_50HZ", "DE_AMPRION", "DE_TENNET", "DE_TRANSNET"]


def f_imb_volume_total(c, area, s, e):
    """Signed system imbalance of the whole country (MW-average per ISP).
    'SUM_DE_TSOS' = sum of the four German control areas (all four needed)."""
    if area == "SUM_DE_TSOS":
        parts = [single(c.query_imbalance_volumes(t, start=s, end=e), t) for t in DE_TSOS]
        if any(p is None for p in parts):
            return None
        df = pd.concat(parts, axis=1).sum(axis=1, min_count=len(DE_TSOS)).dropna()
        return df.to_frame("Imb_Volume_Total")
    return single(c.query_imbalance_volumes(area, start=s, end=e), "Imb_Volume_Total")


def f_net_position(c, area, s, e):
    return single(c.query_net_position(area, start=s, end=e, dayahead=True), "NetPosition_DA")


def make_flow_fn(label, export):
    col = f"Flow_{'Export' if export else 'Import'}_{label}"

    def fn(c, area, s, e):
        home, neighbour = area
        a_from, a_to = (home, neighbour) if export else (neighbour, home)
        return single(c.query_crossborder_flows(a_from, a_to, start=s, end=e,
                                                lookup_bzones=True), col)
    return fn


def flow_areas(nb_fn):
    # only borders that exist in the ENTSO-E neighbour map for that period
    def fn(ts):
        home, nb = P.get("flow_home", P["zone"])(ts), nb_fn(ts)
        return [(home, nb)] if nb in NEIGHBOURS.get(home, []) else []
    return fn


def f_capacity(c, area, s, e):
    return flatten(c.query_installed_generation_capacity(area, start=s, end=e), "Cap_")


RES_DELTA = {"PT15M": "15min", "PT30M": "30min", "PT60M": "1h", "PT1H": "1h",
             "PT4H": "4h", "P1D": "1D", "P7D": "7D"}
DIRECTION = {"A01": "Up", "A02": "Down", "A03": "Sym"}


def parse_reserve_zip(content, label):
    """Tolerant parser for contracted reserves (A81/B95). entsoe-py's own
    parser crashes when a point has no price; here such points just get NaN.
    Output: per timestamp and direction the mean price and summed volume."""
    rows = []
    with zipfile.ZipFile(io.BytesIO(content)) as arc:
        n_xml = 0
        for info in arc.infolist():
            if not info.filename.endswith("xml"):
                continue
            n_xml += 1
            soup = bs4.BeautifulSoup(arc.read(info), "html.parser")
            for ts in soup.find_all("timeseries"):
                d = ts.find("flowdirection.direction")
                direction = DIRECTION.get(d.text if d else "A03", "Sym")
                ct = ts.find("curvetype")
                a03 = ct is not None and ct.text == "A03"   # value holds until next point
                for period in ts.find_all("period"):
                    start = pd.Timestamp(period.find("start").text)
                    end = pd.Timestamp(period.find("end").text)
                    res = period.find("resolution").text
                    delta = pd.Timedelta(RES_DELTA.get(res, "1h"))
                    pts = {}
                    for pt in period.find_all("point"):
                        pos = pt.find("position")
                        if pos is None:
                            continue
                        price = pt.find("procurement_price.amount")
                        qty = pt.find("quantity")
                        pts[int(pos.text)] = (float(price.text) if price else np.nan,
                                              float(qty.text) if qty else np.nan)
                    if not pts:
                        continue
                    n = int((end - start) / delta) if a03 else max(pts)
                    last = None
                    for k in range(1, n + 1):
                        if k in pts:
                            last = pts[k]
                        elif not a03:
                            continue
                        if last is not None:
                            rows.append((start + (k - 1) * delta, direction, *last))
    if not rows:
        return None, n_xml
    df = pd.DataFrame(rows, columns=["t", "dir", "price", "qty"])
    df = df.groupby(["t", "dir"]).agg(price=("price", "mean"), qty=("qty", "sum")).unstack("dir")
    df.columns = [f"Res_{label}_{d}_{'Price' if m == 'price' else 'Volume_MW'}" for m, d in df.columns]
    df.index = pd.DatetimeIndex(df.index).tz_convert(TZ)
    return df.sort_index(), n_xml


def make_reserve_fn(process_type, label):
    def fn(c, area, s, e):
        parts, offset = [], 0
        while offset <= 1000:                      # paginate (100 docs/page)
            try:
                # call the RAW method: the pandas client wraps it with its own
                # pagination decorator (which clashes with our offset)
                content = EntsoeRawClient.query_contracted_reserve_prices_procured_capacity(
                    c, area, start=s, end=e, process_type=process_type,
                    type_marketagreement_type="A01", offset=offset)
            except NoMatchingDataError:
                break
            df, n_xml = parse_reserve_zip(content, label)
            if df is not None:
                parts.append(df)
            if n_xml < 100:
                break
            offset += 100
        if not parts:
            return None
        out = pd.concat(parts)
        return out.groupby(level=0).first()
    return fn


# =============================================================================
#  NETZTRANSPARENZ.DE  (German TSOs' own data service, OAuth2 client credentials)
# =============================================================================
NTP_TOKEN_URL = "https://identity.netztransparenz.de/users/connect/token"
NTP_BASE = "https://ds.netztransparenz.de/api/v1/data"
NTP_LIMITER = RateLimiter(50)              # API allows 2 req/s per IP; stay well below
_ntp = {"token": None, "exp": 0.0}
_ntp_lock = threading.Lock()


def ntp_token():
    if not (NTP_CLIENT_ID and NTP_CLIENT_SECRET):
        raise RuntimeError("netztransparenz credentials missing (Colab secrets "
                           "NTP_CLIENT_ID / NTP_CLIENT_SECRET), see credentials_check()")
    with _ntp_lock:
        if _ntp["token"] and time.time() < _ntp["exp"]:
            return _ntp["token"]
        r = requests.post(NTP_TOKEN_URL, data={"grant_type": "client_credentials",
                                               "client_id": NTP_CLIENT_ID,
                                               "client_secret": NTP_CLIENT_SECRET},
                          timeout=60)
        r.raise_for_status()
        js = r.json()
        _ntp["token"] = js["access_token"]
        _ntp["exp"] = time.time() + int(js.get("expires_in", 3600)) - 300
        return _ntp["token"]


def parse_ntp_csv(text, prefix):
    """netztransparenz CSV: ';' separated, decimal comma, Datum dd.mm.yyyy,
    von HH:MM, Zeitzone (UTC). Returns numeric columns with UTC index."""
    df = pd.read_csv(io.StringIO(text.lstrip("\ufeff")), sep=";", decimal=",",
                     dtype=str, keep_default_na=False)
    df.columns = [str(c).strip() for c in df.columns]
    # headers vary by product: "Datum;Zeitzone;von;bis;..." or
    # "Datum von;(Uhrzeit) von;Zeitzone;(Uhrzeit) bis;Zeitzone;ID AEP in €/MWh"
    date_col = next((c for c in df.columns if c.startswith("Datum")), None)
    time_col = next((c for c in df.columns if c != date_col and c.endswith("von")), None)
    zone_cols = [c for c in df.columns if c.startswith("Zeitzone")]
    if df.empty or date_col is None or time_col is None:
        return None
    zone = df[zone_cols[0]].iloc[0].strip().upper() if zone_cols else "UTC"
    stamp = df[date_col].str.strip() + " " + df[time_col].str.strip()
    t = None
    for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            t = pd.to_datetime(stamp, format=fmt)
            break
        except (ValueError, TypeError):
            continue
    if t is None:                                    # e.g. IdAep: ISO date
        t = pd.to_datetime(stamp, format="mixed", dayfirst=True)
    if zone == "UTC":
        t = t.dt.tz_localize("UTC")
    else:                                            # fall back to local time
        t = t.dt.tz_localize(TZ, ambiguous="infer", nonexistent="shift_forward")
    skip = {date_col, time_col, *zone_cols, "Datenkategorie", "Datentyp", "Einheit"}
    out = {}
    for c in df.columns:
        if c in skip or c.endswith("bis") or c.split(".")[0] in zone_cols:
            continue
        col = df[c].str.strip()
        if col.str.contains(",", regex=False).any():     # German: 1.234,56
            col = col.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        v = pd.to_numeric(col, errors="coerce")           # else: 1234.56
        if v.notna().any():
            out[prefix + clean(c.replace("ü", "ue").replace("ö", "oe").replace("ä", "ae")
                               .replace("€", "EUR"))] = v.values
    if not out:
        return None
    res = pd.DataFrame(out, index=pd.DatetimeIndex(t, name="timestamp"))
    res = res[~res.index.duplicated(keep="first")].sort_index()
    return res.tz_convert(TZ)


def ntp_get(path, a, b):
    headers = {"Authorization": f"Bearer {ntp_token()}"}
    NTP_LIMITER.wait()
    r = requests.get(f"{NTP_BASE}/{path}/{a}/{b}", headers=headers, timeout=120)
    if r.status_code in (400, 404):              # try query-parameter variant
                                                 # (e.g. IdAep only accepts ?dateFrom=&dateTo=)
        NTP_LIMITER.wait()
        r = requests.get(f"{NTP_BASE}/{path}", headers=headers, timeout=120,
                         params={"dateFrom": a, "dateTo": b})
    return r


def make_ntp_fn(path, prefix, fallback=None, single_name=None):
    """path = quality-assured product. fallback = operational ('Betrieblich')
    product, used only when the quality-assured file has no data for that
    month; such rows get <prefix>Operational = 1."""
    def fn(c, area, s, e):
        a = s.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S")
        b = e.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S")
        for pth, operational in [(path, 0.0)] + ([(fallback, 1.0)] if fallback else []):
            r = ntp_get(pth, a, b)
            if r.status_code == 204 or not r.text.strip():
                continue
            if operational and r.status_code in (400, 404):
                continue
            r.raise_for_status()
            df = parse_ntp_csv(r.text, prefix)
            if df is not None and len(df) and single_name and df.shape[1] == 1:
                df.columns = [single_name]
            if df is not None and len(df):
                if fallback:
                    df[prefix + "Operational"] = operational
                return df
        return None
    return fn


# =============================================================================
#  ELIA OPEN DATA  (Belgian TSO, OpenDataSoft API, no key)
# =============================================================================
ELIA_URL = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/{}/exports/json"
ELIA_LIMITER = RateLimiter(30)
ELIA_MARI = pd.Timestamp("2024-05-22", tz=TZ)   # ods047 until 21.05.2024, ods134 after
ELIA_SKIP = {"resolutioncode", "qualitystatus"}


def elia_query(dataset, s, e):
    """All rows of one Elia dataset between s and e (one day margin on both
    sides; the exact cut to [s, e) happens later)."""
    a = (s - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    b = (e + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    ELIA_LIMITER.wait()
    r = requests.get(ELIA_URL.format(dataset), timeout=180, params={
        "where": f"datetime >= date'{a}' AND datetime < date'{b}'",
        "timezone": "UTC"})
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return None
    df = pd.DataFrame(rows)
    if "datetime" not in df.columns:
        return None
    if "resolutioncode" in df.columns and (df["resolutioncode"] == "PT15M").any():
        df = df[df["resolutioncode"] == "PT15M"]
    idx = pd.DatetimeIndex(pd.to_datetime(df["datetime"], utc=True), name="timestamp")
    out = {}
    for col in df.columns:
        if col == "datetime" or col in ELIA_SKIP:
            continue
        v = pd.to_numeric(df[col], errors="coerce")
        if v.notna().any():
            out["Elia_" + clean(col)] = v.values
    if not out:
        return None
    res = pd.DataFrame(out, index=idx)
    return res[~res.index.duplicated(keep="last")].sort_index().tz_convert(TZ)


def f_elia_imbalance(c, area, s, e):
    """Imbalance price, system imbalance, alpha, MIP/MDP ... (all numeric
    fields). Before/after the MARI go-live Elia uses two datasets."""
    parts = []
    if s < ELIA_MARI:
        parts.append(elia_query("ods047", s, min(e, ELIA_MARI)))
    if e > ELIA_MARI:
        parts.append(elia_query("ods134", max(s, ELIA_MARI), e))
    parts = [p for p in parts if p is not None]
    if not parts:
        return None
    df = pd.concat(parts)
    return df[~df.index.duplicated(keep="last")].sort_index()


# =============================================================================
#  REGELLEISTUNG.NET  (German balancing capacity auctions; EXPERIMENTAL)
# =============================================================================
#  Uses the download interface behind the public "Datencenter" page. It is
#  not an officially documented API and may change. One file per day/product.
RL_URL = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/resultsoverview"
RL_LIMITER = RateLimiter(120)
RL_DIR = {"NEGPOS": "Sym", "POSNEG": "Sym", "POS": "Up", "NEG": "Down"}
# parameter spellings seen on the site; the first one that works is remembered
RL_PRODUCT_ALIASES = {"FCR": ["FCR", "PRL"], "aFRR": ["aFRR", "SRL"], "mFRR": ["mFRR", "MRL"]}
RL_MARKETS = ["CAPACITY", "BALANCING_CAPACITY"]
RL_WORKING = {}
RL_LOCK = threading.Lock()


def rl_get(product, day):
    """GET one day of results; tries the known parameter variants once and
    remembers the one that works."""
    with RL_LOCK:
        known = RL_WORKING.get(product)
    variants = [known] if known else [(p, m) for p in RL_PRODUCT_ALIASES[product] for m in RL_MARKETS]
    last = None
    for prod, market in variants:
        RL_LIMITER.wait()
        r = requests.get(RL_URL, timeout=120, params={
            "date": day.strftime("%Y-%m-%d"), "exportFormat": "xlsx",
            "market": market, "productTypes": prod})
        if r.status_code in (400, 404, 422) and not known:
            last = r
            continue
        if r.status_code == 200 and len(r.content) > 0 and not known:
            with RL_LOCK:
                RL_WORKING[product] = (prod, market)
        return r
    return last


def parse_rl_product(product):
    """'NEGPOS_00_04' -> ('Sym', 0); 'POS_04_08' -> ('Up', 4);
    unknown formats -> (prefix, None)."""
    parts = str(product).strip().upper().split("_")
    direction = RL_DIR.get(parts[0], clean(parts[0]))
    hour = None
    if len(parts) >= 2 and parts[1].isdigit():
        h = int(parts[1])
        hour = h if h < 24 else None
    return direction, hour


def parse_rl_xlsx(content, label):
    df = pd.read_excel(io.BytesIO(content), sheet_name=0)
    if df.empty:
        return None
    cols = {c: str(c).strip() for c in df.columns}
    df = df.rename(columns=cols)
    date_col = next((c for c in df.columns if c.upper().startswith("DATE_FROM")), None) \
        or next((c for c in df.columns if "DATE" in c.upper()), None)
    # the product column is the one whose values look like NEGPOS_00_04 / POS_04_08
    pat = r"^(NEGPOS|POSNEG|NEG|POS)_\d"
    cands = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    score = {c: df[c].astype(str).str.upper().str.match(pat).mean() for c in cands}
    prod_col = max(score, key=score.get) if score and max(score.values()) > 0.5 else \
        next((c for c in df.columns if "PRODUCT" in c.upper()), None)
    if date_col is None or prod_col is None:
        raise ValueError(f"unexpected regelleistung columns: {list(df.columns)[:10]}")
    day = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
    parsed = df[prod_col].map(parse_rl_product)
    direction = parsed.map(lambda x: x[0])
    hour = parsed.map(lambda x: 0 if x[1] is None else int(x[1])).astype(int)
    t = (day.dt.normalize() + pd.to_timedelta(hour, unit="h")).dt.tz_localize(
        TZ, ambiguous="NaT", nonexistent="shift_forward")
    num = {}
    for col in df.columns:
        if col in (date_col, prod_col):
            continue
        v = pd.to_numeric(df[col].astype(str).str.replace(",", ".", regex=False)
                          .str.replace(r"[^0-9.\-eE]", "", regex=True), errors="coerce")
        if v.notna().mean() > 0.5:
            num[clean(col.upper().replace("[", "").replace("]", ""))] = v
    if not num:
        return None
    long = pd.DataFrame(num)
    # if the start hour could not be read, keep the product name as key so
    # several products of one day are not collapsed into one row
    key = direction.where(parsed.map(lambda x: x[1] is not None),
                          df[prod_col].astype(str).map(clean))
    long["t"], long["dir"] = t, key                # same row index -> keeps tz
    long = long.dropna(subset=["t"])
    wide = long.groupby(["t", "dir"]).first().unstack("dir")
    wide.columns = [f"RL_{label}_{d}_{m}" for m, d in wide.columns]
    wide.index = pd.DatetimeIndex(wide.index, name="timestamp")
    return wide.sort_index()


def make_rl_fn(product, label):
    def fn(c, area, s, e):
        first = max(s, pd.Timestamp(REGELLEISTUNG_START, tz=TZ))
        days = pd.date_range(first.normalize(), e, freq="1D", inclusive="left")
        parts = []
        for d in days:
            r = rl_get(product, d)
            if r.status_code == 204 or len(r.content) == 0:
                continue
            r.raise_for_status()
            df = parse_rl_xlsx(r.content, label)
            if df is not None:
                parts.append(df)
        if not parts:
            return None
        out = pd.concat(parts)
        return out[~out.index.duplicated(keep="last")].sort_index()
    return fn


def rl_areas(ts_end_exclusive=None):
    return lambda ts: ["RL"] if ts >= pd.Timestamp(REGELLEISTUNG_START, tz=TZ).replace(day=1) else []


# =============================================================================
#  WEATHER  (Open-Meteo historical ERA5 API, no key, CC BY 4.0)
# =============================================================================
#  Population-weighted average over main cities of each country / zone
#  (approximate metro populations in millions). Hourly, UTC.
WX_URL = "https://archive-api.open-meteo.com/v1/archive"
WX_VARS = {"temperature_2m": "WX_Temp_C", "shortwave_radiation": "WX_GHI_Wm2",
           "wind_speed_100m": "WX_Wind100m_kmh", "cloud_cover": "WX_Cloud_pct"}
WX_LIMITER = RateLimiter(30)
WX_POINTS = {
    "DE": [(52.52, 13.40, 4.5), (53.55, 10.00, 3.0), (48.14, 11.58, 2.9), (51.45, 7.00, 10.0),
           (50.11, 8.68, 2.7), (48.78, 9.18, 2.8)],
    "BE": [(50.85, 4.35, 2.1), (51.22, 4.40, 1.1), (51.05, 3.72, 0.6), (50.63, 5.57, 0.75), (50.41, 4.44, 0.5)],
    "FR": [(48.86, 2.35, 12.3), (45.76, 4.84, 2.3), (43.30, 5.37, 1.9), (43.60, 1.44, 1.4), (50.63, 3.06, 1.2),
           (44.84, -0.58, 1.3), (47.22, -1.55, 1.0), (48.57, 7.75, 0.8)],
    "NL": [(52.37, 4.90, 2.5), (51.92, 4.48, 1.8), (52.08, 4.30, 1.0), (52.09, 5.12, 0.9), (51.44, 5.47, 0.8),
           (53.22, 6.57, 0.5)],
    "AT": [(48.21, 16.37, 2.9), (47.07, 15.44, 0.65), (48.31, 14.29, 0.8), (47.81, 13.04, 0.4), (47.27, 11.39, 0.3)],
    "PL": [(52.23, 21.01, 3.1), (50.26, 19.02, 2.7), (50.06, 19.94, 1.5), (51.76, 19.46, 1.1), (51.11, 17.04, 1.2),
           (52.41, 16.93, 1.0), (54.35, 18.65, 1.1)],
    "ES": [(40.42, -3.70, 6.7), (41.39, 2.17, 5.6), (39.47, -0.38, 2.5), (37.39, -5.98, 1.5), (43.26, -2.93, 1.0),
           (36.72, -4.42, 1.0), (41.65, -0.89, 0.8)],
    "IT": [(45.46, 9.19, 7.4), (41.90, 12.50, 4.3), (40.85, 14.27, 3.1), (45.07, 7.69, 2.2), (44.49, 11.34, 1.0),
           (43.77, 11.26, 1.0), (38.12, 13.36, 1.2), (41.12, 16.87, 1.2), (45.44, 12.33, 2.0)],
    "SE_1": [(65.58, 22.15, 0.08), (67.86, 20.23, 0.02)],
    "SE_2": [(62.39, 17.31, 0.10), (63.83, 20.26, 0.13), (63.18, 14.64, 0.06)],
    "SE_3": [(59.33, 18.07, 2.4), (57.71, 11.97, 1.1), (59.86, 17.64, 0.25), (59.27, 15.21, 0.16)],
    "SE_4": [(55.60, 13.00, 0.75), (56.05, 12.69, 0.15), (56.66, 16.36, 0.07)],
    "NO_1": [(59.91, 10.75, 1.5)], "NO_2": [(58.15, 8.00, 0.12), (58.97, 5.73, 0.33)],
    "NO_3": [(63.43, 10.40, 0.2)], "NO_4": [(69.65, 18.96, 0.08), (67.28, 14.40, 0.05)],
    "NO_5": [(60.39, 5.32, 0.42)],
    "DK_1": [(56.16, 10.20, 0.35), (57.05, 9.92, 0.2), (55.40, 10.39, 0.2)], "DK_2": [(55.68, 12.57, 1.4)],
    "FI": [(60.17, 24.94, 1.5), (61.50, 23.76, 0.4), (60.45, 22.27, 0.3), (65.01, 25.47, 0.2)],
    "CZ": [(50.08, 14.44, 2.2), (49.20, 16.61, 0.7), (49.82, 18.26, 1.0), (49.74, 13.37, 0.3)],
    "SK": [(48.15, 17.11, 0.7), (48.72, 21.26, 0.4), (49.22, 18.74, 0.2)],
    "HU": [(47.50, 19.04, 3.0), (47.53, 21.63, 0.2), (46.25, 20.15, 0.16), (47.69, 17.63, 0.13)],
    "RO": [(44.43, 26.10, 2.3), (46.77, 23.59, 0.4), (45.76, 21.23, 0.35), (47.16, 27.59, 0.4), (44.18, 28.65, 0.4)],
    "SI": [(46.06, 14.51, 0.5), (46.55, 15.65, 0.2)],
    "HR": [(45.81, 15.98, 1.1), (43.51, 16.44, 0.35), (45.33, 14.44, 0.2), (45.55, 18.69, 0.15)],
    "BG": [(42.70, 23.32, 1.5), (42.14, 24.75, 0.5), (43.21, 27.91, 0.4)],
    "PT": [(38.72, -9.14, 2.9), (41.15, -8.61, 1.7), (41.55, -8.42, 0.2), (40.21, -8.43, 0.15)],
    "EE": [(59.44, 24.75, 0.6), (58.38, 26.72, 0.1)], "LV": [(56.95, 24.11, 0.9), (55.87, 26.54, 0.08)],
    "LT": [(54.69, 25.28, 0.8), (54.90, 23.90, 0.4), (55.71, 21.13, 0.15)],
    "GR": [(37.98, 23.73, 3.6), (40.64, 22.94, 1.0), (38.25, 21.73, 0.2), (35.34, 25.13, 0.2)],
    "IE_SEM": [(53.35, -6.26, 1.9), (54.60, -5.93, 0.6), (51.90, -8.47, 0.3), (53.27, -9.05, 0.1)],
    "CH": [(47.37, 8.54, 1.4), (46.20, 6.14, 0.6), (47.56, 7.59, 0.5), (46.95, 7.45, 0.4), (46.52, 6.63, 0.4)],
    "TR": [(41.01, 28.98, 15.6), (39.93, 32.86, 5.7), (38.42, 27.14, 4.4), (40.19, 29.06, 3.1),
           (36.90, 30.70, 2.6), (37.00, 35.32, 2.3), (37.87, 32.48, 2.3), (37.07, 37.38, 2.1),
           (40.77, 29.92, 2.1)],
}


def f_weather(c, area, s, e):
    pts = WX_POINTS.get(COUNTRY)
    if not pts:
        return None
    last = min(e - pd.Timedelta(days=1), pd.Timestamp.now(tz=TZ) - pd.Timedelta(days=6))
    if last < s:
        return None
    WX_LIMITER.wait()
    r = requests.get(WX_URL, timeout=180, params={
        "latitude": ",".join(str(p[0]) for p in pts),
        "longitude": ",".join(str(p[1]) for p in pts),
        "start_date": s.tz_convert("UTC").strftime("%Y-%m-%d"),            # API dates are UTC
        "end_date": (last + pd.Timedelta(days=1) - pd.Timedelta(hours=1)).tz_convert("UTC").strftime("%Y-%m-%d"),
        "hourly": ",".join(WX_VARS), "timezone": "GMT", "timeformat": "unixtime"})
    r.raise_for_status()
    js = r.json()
    js = js if isinstance(js, list) else [js]
    frames, weights = [], []
    for loc, (_, _, w) in zip(js, pts):
        h = loc.get("hourly") or {}
        if "time" not in h:
            continue
        idx = pd.to_datetime(h["time"], unit="s", utc=True)
        frames.append(pd.DataFrame({out: h.get(var) for var, out in WX_VARS.items()},
                                   index=idx).astype(float))
        weights.append(w)
    if not frames:
        return None
    cols = list(WX_VARS.values())
    num = sum(f[cols].fillna(0) * w for f, w in zip(frames, weights))
    den = sum(f[cols].notna() * w for f, w in zip(frames, weights))
    out = (num / den).where(den > 0)
    out.index.name = "timestamp"
    return out.tz_convert(TZ)


# =============================================================================
#  EUROSTAT  (nrg_pc_205: non-household electricity prices by consumption band,
#  semi-annual; nrg_pc_205_c: price components, annual)  – free, attribution
# =============================================================================
EUROSTAT_URL = ("https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{}"
                "?format=SDMX-CSV&compressed=false")
_EUROSTAT_CACHE = {}
_EUROSTAT_LOCK = threading.Lock()


def eurostat_geo():
    code = COUNTRY.split("_")[0]
    return {"GR": "EL", "IE": "IE"}.get(code, code)


def eurostat_table(dataset):
    with _EUROSTAT_LOCK:
        if dataset not in _EUROSTAT_CACHE:
            r = requests.get(EUROSTAT_URL.format(dataset), timeout=600)
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text), dtype=str)
            df.columns = [str(c).strip().lower() for c in df.columns]
            _EUROSTAT_CACHE[dataset] = df
        return _EUROSTAT_CACHE[dataset]


def eurostat_period(p):
    p = str(p).strip()
    if "-S" in p:
        y, h = p.split("-S")
        return pd.Timestamp(f"{y}-{'01' if h == '1' else '07'}-01", tz=TZ)
    return pd.Timestamp(f"{p[:4]}-01-01", tz=TZ)


def make_eurostat_fn(dataset, prefix, key_cols):
    def fn(c, area, s, e):
        df = eurostat_table(dataset)
        df = df[df["geo"] == eurostat_geo()]
        for col, val in [("currency", "EUR"), ("unit", "KWH"), ("product", "6000")]:
            if col in df.columns and (df[col] == val).any():
                df = df[df[col] == val]
        if df.empty:
            return None
        keys = [k for k in key_cols if k in df.columns]
        df = df.assign(t=df["time_period"].map(eurostat_period),
                       v=pd.to_numeric(df["obs_value"], errors="coerce"),
                       k=df[keys].astype(str).agg("_".join, axis=1))
        wide = df.pivot_table(index="t", columns="k", values="v", aggfunc="first")
        wide.columns = [prefix + clean(c) for c in wide.columns]
        wide.index = pd.DatetimeIndex(wide.index, name="timestamp")
        return wide.sort_index()
    return fn


# =============================================================================
#  FRED  (monthly IMF commodity prices + USD/EUR; no key needed for CSV)
# =============================================================================
FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"
FRED_SERIES = {"PNGASEUUSDM": "Gas_EU_USD_MMBtu", "PCOALAUUSDM": "Coal_AU_USD_t",
               "POILBREUSDM": "Brent_USD_bbl", "EXUSEU": "USD_per_EUR"}


def f_fred(c, area, s, e):
    r = requests.get(FRED_URL.format(",".join(FRED_SERIES)), timeout=120)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    t = pd.to_datetime(df.iloc[:, 0]).dt.tz_localize(TZ)
    out = pd.DataFrame({FRED_SERIES[c]: pd.to_numeric(df[c], errors="coerce")
                        for c in df.columns[1:] if c in FRED_SERIES}).set_index(t)
    out.index.name = "timestamp"
    if {"Gas_EU_USD_MMBtu", "USD_per_EUR"} <= set(out.columns):
        out["Gas_EU_EUR_MWh"] = out["Gas_EU_USD_MMBtu"] / out["USD_per_EUR"] * 3.412
    if {"Coal_AU_USD_t", "USD_per_EUR"} <= set(out.columns):
        out["Coal_AU_EUR_t"] = out["Coal_AU_USD_t"] / out["USD_per_EUR"]
    if {"Brent_USD_bbl", "USD_per_EUR"} <= set(out.columns):
        out["Brent_EUR_bbl"] = out["Brent_USD_bbl"] / out["USD_per_EUR"]
    return out[out.index >= START]


# =============================================================================
#  EPİAŞ  (Türkiye transparency platform, via the eptr2 package)
# =============================================================================
_EPIAS = {"client": None}
_EPIAS_LOCK = threading.Lock()
EPIAS_LIMITER = RateLimiter(60)
# real-time generation fields -> ENTSO-E-like names (sums where several map)
EPIAS_GEN_MAP = {
    "naturalgas": "Fossil_Gas", "lng": "Fossil_Gas", "lignite": "Fossil_Brown_coal_Lignite",
    "importcoal": "Fossil_Hard_coal", "blackcoal": "Fossil_Hard_coal",
    "asphaltitecoal": "Fossil_Hard_coal", "fueloil": "Fossil_Oil", "naphta": "Fossil_Oil",
    "wind": "Wind_Onshore", "sun": "Solar", "geothermal": "Geothermal", "biomass": "Biomass",
    "river": "Hydro_Run_of_river_and_pondage", "dammedhydro": "Hydro_Water_Reservoir",
    "wasteheat": "Other",
}
EPIAS_RENAME = {
    "mcp": {"priceeur": "P_DA", "price": "P_DA_TRY", "priceusd": "P_DA_USD"},
    "smp": {"systemmarginalprice": "P_SMF_TRY"},
    "wap": {"wap": "P_IDM_WAP_TRY"},
    "rt-cons": {"consumption": "Load_Actual"},
    "anc-pfk": {"price": "TR_FCR_Capacity_Price_TRY"},
    "anc-sfk": {"price": "TR_aFRR_Capacity_Price_TRY"},
    "anc-pf-qty": {"amount": "TR_FCR_Capacity_MW"},
    "anc-sf-qty": {"amount": "TR_aFRR_Capacity_MW"},
}


def epias_client():
    with _EPIAS_LOCK:
        if _EPIAS["client"] is None:
            from eptr2 import EPTR2
            _EPIAS["client"] = EPTR2(username=EPIAS_USERNAME, password=EPIAS_PASSWORD,
                                     recycle_tgt=True, use_dotenv=False)
        return _EPIAS["client"]


def make_epias_fn(key, prefix):
    def fn(c, area, s, e):
        EPIAS_LIMITER.wait()
        res = epias_client().call(key, start_date=s.strftime("%Y-%m-%d"),
                                  end_date=(e - pd.Timedelta(days=1)).strftime("%Y-%m-%d"))
        df = res if isinstance(res, pd.DataFrame) else pd.DataFrame(
            res.get("items", res) if isinstance(res, dict) else res)
        if df is None or df.empty:
            return None
        tcol = next((x for x in df.columns
                     if str(x).lower() in ("date", "datetime", "time", "period")), None)
        if tcol is None:
            raise ValueError(f"EPİAŞ {key}: no date column in {list(df.columns)[:8]}")
        idx = pd.DatetimeIndex(pd.to_datetime(df[tcol], utc=True), name="timestamp")
        out = {}
        for col in df.columns:
            if col == tcol:
                continue
            v = pd.to_numeric(df[col], errors="coerce")
            if not v.notna().any():
                continue
            low = str(col).lower()
            if low in ("hour", "period"):
                continue
            if key == "rt-gen":
                if low in ("total", "importexport"):
                    continue
                name = "Gen_" + EPIAS_GEN_MAP.get(low, "TR_" + clean(col))
                out[name] = out[name] + v.fillna(0).values if name in out else v.values
            else:
                out[EPIAS_RENAME.get(key, {}).get(low, prefix + clean(col))] = v.values
        if not out:
            return None
        res = pd.DataFrame(out, index=idx)
        return res[~res.index.duplicated(keep="first")].sort_index().tz_convert(TZ)
    return fn


# =============================================================================
#  DATASET REGISTRY
# =============================================================================
#  merge=True -> joined into the 15-min / 1-h master tables
#  merge=False -> only stored as raw table (different structure/frequency)

def D(name, fn, areas, group, unit, desc, merge=True, segment="month", cache=None):
    """cache: raw folder name; a new name forces a fresh download after a
    change in the fetch logic (clean_storage() removes the unused old folder)."""
    return dict(name=name, fn=fn, areas=areas, group=group, unit=unit,
                desc=desc, merge=merge, segment=segment, cache=cache or name)


def build_datasets():
  return [
    # ---- core -------------------------------------------------------------
    D("P_DA", make_price_fn("P_DA"), AREAS_ZONE, "core", "EUR/MWh",
      f"Day-ahead price {P['name']} (DE: DE_AT_LU before 2018-10); hourly, 15-min since 2025-10"),
    D("Generation", f_generation, AREAS_PHYSICAL, "core", "MW",
      "Actual generation per production type (all types, incl. pumped-storage consumption)"),
    D("Load", f_load, AREAS_PHYSICAL, "core", "MW", "Actual total load"),
    D("Load_Forecast", f_load_forecast, AREAS_PHYSICAL, "core", "MW", "Day-ahead load forecast"),
    D("RES_Forecast_DA", f_res_forecast_da, AREAS_PHYSICAL, "core", "MW",
      "Day-ahead wind & solar forecast"),
    D("Imbalance_Prices", f_imb_prices, AREAS_IMBALANCE, "core", "EUR/MWh",
      "Imbalance price (DE: reBAP, BE: Elia); Long/Short"),
    # ---- extended ---------------------------------------------------------
    *[D(f"P_DA_{lab}", make_price_fn(f"P_DA_{lab}"), (lambda ts, fn=fn: [fn(ts)]),
        "extended", "EUR/MWh", f"Day-ahead price {lab} (neighbour)")
      for lab, fn in P["price_neighbours"] if P.get("neighbour_prices", True)],
    D("P_IDA1", f_ida1, AREAS_ZONE, "extended", "EUR/MWh",
      "Intraday auction IDA1 price (from mid-2024)"),
    *[D(f"P_IDA{q}", make_ida_fn(q), (lambda ts: AREAS_ZONE(ts) if ts >= IDA_MONTH else []),
        "extended", "EUR/MWh", f"Intraday auction IDA{q} price (SIDC, from 13.06.2024)") for q in (2, 3)],
    *[D(f"BalE_Price_{prod}", make_bal_price_fn(prod), _areas_from(BAL_START), "extended", "EUR/MWh",
        f"Prices of activated balancing energy {prod} up/down [17.1.F], from 2019") for prod in BAL_TYPES
      if COUNTRY not in BAL_EXCLUDE],
    D("Gen_Forecast_DA", f_gen_forecast, AREAS_PHYSICAL, "extended", "MW",
      "Day-ahead total scheduled generation"),
    D("RES_Forecast_ID", f_res_forecast_id, AREAS_PHYSICAL, "extended", "MW",
      "Intraday wind & solar forecast (coverage varies)"),
    D("Imbalance_Volume_Total", f_imb_volume_total, P["imbalance_volume"], "extended", "MW",
      "Signed system imbalance of the whole country (DE: DE_LU or sum of 4 TSOs)"),
    D("Net_Position", f_net_position, P.get("net_position", AREAS_ZONE), "extended", "MW",
      "Day-ahead net position (positive = export)",
      cache=P.get("net_position_cache")),
    *[D(f"Flow_{d}_{lab}", make_flow_fn(lab, d == "Export"), flow_areas(fn),
        "extended", "MW", f"Physical flow {COUNTRY} {'->' if d == 'Export' else '<-'} {lab}")
      for lab, fn in P["flow_neighbours"] for d in ("Import", "Export")],
    D("Installed_Capacity", f_capacity, AREAS_PHYSICAL, "extended", "MW",
      "Installed capacity per production type (yearly)", merge=False, segment="year"),
    # ---- experimental (coverage for DE uncertain; raw only) ----------------
    # ---- netztransparenz.de (Germany only, needs NTP_CLIENT_ID/SECRET) ------
    *([D("NTP_reBAP", make_ntp_fn("NrvSaldo/reBAP/Qualitaetsgesichert", "P_"),
         lambda ts: ["NTP"], "core", "EUR/MWh",
         "Official reBAP (netztransparenz.de, quality-assured); unterdeckt/ueberdeckt"),
       D("NTP_NRV_Saldo", make_ntp_fn("NrvSaldo/NRVSaldo/Qualitaetsgesichert", "NRV_",
                                       fallback="NrvSaldo/NRVSaldo/Betrieblich"),
         lambda ts: ["NTP"], "extended", "MW",
         "Official German control-block balance (NRV-Saldo); operational data where "
         "quality-assured is missing (NRV_Operational = 1)", cache="NTP_NRV_Saldo_v2"),
       D("NTP_aFRR_Activated", make_ntp_fn("NrvSaldo/AktivierteSRL/Qualitaetsgesichert", "aFRR_Act_",
                                            fallback="NrvSaldo/AktivierteSRL/Betrieblich"),
         lambda ts: ["NTP"], "extended", "MW",
         "Activated aFRR energy, pos./neg.; operational fallback (aFRR_Act_Operational = 1)",
         cache="NTP_aFRR_Activated_v2"),
       D("NTP_mFRR_Activated", make_ntp_fn("NrvSaldo/AktivierteMRL/Qualitaetsgesichert", "mFRR_Act_",
                                            fallback="NrvSaldo/AktivierteMRL/Betrieblich"),
         lambda ts: ["NTP"], "extended", "MW",
         "Activated mFRR energy, pos./neg.; operational fallback (mFRR_Act_Operational = 1)",
         cache="NTP_mFRR_Activated_v2"),
       D("NTP_FCR", make_ntp_fn("NrvSaldo/PRL/Qualitaetsgesichert", "FCR_NTP_"),
         # published only from 22.06.2022 on; earlier months -> HTTP 400
         lambda ts: ["NTP"] if ts >= pd.Timestamp("2022-06-01", tz=TZ) else [],
         "extended", "MW",
         "FCR (primary control) data (netztransparenz.de)")]
      if COUNTRY == "DE" else []),
    # ---- Elia Open Data (Belgium only) ---------------------------------------
    *([D("Elia_Imbalance", f_elia_imbalance, lambda ts: ["ELIA"], "core", "EUR/MWh, MW",
         "Elia imbalance price, system imbalance and price components (ods047 / ods134)")]
      if COUNTRY == "BE" and USE_ELIA else []),
    # ---- regelleistung.net (Germany only, EXPERIMENTAL) ----------------------
    *([D(f"RL_{lab}", make_rl_fn(prod, lab), rl_areas(), "experimental", "EUR/MW, MW",
         f"{lab} capacity auction results per 4h product (regelleistung.net)", merge=False,
         cache=f"RL_{lab}_v2")
       for prod, lab in [("FCR", "FCR"), ("aFRR", "aFRR"), ("mFRR", "mFRR")]]
      if COUNTRY == "DE" and USE_REGELLEISTUNG else []),
    # ---- netztransparenz ID-AEP (Germany): intraday index price ----------------
    *([D("NTP_ID_AEP", make_ntp_fn("IdAep", "P_ID_AEP_", single_name="P_ID_AEP"),
         lambda ts: ["NTP"] if ts >= pd.Timestamp("2020-01-01", tz=TZ) else [],
         "extended", "EUR/MWh",
         "Index Ausgleichsenergiepreis (ID-AEP): volume-weighted price of the last 500 MW "
         "of intraday trades per quarter-hour (netztransparenz.de)")]
      if COUNTRY == "DE" else []),
    # ---- EPİAŞ (Türkiye) ---------------------------------------------------------
    *([D("EPIAS_MCP", make_epias_fn("mcp", "EPIAS_MCP_"), lambda ts: ["EPIAS"], "core",
         "EUR/MWh, TRY/MWh, USD/MWh", "Day-ahead market clearing price PTF (EUR = P_DA)"),
       D("EPIAS_SMP", make_epias_fn("smp", "EPIAS_SMP_"), lambda ts: ["EPIAS"], "core",
         "TRY/MWh", "System marginal price SMF (balancing power market)"),
       D("EPIAS_IDM_WAP", make_epias_fn("wap", "EPIAS_WAP_"), lambda ts: ["EPIAS"], "extended",
         "TRY/MWh", "Intraday market weighted average price"),
       D("EPIAS_Consumption", make_epias_fn("rt-cons", "EPIAS_CONS_"), lambda ts: ["EPIAS"],
         "core", "MWh/h", "Real-time consumption (= Load_Actual)"),
       D("EPIAS_Generation", make_epias_fn("rt-gen", "EPIAS_GEN_"), lambda ts: ["EPIAS"],
         "core", "MWh/h", "Real-time generation by source (mapped to Gen_* names)"),
       D("EPIAS_YAL", make_epias_fn("bpm-up", "TR_YAL_"), lambda ts: ["EPIAS"], "extended",
         "MWh", "Balancing power market: load-up (YAL) instructions, order summary"),
       D("EPIAS_YAT", make_epias_fn("bpm-down", "TR_YAT_"), lambda ts: ["EPIAS"], "extended",
         "MWh", "Balancing power market: load-down (YAT) instructions, order summary"),
       D("EPIAS_Imbalance_Qty", make_epias_fn("imb-qty", "TR_IMB_"), lambda ts: ["EPIAS"],
         "extended", "MWh", "System imbalance quantities (positive / negative)"),
       D("EPIAS_FCR_Price", make_epias_fn("anc-pfk", "TR_PFK_"), lambda ts: ["EPIAS"],
         "extended", "TRY/MW", "Primary frequency reserve (FCR) capacity price"),
       D("EPIAS_aFRR_Price", make_epias_fn("anc-sfk", "TR_SFK_"), lambda ts: ["EPIAS"],
         "extended", "TRY/MW", "Secondary frequency reserve (aFRR) capacity price"),
       D("EPIAS_FCR_Qty", make_epias_fn("anc-pf-qty", "TR_PFQ_"), lambda ts: ["EPIAS"],
         "extended", "MW", "Primary frequency reserve amount"),
       D("EPIAS_aFRR_Qty", make_epias_fn("anc-sf-qty", "TR_SFQ_"), lambda ts: ["EPIAS"],
         "extended", "MW", "Secondary frequency reserve amount"),
       D("EPIAS_YEKDEM_Unit_Cost", make_epias_fn("ren-unit-cost", "TR_YEKDEM_"),
         lambda ts: ["EPIAS"], "extended", "TRY/MWh",
         "YEKDEM unit cost passed on to consumers (monthly) – part of the all-in cost",
         merge=False)]
      if COUNTRY == "TR" else []),
    # ---- weather / Eurostat / FRED (all countries) --------------------------------
    *([D("Weather", f_weather, lambda ts: ["OPEN_METEO"], "extended", "°C, W/m², km/h, %",
         "Population-weighted ERA5 weather (Open-Meteo): temperature, GHI, wind 100 m, clouds",
         segment="year")] if USE_WEATHER else []),
    *([D("Eurostat_Prices", make_eurostat_fn("nrg_pc_205", "ES_", ["nrg_cons", "tax"]),
         lambda ts: ["EUROSTAT"], "extended", "EUR/kWh",
         "Eurostat nrg_pc_205: non-household electricity prices by band (IA-IG) and tax level, "
         "semi-annual", merge=False, segment="once"),
       D("Eurostat_Components", make_eurostat_fn("nrg_pc_205_c", "ESC_", ["nrg_cons", "nrg_prc"]),
         lambda ts: ["EUROSTAT"], "extended", "EUR/kWh",
         "Eurostat nrg_pc_205_c: price components (energy, network, taxes, levies), annual",
         merge=False, segment="once")] if USE_EUROSTAT else []),
    *([D("FRED_Fuels", f_fred, lambda ts: ["FRED"], "extended", "USD and EUR",
         "Monthly IMF prices via FRED: EU natural gas, Australian coal, Brent; USD/EUR",
         merge=False, segment="once")] if USE_FRED else []),
    D("Reserve_FCR", make_reserve_fn("A52", "FCR"), P["reserve"],
      "experimental", "EUR/MW", "Contracted FCR capacity: price (EUR/MW per product) and volume, per direction", merge=False),
    D("Reserve_aFRR", make_reserve_fn("A51", "aFRR"), P["reserve"],
      "experimental", "EUR/MW", "Contracted aFRR capacity: price and volume, per direction", merge=False),
    D("Reserve_mFRR", make_reserve_fn("A47", "mFRR"), P["reserve"],
      "experimental", "EUR/MW", "Contracted mFRR capacity: price and volume, per direction", merge=False),
  ]


_NON_ENTSOE = ("EPIAS_", "Weather", "Eurostat_", "FRED_")


def build_datasets_for_profile():
    ds = build_datasets()
    if not P.get("entsoe", True):               # Türkiye: only non-ENTSO-E sources
        ds = [d for d in ds if d["name"].startswith(_NON_ENTSOE)]
    return ds


DATASETS = build_datasets_for_profile()


def configure(country):
    """Switch all country-dependent globals (profile, folders, datasets)."""
    global COUNTRY, P, DATASET_NAME, DRIVE_FOLDER, LOCAL_FOLDER, DATASETS, TZ, START, END
    COUNTRY = country
    P = PROFILES[country]
    TZ = P.get("tz", "Europe/Berlin")
    START = pd.Timestamp(START_DATE, tz=TZ)
    END = pd.Timestamp(END_DATE, tz=TZ) if END_DATE else pd.Timestamp.now(tz=TZ).normalize()
    DRIVE_FOLDER, DATASET_NAME = zone_location(country)
    LOCAL_FOLDER = DRIVE_FOLDER
    DATASETS = build_datasets_for_profile()
    FAILURES.clear()

# =============================================================================
#  SEGMENTS & CHECKPOINTS
# =============================================================================

ONCE_END = pd.Timestamp("2100-01-01", tz="UTC")


def segments(kind="month"):
    """[start, end) pieces; never cross the 2018 bidding-zone split.
    kind 'once': one segment for small global tables (Eurostat, FRED)."""
    if kind == "once":
        yield START, ONCE_END.tz_convert(TZ)
        return
    cur = START
    while cur < END:
        if kind == "year":
            nxt = pd.Timestamp(year=cur.year + 1, month=1, day=1, tz=TZ)
        else:
            nxt = cur + pd.offsets.MonthBegin(1)
        nxt = min(nxt, END)
        if cur < ZONE_SWITCH < nxt and TZ == "Europe/Berlin":
            nxt = ZONE_SWITCH
        yield cur, nxt
        cur = nxt


def seg_key(s, e):
    return f"{s:%Y-%m-%d}_{e:%Y-%m-%d}"


def area_tag(area):
    return "-".join(area) if isinstance(area, tuple) else str(area)


NODATA = object()
NODATA_ERRORS = (NoMatchingDataError, InvalidBusinessParameterError, InvalidPSRTypeError)


_TOKEN_RE = re.compile(r"(securityToken|access_token|token|apikey|api_key)=(?!\*\*\*)[^&\s\"'<>]+", re.I)


def scrub(text):
    """Remove API keys / tokens from URLs in error messages before they are
    printed or stored (entsoe-py puts the key into the request URL)."""
    return _TOKEN_RE.sub(lambda m: f"{m.group(1)}=***", str(text))


def scrub_files(root=None):
    """Removes API keys from stored files (manifest.json, *.rejected, audit files);
    request URLs in error messages of older downloads could contain them.
    Returns the number of files fixed."""
    root = root or root_dir()
    fixed = 0
    pats = ["**/manifest.json", "**/*.rejected", "audit/*.csv", "**/gap_report.csv"]
    for pat in pats:
        for f in glob.glob(os.path.join(root, pat), recursive=True):
            try:
                with open(f, encoding="utf-8", errors="replace") as fh:
                    txt = fh.read()
                if _TOKEN_RE.search(txt):
                    with open(f, "w", encoding="utf-8") as fh:
                        fh.write(scrub(txt))
                    fixed += 1
            except OSError:
                continue
    if fixed:
        print(f">>> removed API keys from {fixed} stored file(s)")
    return fixed


class FetchError(Exception):
    pass


def call_with_retry(fn, area, s, e):
    last = None
    for attempt in range(MAX_TRIES):
        try:
            res = fn(get_client(), area, s, e)
            if res is None or len(res) == 0:
                return NODATA
            return res
        except NODATA_ERRORS:
            return NODATA
        except Exception as ex:
            if "No objects to concatenate" in str(ex):
                return NODATA
            last = ex
            resp = getattr(ex, "response", None)
            code = getattr(resp, "status_code", None)
            if code in (401, 403) and "entsoe" in str(getattr(resp, "url", "") or ex):
                raise ApiBlocked(f"ENTSO-E answered {code} during the download -> key refused or "
                                 "IP temporarily blocked (too many requests)")
            transient = (isinstance(ex, (requests.ConnectionError, requests.Timeout))
                         or "Timeout" in type(ex).__name__ or "timed out" in str(ex).lower()
                         or type(ex).__name__ == "JSONDecodeError"    # truncated response
                         or (isinstance(ex, requests.HTTPError)
                             and (code is None or code == 429 or code >= 500)))
            if not transient:             # parser bugs, 400 etc.: retrying is useless
                break
            time.sleep(15 * 2 ** attempt)
    msg = f"{type(last).__name__}: {scrub(last)[:300]}"
    resp = getattr(last, "response", None)
    if getattr(resp, "status_code", None) == 400:
        msg = f"HTTP400: {scrub(_api_reason(getattr(resp, 'text', '')))[:200]} | {msg}"
    raise FetchError(msg)


FAILURES = []
FAIL_LOCK = threading.Lock()


def _needs_currency(ds, existing, markers=()):
    """Balancing prices of a non-euro zone stored without currency column (Cur_BalE_);
    asked once more only (a marker written after that attempt ends it)."""
    if OFFLINE or not existing or not ds["name"].startswith("BalE_Price") or COUNTRY not in BALE_CURRENCY_MIGRATE:
        return False
    if any(os.path.exists(m) and os.path.getmtime(m) >= os.path.getmtime(existing[0]) for m in markers):
        for f in existing:          # asked again, nothing usable came back: the old file
            os.remove(f)            # (currency unknown) must not be used any more
        return False
    import pyarrow.parquet as pq
    try:
        return not any(n.startswith("Cur_BalE_") for n in pq.read_schema(existing[0]).names)
    except Exception:
        return True


def run_task(ds, s, e, now):
    folder = os.path.join(RAW_DIR, ds["cache"])
    os.makedirs(folder, exist_ok=True)
    key = seg_key(s, e)
    existing = glob.glob(os.path.join(folder, f"{key}__*.parquet"))
    empty_marker = os.path.join(folder, f"{key}.empty")
    volatile = e > now - pd.Timedelta(days=VOLATILE_DAYS)

    rejected = os.path.join(folder, f"{key}.rejected")
    refetch = _needs_currency(ds, existing, (empty_marker, rejected))   # file without currency column
    if not volatile and not refetch and (existing or (os.path.exists(empty_marker) and not RETRY_EMPTY)
                                         or (os.path.exists(rejected) and not RETRY_REJECTED)):
        return "cached"
    if volatile and not OFFLINE:                 # recent month already refreshed a few hours ago?
        checked = os.path.join(folder, f"{key}.checked")
        stamps = [os.path.getmtime(f) for f in existing + [checked] if os.path.exists(f)]
        if stamps and time.time() - max(stamps) < RESUME_HOURS * 3600:
            return "cached"

    errors = []
    for area in ds["areas"](s):
        try:
            res = call_with_retry(ds["fn"], area, s, e)
        except FetchError as err:
            errors.append(f"{area_tag(area)}: {err}")
            continue
        if res is NODATA:
            continue
        res = as_frame(res)
        res = res[(res.index >= s) & (res.index < e)]   # no boundary duplicates
        if res.empty:
            continue
        res = res[~res.index.duplicated(keep="first")].sort_index()
        for f in existing + [x for x in glob.glob(os.path.join(folder, f"{s:%Y-%m-%d}_*"))
                             if not os.path.basename(x).startswith(key)]:
            os.remove(f)                         # also older versions of a running month
        res.to_parquet(os.path.join(folder, f"{key}__{area_tag(area)}.parquet"))
        for m in (empty_marker, os.path.join(folder, f"{key}.checked")):
            if os.path.exists(m):
                os.remove(m)
        return "ok"

    if errors and not volatile and all(x.split(": ", 1)[-1].startswith("HTTP400") for x in errors):
        with open(rejected, "w") as f:           # the source refuses this month for good:
            f.write("\n".join(errors))           # not asked again (RETRY_REJECTED = True does)
        return "rejected"
    if errors:
        with FAIL_LOCK:
            FAILURES.append({"dataset": ds["name"], "segment": key, "errors": errors})
        return "failed"
    if not volatile:
        open(empty_marker, "w").close()
    elif existing:                               # recent month, nothing new: keep data, note the check
        os.utime(existing[0])
    else:                                        # recent month, nothing published yet: only note
        open(os.path.join(folder, f"{key}.checked"), "w").close()   # the check (not "empty")
    return "empty"


# =============================================================================
#  ASSEMBLY
# =============================================================================

def load_dataset(ds):
    folder = os.path.join(RAW_DIR, ds["cache"])
    parts, areas_used, missing = [], {}, []
    for s, e in segments(ds["segment"]):
        key = seg_key(s, e)
        files = glob.glob(os.path.join(folder, f"{key}__*.parquet"))
        if not files:      # open month saved on an earlier day (key ends at that day's date)
            files = glob.glob(os.path.join(folder, f"{s:%Y-%m-%d}_*__*.parquet"))
        if files:
            f = max(files, key=os.path.getmtime)          # several versions: the newest
            parts.append(pd.read_parquet(f))
            area = os.path.basename(f).split("__", 1)[1][:-len(".parquet")]
            areas_used[area] = areas_used.get(area, 0) + 1
        elif not os.path.exists(os.path.join(folder, f"{key}.empty")):
            missing.append(key)
    if not parts:
        return None, areas_used, missing
    df = pd.concat(parts)
    df.index = pd.to_datetime(df.index, utc=True).tz_convert(TZ)
    df = df[~df.index.duplicated(keep="first")].sort_index()
    df.index.name = "timestamp"
    return df, areas_used, missing


def to_15min(df):
    """Spread each value over ITS OWN interval only (hourly -> 4 quarters,
    15-min -> 1 quarter). Real gaps stay NaN, also in 15-min data."""
    utc = df.copy()
    utc.index = utc.index.tz_convert("UTC")
    stamps = pd.Series(utc.index, index=utc.index)
    step = stamps.shift(-1) - stamps
    local_day = utc.index.tz_convert(TZ).date
    res = step.groupby(local_day).transform("min")
    res = res.fillna(pd.Timedelta("15min"))
    res = res.clip(lower=pd.Timedelta("15min"), upper=pd.Timedelta("1h"))
    utc["_src"] = utc.index
    utc["_res"] = res.values
    grid = pd.date_range(utc.index[0].floor("15min"),
                         utc.index[-1] + pd.Timedelta("45min"), freq="15min")
    up = utc.reindex(grid, method="ffill")   # carries NaNs too; masked below
    valid = (up.index.to_series() - up["_src"]) < up["_res"]
    out = up.drop(columns=["_src", "_res"]).where(valid, axis=0)
    out.index = out.index.tz_convert(TZ)
    return out


STORAGE_KEYS = ("Hydro_Pumped_Storage", "Energy_storage")


_ODD_GEN = re.compile(r"^Gen_'(.+?)'_'Actual_(Aggregated|Consumption)'$")


def normalise_gen_columns(df):
    """Some raw months were stored with tuple column names, e.g.
    Gen_'Fossil_Gas'_'Actual_Aggregated' -> merge into Gen_Fossil_Gas."""
    odd = [c for c in df.columns if _ODD_GEN.match(c)]
    for c in odd:
        name, kind = _ODD_GEN.match(c).groups()
        target = f"Gen_{name}" + ("_Consumption" if kind == "Consumption" else "")
        df[target] = df[target].combine_first(df[c]) if target in df.columns else df[c]
    return df.drop(columns=odd) if odd else df


def master_columns(df):
    """Generation '_Consumption' columns are ENTSO-E reporting artefacts
    (sparse self-consumption values) except for storage. They stay in
    raw_combined/Generation.parquet but are kept out of the master table."""
    df = normalise_gen_columns(df)
    drop = [c for c in df.columns if (c.startswith("Gen_") and c.endswith("_Consumption")
            and not any(k in c for k in STORAGE_KEYS)) or (c.startswith("TR_") and c.endswith("_hour"))]
    return df.drop(columns=drop)


def apply_structural_zeros(df):
    for col, day in P["zero_after"].items():
        if col in df.columns:
            mask = df.index >= pd.Timestamp(day, tz=TZ)
            df.loc[mask, col] = df.loc[mask, col].fillna(0.0)
    return df


A03_MAX_GAP = pd.Timedelta(hours=8)


def fill_a03_price_gaps(df):
    """ENTSO-E 'curveType A03' omits points whose price equals the previous
    one. Some older documents (e.g. PL 2015-2017: night hours ~85 % missing,
    day hours ~10 %) arrive with these holes. An interior gap of at most 8 h
    between two published prices is filled with the last published price.
    Longer gaps, whole missing days and the series' end stay NaN.
    Filled points are flagged in Flag_P_DA_Filled (1) so they can be excluded."""
    if not P.get("entsoe", True):
        return df
    for col in [c for c in df.columns if c == "P_DA" or (c.startswith("P_DA_") and c != "P_DA_TRY")]:
        s = df[col]
        if s.notna().all() or s.notna().sum() == 0:
            continue
        valid_t = pd.Series(s.index, index=s.index).where(s.notna())
        prev_t = valid_t.ffill()
        next_t = valid_t.bfill()
        gap = (next_t - prev_t)
        fill = s.isna() & prev_t.notna() & next_t.notna() & (gap <= A03_MAX_GAP)
        n = int(fill.sum())
        if n:
            df[col] = s.where(~fill, s.ffill())
            if col == "P_DA":
                df["Flag_P_DA_Filled"] = fill.astype("Int8").where(df[col].notna())
            print(f"   {col}: {n} points in short gaps (<= 8 h, curveType A03) filled with the last price")
    return df


def clean_load(df):
    """Load_Actual: values <= 0 and isolated one-off spikes (> 40 % of the
    typical load away from the 5-hour rolling median) are reporting errors
    of the TSO -> NaN. Real national load never jumps like that."""
    if "Load_Actual" not in df.columns:
        return df
    s = df["Load_Actual"]
    bad = s <= 0
    typical = s[~bad].median()
    if pd.notna(typical) and typical > 0:
        med = s.where(~bad).rolling("5h", center=True, min_periods=3).median()
        bad |= (s - med).abs() > 0.4 * typical
    n = int(bad.sum())
    if n:
        df.loc[bad, "Load_Actual"] = np.nan
        print(f"   cleaned Load_Actual: {n} implausible values (<= 0 or spikes) set to NaN")
    return df


# ---------------------------------------------------------------------------
#  Currency harmonisation
# ---------------------------------------------------------------------------
#  ENTSO-E sometimes stores prices of non-euro zones in the LOCAL currency
#  although the XML says EUR (PL 2017-19 in PLN, RO in RON and BG in BGN before
#  market coupling, imbalance prices in CZK/RON, ...). Detection: per month the
#  price level is compared with a euro reference zone; if the ratio matches the
#  ECB exchange rate of the local currency, the month is converted. Around a
#  switch the decision is refined day by day. Every conversion is logged in the
#  manifest ("currency_conversions") and printed.
LOCAL_CURRENCY = {"PL": "PLN", "RO": "RON", "BG": "BGN", "HR": "HRK", "CZ": "CZK",
                  "HU": "HUF", "SE": "SEK", "NO": "NOK", "DK": "DKK"}
# Day-ahead detection only where ENTSO-E is known to have mixed currencies.
# Nordic zones are always EUR on ENTSO-E (Nord Pool); detecting there gives
# false alarms (e.g. NO vs. near-zero Finnish prices) -> excluded, the audit's
# cross-zone level check still watches them.
DA_DETECT = {"PL", "RO", "BG", "HR", "CZ", "HU"}
# coupled euro zone used as the price-level reference
EUR_REFERENCE = {"PL": "DE", "RO": "HU", "BG": "GR", "HR": "SI", "CZ": "SK", "HU": "SK"}
REF_MIN_LEVEL = 20.0     # months/days with a lower reference price are not judged
# Zones that switched ONCE from local currency to EUR on ENTSO-E (no way back).
# Their uncoupled price level can be far from the reference (BG was 25-40 %
# cheaper than GR before 2021, so single BGN months look like EUR months);
# for them everything up to the LAST clearly local-currency month is local.
DA_SINGLE_SWITCH = {"BG", "RO", "HR"}
FIXED_RATES = {"BGN": 1.95583}
# Imbalance prices that ENTSO-E stores in the TSO's currency for the whole
# period (checked on the data: CZ ~25x and RO ~4-5x the euro level, with RON
# floor values of 0.10). Converted with the ECB monthly rate, no detection.
FORCED_IMBALANCE_CURRENCY = {"CZ": ("CZK", None), "RO": ("RON", None),
                             # Poland: PSE settles imbalance (CRO) in PLN; on the
                             # data the level is ~4.3x the day-ahead price in
                             # every month 2016-07 .. 2026 (3557 of 3557 days)
                             "PL": ("PLN", None),
                             # Croatia: HRK until the euro (01.01.2023)
                             "HR": ("HRK", "2023-01-01")}
_ECB_CACHE = {}
_FX_LAST = {}                         # newest real (not carried-forward) month per currency
FX_DISCONTINUED = {"HRK"}             # replaced by the euro on 01.01.2023
ECB_URL = "https://data-api.ecb.europa.eu/service/data/EXR/M.{}.EUR.SP00.A?format=csvdata"


def currency_of(code):
    return LOCAL_CURRENCY.get(code.split("_")[0]) if code else None


FX_FALLBACK_URLS = ["https://api.frankfurter.dev/v1/{a}..{b}?from=EUR&to={cur}",
                    "https://api.frankfurter.app/{a}..{b}?from=EUR&to={cur}"]


def _fx_cache_path(cur):
    folder = os.path.join(root_dir(), "fx_rates")
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f"EUR_{cur}_monthly.csv")


FX_HEADERS = {"Accept": "text/csv,application/vnd.sdmx.data+csv;version=1.0.0,*/*",
              "User-Agent": "Mozilla/5.0 (compatible; SB-Energy-Dataset research script)"}
ECB_URLS = [ECB_URL, "https://sdw-wsrest.ecb.europa.eu/service/data/EXR/M.{}.EUR.SP00.A?format=csvdata"]
# FRED (OECD/IMF): national currency per USD, monthly average; x USD per EUR
FRED_FX = {"PLN": ["CCUSMA02PLM618N"], "CZK": ["CCUSMA02CZM618N"], "HUF": ["CCUSMA02HUM618N"],
           "RON": ["CCUSMA02ROM618N", "ROUCCUSMA02STM"], "HRK": ["CCUSMA02HRM618N", "HRVCCUSMA02STM"],
           "TRY": ["CCUSMA02TRM618N"]}


def _fx_from_fred(cur):
    def series(sid):
        r = requests.get(FRED_URL.format(sid), timeout=60, headers=FX_HEADERS)
        r.raise_for_status()
        d = pd.read_csv(io.StringIO(r.text))
        v = pd.to_numeric(d.iloc[:, 1], errors="coerce")
        return pd.Series(v.values, index=pd.DatetimeIndex(pd.to_datetime(d.iloc[:, 0])).to_period("M")).dropna()
    usd_per_eur = series("EXUSEU")
    last = None
    for sid in FRED_FX.get(cur, []):
        try:
            per_usd = series(sid)
            out = (per_usd * usd_per_eur).dropna()
            if len(out):
                return out
        except Exception as ex:
            last = ex
    raise last or ValueError(f"no FRED series for {cur}")


def _fx_from_ecb(cur):
    last = None
    for attempt in range(4):
        try:
            r = requests.get(ECB_URLS[attempt % 2].format(cur), timeout=60, headers=FX_HEADERS)
            r.raise_for_status()
            t = pd.read_csv(io.StringIO(r.text))[["TIME_PERIOD", "OBS_VALUE"]]
            return pd.Series(t["OBS_VALUE"].values, index=pd.PeriodIndex(t["TIME_PERIOD"], freq="M"))
        except Exception as ex:
            last = ex
            time.sleep(3 * (attempt + 1))
    raise last


def _fx_from_frankfurter(cur):
    """Same ECB reference rates (daily) via frankfurter -> monthly means."""
    a, b = "2010-01-01", pd.Timestamp.now().strftime("%Y-%m-%d")
    last = None
    for url in FX_FALLBACK_URLS:
        try:
            r = requests.get(url.format(a=a, b=b, cur=cur), timeout=60, headers=FX_HEADERS)
            r.raise_for_status()
            rates = r.json()["rates"]
            d = pd.Series({pd.Timestamp(k): v[cur] for k, v in rates.items() if cur in v})
            if len(d):
                return d.groupby(d.index.to_period("M")).mean()
        except Exception as ex:
            last = ex
    raise last or ValueError("no data")


def ecb_rates(cur):
    """Monthly ECB reference rate (units of `cur` per EUR) as Series[Period].
    Order: memory -> ECB API (3 tries) -> frankfurter (ECB data) -> last
    copy saved in the project folder (fx_rates/). Successful downloads are
    saved there, so a temporary outage never blocks a rebuild."""
    if _ECB_CACHE.get(cur) is not None:
        return _ECB_CACHE[cur]
    if cur in FIXED_RATES:
        idx = pd.period_range("2010-01", pd.Timestamp.now().strftime("%Y-%m"), freq="M")
        r = pd.Series(FIXED_RATES[cur], index=idx)
        _ECB_CACHE[cur] = r
        _FX_LAST[cur] = idx.max()
        return r
    r, source = None, None
    for name, fn in [("ECB", _fx_from_ecb), ("frankfurter", _fx_from_frankfurter),
                     ("FRED", _fx_from_fred)]:
        try:
            r, source = fn(cur), name
            break
        except Exception as ex:
            print(f"   !! {name} {cur} rates not reachable ({type(ex).__name__})")
    path = _fx_cache_path(cur)
    if r is not None and len(r):
        pd.DataFrame({"month": r.index.astype(str), "rate": r.values}).to_csv(path, index=False)
        if source != "ECB":
            print(f"   {cur} rates taken from {source} (last month {r.index.max()})")
    elif os.path.exists(path):
        t = pd.read_csv(path)
        r = pd.Series(t["rate"].values, index=pd.PeriodIndex(t["month"], freq="M"))
        print(f"   {cur} rates taken from the saved copy {path}")
    if r is None or not len(r):
        print(f"   !! NO {cur} exchange rates available -> {cur} values are left UNCONVERTED")
        return None
    # extend the last known rate (e.g. HRK ends 2022-12) to today
    r = r.sort_index().dropna()
    _FX_LAST[cur] = r.index.max()
    lag = (pd.Period(pd.Timestamp.now(), "M") - r.index.max()).n
    if lag > 2 and cur not in FX_DISCONTINUED:
        print(f"   !! {cur}: newest real rate is {r.index.max()} ({lag} months old) -> carried forward")
    r = r.reindex(pd.period_range(r.index.min(), pd.Timestamp.now().strftime("%Y-%m"), freq="M")).ffill()
    _ECB_CACHE[cur] = r
    return r


def fx_check(currencies=("PLN", "RON", "CZK", "HUF", "HRK", "TRY")):
    """Loads all exchange rates needed by the currency rules (and saves a
    copy in the project folder). Run before rebuild(); True = all available."""
    ok = True
    for cur in currencies:
        _ECB_CACHE.pop(cur, None)
        r = ecb_rates(cur)
        stale = False
        if r is None:
            print(f"   {cur}: NOT AVAILABLE")
        else:
            lag = (pd.Period(pd.Timestamp.now(), "M") - _FX_LAST[cur]).n
            stale = lag > 3 and cur not in FX_DISCONTINUED
            print(f"   {cur}: {len(r)} months, newest real rate {_FX_LAST[cur]} = {r[_FX_LAST[cur]]:.4f}"
                  + ("   !! STALE" if stale else ""))
        ok &= (r is not None and not stale) or cur == "TRY"     # TRY only used by the audit
    print(">>> exchange rates OK" if ok else ">>> missing rates: do NOT rebuild now, try again later")
    return ok


def _reference_price(df, ref_code):
    """Euro price of the reference zone: own column P_DA_<ref> if present,
    otherwise the P_DA column of the reference zone's own hourly table."""
    if ref_code is None:
        return None
    if f"P_DA_{ref_code}" in df.columns:
        return df[f"P_DA_{ref_code}"]
    try:
        folder, prefix = zone_location(ref_code)
        path = os.path.join(folder, "output", f"{prefix}_1h.parquet")
        ref = pd.read_parquet(path, columns=["P_DA"])["P_DA"]
        ref.index = ref.index.tz_convert(df.index.tz)
        return ref.resample(pd.infer_freq(df.index[:10]) or "1h").ffill().reindex(df.index)
    except Exception:
        return None


def _closer_to_rate(ratio, rate):
    """level ratio is nearer (in logs) to the exchange rate than to 1, i.e.
    above the geometric midpoint sqrt(rate)."""
    return (rate is not None and np.isfinite(rate) and rate >= 1.5
            and np.isfinite(ratio) and ratio > np.sqrt(rate))


def _convert(df, target, ref, cur, label, log, single_switch=False):
    """Convert the periods of df[target] that are in `cur` instead of EUR.
    Monthly decision (only months with a reference level >= REF_MIN_LEVEL;
    undecided months take the state of their neighbours), refined day by
    day in the months where the state switches."""
    if target not in df.columns or ref is None or ref.notna().sum() == 0:
        return df
    rates = ecb_rates(cur)
    if rates is None:
        # without a rate no decision is possible; it only matters if the level
        # looks wrong (> 2x the reference in some month)
        mon_ = df.index.tz_localize(None).to_period("M")
        lt, lr = df[target].abs().groupby(mon_).median(), ref.abs().groupby(mon_).median()
        if ((lt / lr.where(lr >= REF_MIN_LEVEL)) > 2).any():
            log.setdefault("currency_unconverted", []).append(target)
        return df
    t, r = df[target].abs(), ref.abs()
    month = pd.Series(df.index.tz_localize(None).to_period("M"), index=df.index)
    lvl_t = t.groupby(month.array).median()
    lvl_r = r.groupby(month.array).median()
    rate_m = rates.reindex(lvl_t.index) if rates is not None else pd.Series(np.nan, lvl_t.index)
    state = pd.Series(np.nan, index=lvl_t.index)
    ok = (lvl_r >= REF_MIN_LEVEL) & lvl_t.notna()
    for m in lvl_t.index[ok.reindex(lvl_t.index, fill_value=False)]:
        state[m] = float(_closer_to_rate(lvl_t[m] / lvl_r[m], rate_m.get(m)))
    if single_switch and (state == 1).any():
        # one switch local -> EUR: everything up to the last local DAY is local
        last_local = state[state == 1].index.max()
        day_ = pd.Series(df.index.tz_localize(None).normalize(), index=df.index)
        cand = (month.array >= last_local) & (month.array <= last_local + 1)
        dt = t[cand].groupby(day_[cand].values).median()
        dr = r[cand].groupby(day_[cand].values).median()
        # each candidate day: +1 looks local, -1 looks EUR, 0 undecided (low reference)
        days_ = sorted(dt.index)
        vote = np.array([0 if not dr.get(d, 0) >= REF_MIN_LEVEL else
                         (1 if _closer_to_rate(dt[d] / dr[d], rate_m.get(pd.Period(d, "M"))) else -1)
                         for d in days_])
        # split = last local day; choose the split with the fewest misclassified
        # days (a single EUR price spike after the switch cannot move it)
        best_cost, best_k = None, -1
        for k in range(-1, len(days_)):
            cost = int((vote[:k + 1] == -1).sum() + (vote[k + 1:] == 1).sum())
            if best_cost is None or cost < best_cost:
                best_cost, best_k = cost, k
        if best_k >= 0:
            last_day = days_[best_k]
        else:                                   # all candidate days are EUR
            last_day = (days_[0] - pd.Timedelta(days=1)) if days_ else last_local.end_time.normalize()
        mask = (day_ <= last_day) & df[target].notna()
        return _apply_conversion(df, target, mask, month, rates, cur, label, log, day_)
    state = state.ffill().bfill()
    if not (state == 1).any():
        return df
    flag = state == 1
    switch = flag.astype(int).diff().fillna(0) != 0
    border = set(flag.index[switch | switch.shift(-1, fill_value=False)])
    day = pd.Series(df.index.tz_localize(None).normalize(), index=df.index)
    mask = pd.Series(False, index=df.index)
    for m, f in flag.items():
        in_m = (month == m).values
        if m in border:
            dt = t[in_m].groupby(day[in_m].values).median()
            dr = r[in_m].groupby(day[in_m].values).median()
            local = {d for d in dt.index if dr.get(d, 0) >= REF_MIN_LEVEL
                     and _closer_to_rate(dt[d] / dr[d], rate_m.get(m))}
            undecided = {d for d in dt.index if not dr.get(d, 0) >= REF_MIN_LEVEL}
            if f:                                   # low-reference days follow the month
                local |= undecided
            mask[in_m] = day[in_m].isin(local).values
        elif f:
            mask[in_m] = True
    mask &= df[target].notna()
    return _apply_conversion(df, target, mask, month, rates, cur, label, log, day)


def _apply_conversion(df, target, mask, month, rates, cur, label, log, day):
    if not mask.any():
        return df
    rate = month[mask].map(rates)
    no_rate = int(rate.isna().sum())
    df.loc[mask, target] = df.loc[mask, target] / rate.values
    days = day[mask]
    entry = {"column": target, "currency": cur, "reference": label, "how": f"ECB monthly EUR/{cur}",
             "days": int(days.nunique()),
             "from": str(days.min().date()), "to": str(days.max().date())}
    if no_rate:
        entry["values_without_rate_set_NaN"] = no_rate
        print(f"   !! {target}: {no_rate} values in months without an exchange rate -> NaN")
    log.setdefault("currency_conversions", []).append(entry)
    print(f"   currency: {target} delivered in {cur} on {days.nunique()} days "
          f"({entry['from']} .. {entry['to']}) -> converted to EUR")
    return df


HMMCP_CHANGE = pd.Timestamp("2022-05-10", tz="UTC")   # SDAC max 3000 -> 4000 (later 5000)


def apply_price_bounds(df, log):
    """Day-ahead prices outside the harmonised SDAC limits cannot exist
    (min -500, max 3000 EUR/MWh until 10.05.2022, afterwards up to 5000)
    -> data errors, set to NaN. Not for IE_SEM / TR (own market rules)."""
    if COUNTRY in ("IE_SEM", "TR"):
        return df
    utc = df.index.tz_convert("UTC")
    cap = np.where(utc < HMMCP_CHANGE, 3000.0, 5000.0)
    for col in [c for c in df.columns if c == "P_DA" or (c.startswith("P_DA_")
                and c not in ("P_DA_TRY",) and "IE" not in c and "GB" not in c)]:
        bad = (df[col] < -500) | (df[col] > cap)
        n = int(bad.sum())
        if n:
            log.setdefault("price_bound_removed", []).append(
                {"column": col, "n": n, "values": [round(float(v), 1) for v in df.loc[bad, col].head(5)],
                 "first": str(df.index[bad][0])[:16]})
            df.loc[bad, col] = np.nan
            print(f"   {col}: {n} values outside the SDAC price limits set to NaN")
    return df


def drop_empty_short_price(df, log):
    """Single-price systems (e.g. IT from 2022-04, SK from 2024-07): ENTSO-E fills
    the second imbalance price with 0. A month in which P_Imb_Short is exactly 0 in
    more than half of the intervals while P_Imb_Long has real values cannot be a
    real two-price month -> P_Imb_Short empty there."""
    if not {"P_Imb_Short", "P_Imb_Long"} <= set(df.columns):
        return df
    month = df.index.tz_localize(None).to_period("M")
    sh, lo = df["P_Imb_Short"], df["P_Imb_Long"]
    stat = pd.DataFrame({"zero": sh.eq(0) & sh.notna(), "n": sh.notna(),
                         "long_real": lo.notna() & lo.ne(0)}).groupby(month).sum()
    bad = stat.index[(stat["n"] > 0) & (stat["zero"] > 0.5 * stat["n"]) & (stat["long_real"] > 0.5 * stat["n"])]
    if len(bad):
        df.loc[month.isin(bad), "P_Imb_Short"] = np.nan
        log["single_price_short_emptied"] = f"{bad.min()} .. {bad.max()} ({len(bad)} months)"
        print(f"   P_Imb_Short = 0 (single-price system) in {len(bad)} months {bad.min()} .. {bad.max()} -> left empty")
    return df


def harmonise_currency(df, log):
    """All price columns of this table -> EUR (see block comment above)."""
    own_cur = currency_of(COUNTRY) if P.get("entsoe", True) else None
    # 1) own day-ahead (and intraday auction) price against the euro reference
    if own_cur and COUNTRY in DA_DETECT:
        ref_code = EUR_REFERENCE.get(COUNTRY)
        ref = _reference_price(df, ref_code)
        if ref is None:
            print(f"   !! currency check of P_DA skipped (reference {ref_code} not downloaded yet) "
                  f"-> later: finalize(['{COUNTRY}'])")
            log.setdefault("currency_check_skipped", []).append(f"P_DA (reference {ref_code} missing)")
        else:
            for col in ["P_DA"] + [c for c in df.columns if c.startswith("P_IDA")]:
                df = _convert(df, col, ref, own_cur, f"P_DA {ref_code}", log,
                              single_switch=COUNTRY in DA_SINGLE_SWITCH)
    # 2) neighbour prices against the own (euro) price
    if "P_DA" in df.columns:
        for col in [c for c in df.columns if c.startswith("P_DA_") and c != "P_DA_TRY"]:
            nb = col[5:]
            if nb in DA_DETECT and currency_of(nb):
                df = _convert(df, col, df["P_DA"], currency_of(nb), "own P_DA", log,
                              single_switch=nb in DA_SINGLE_SWITCH)
    # 3) imbalance prices: fixed rules (checked on the data) ...
    month = pd.Series(df.index.tz_localize(None).to_period("M"), index=df.index)
    imb_cols = [c for c in df.columns if c.startswith("P_Imb_")]
    forced = FORCED_IMBALANCE_CURRENCY.get(COUNTRY)
    if forced and imb_cols:
        cur, until = forced
        rates = ecb_rates(cur)
        if rates is None:
            log.setdefault("currency_unconverted", []).extend(imb_cols)
            imb_cols = []
        sel = (df.index < pd.Timestamp(until, tz=df.index.tz)) if until else np.ones(len(df), bool)
        for col in imb_cols:
            rate = month[sel].map(rates)
            had_value = df.loc[sel, col].notna()
            df.loc[sel, col] = df.loc[sel, col] / rate.values
            v = df.loc[sel, col]
            n_months_nan = int(month[sel][rate.isna() & had_value].nunique())
            if n_months_nan:
                print(f"   !! {col}: {n_months_nan} months without an exchange rate -> NaN")
            log.setdefault("currency_conversions", []).append(
                {"column": col, "currency": cur, "reference": "fixed rule",
                 "how": f"ECB monthly EUR/{cur}", "months_without_rate": n_months_nan,
                 "from": str(v.first_valid_index())[:10], "to": str(v.last_valid_index())[:10]})
            print(f"   currency: {col} converted from {cur} (fixed rule"
                  + (f", until {until})" if until else ")"))
    # ... otherwise detection, only where the exchange rate is large (HUF, CZK):
    # an imbalance price 20x the day-ahead level cannot be euro
    elif own_cur and "P_DA" in df.columns and imb_cols:
        rates = ecb_rates(own_cur)
        if rates is not None and rates.median() >= 20:
            for col in imb_cols:
                df = _convert(df, col, df["P_DA"], own_cur, "own P_DA", log)
    # 4) activated balancing energy prices: the currency is known per row
    #    (Cur_BalE_<product>, read from the XML). Raw files without it (older downloads) have
    #    none: checked fallback (RO, HU), else detection where the rate is large.
    month = pd.Series(df.index.tz_localize(None).to_period("M"), index=df.index)
    for prod in BAL_TYPES:
        cols = [c for c in df.columns if c.startswith(f"P_BalE_{prod}_")]
        if not cols:
            continue
        code = df[f"Cur_BalE_{prod}"] if f"Cur_BalE_{prod}" in df.columns else pd.Series(np.nan, index=df.index)
        code = code.round()
        has_val = df[cols].notna().any(axis=1)
        fb = BALE_CURRENCY_FALLBACK.get(COUNTRY)
        if fb:
            code = code.where(code.notna() | ~has_val, ISO_NUM[fb])
        for num in sorted(set(code.dropna().unique()) - {ISO_NUM["EUR"]}):
            cur = NUM_ISO.get(int(num))
            sel = (code == num).to_numpy()
            rates = ecb_rates(cur) if cur else None
            if rates is None:
                log.setdefault("currency_unconverted", []).extend(cols)
                df.loc[sel, cols] = np.nan             # unknown currency: never pass it on as EUR
                print(f"   !! {prod} balancing prices in {cur or num}: no exchange rate -> left empty")
                continue
            rate = month[sel].map(rates).to_numpy()
            for col in cols:
                df.loc[sel, col] = df.loc[sel, col].to_numpy() / rate
            v = df.loc[sel, cols[0]]
            log.setdefault("currency_conversions", []).append(
                {"column": f"P_BalE_{prod}_*", "currency": cur, "reference": "XML currency_Unit"
                 if f"Cur_BalE_{prod}" in df.columns else "checked fallback",
                 "how": f"ECB monthly EUR/{cur}", "months_without_rate": int(month[sel][np.isnan(rate)].nunique()),
                 "from": str(v.first_valid_index())[:10], "to": str(v.last_valid_index())[:10]})
            print(f"   currency: {prod} balancing prices converted from {cur} ({int(sel.sum()):,} rows)")
        legacy = (code.isna() & has_val).to_numpy()
        if legacy.any() and COUNTRY in BALE_CURRENCY_MIGRATE:
            # currency unknown and not checkable on the data (PLN/BGN/HRK are too
            # close to the euro level): never pass these values on as EUR
            df.loc[legacy, cols] = np.nan
            log.setdefault("currency_unconverted", []).extend(cols)
            print(f"   !! {prod} balancing prices: {int(legacy.sum()):,} rows without currency "
                  "information -> left empty (run() fetches them again)")
        elif legacy.any() and own_cur and "P_DA" in df.columns:
            rates = ecb_rates(own_cur)
            if rates is not None and rates.median() >= 20:
                part = df.loc[legacy].copy()
                for col in cols:
                    part = _convert(part, col, part["P_DA"], own_cur, "own P_DA", log)
                df.loc[legacy, cols] = part[cols]
    return df.drop(columns=[c for c in df.columns if c.startswith("Cur_BalE_")])


def col_or_none(df, name):
    return df[name] if name in df.columns else None


def add_derived(df):
    # one continuous imbalance price column: official reBAP where available,
    # otherwise the ENTSO-E value (DE until 2022-06, BE always)
    # BE: Elia "imbalanceprice" after MARI (22.05.2024), "positiveimbalanceprice"
    # before (single price in BE, equal to the negative one in normal hours)
    imb = [c for c in ["P_reBAP_unterdeckt", "Elia_imbalanceprice",
                       "Elia_positiveimbalanceprice", "P_Imb_Long"]
           if c in df.columns]
    if imb:
        pi = df[imb[0]].copy()
        for c in imb[1:]:
            pi = pi.fillna(df[c])
        df["P_Imbalance"] = pi
    p = col_or_none(df, "P_DA")
    if p is not None:
        rank = p.groupby(df.index.date).rank(pct=True)
        df["Is_Cheapest_Energy_Window"] = (rank <= 0.15).astype("Int8").where(p.notna())
        df["Flag_Negative_DA"] = (p < 0).astype("Int8").where(p.notna())

    # forecast errors (actual - forecast)
    for actual, fc, out in [("Gen_Solar", "Solar_Forecast_DA", "Solar_Forecast_Error"),
                            ("Gen_Wind_Onshore", "Wind_Onshore_Forecast_DA", "Wind_On_Forecast_Error"),
                            ("Gen_Wind_Offshore", "Wind_Offshore_Forecast_DA", "Wind_Off_Forecast_Error"),
                            ("Load_Actual", "Load_Forecast_DA", "Load_Forecast_Error")]:
        if actual in df.columns and fc in df.columns:
            df[out] = df[actual] - df[fc]

    # variable renewables, residual load
    vre_cols = [c for c in ["Gen_Solar", "Gen_Wind_Onshore", "Gen_Wind_Offshore"] if c in df.columns]
    if vre_cols and "Load_Actual" in df.columns:
        vre = df[vre_cols].sum(axis=1, skipna=False)
        df["VRE_Generation"] = vre
        df["Residual_Load"] = df["Load_Actual"] - vre
        df["VRE_Share_of_Load"] = vre / df["Load_Actual"]

    # physical net import over all borders reporting at that time
    # (AT is internal before 2018-10, NO_2 only exists since NordLink 2020)
    imp = [c for c in df.columns if c.startswith("Flow_Import_")]
    exp = [c for c in df.columns if c.startswith("Flow_Export_")]
    if imp and exp:
        df["Net_Import_Physical"] = (df[imp].sum(axis=1, min_count=1)
                                     - df[exp].sum(axis=1, min_count=1))
        df["N_Borders_Reporting"] = df[imp].notna().sum(axis=1)

    # approximate grid CO2 intensity of German generation (direct emissions)
    gen = [c for c in df.columns if c.startswith("Gen_") and not c.endswith("_Consumption")
           and c not in ("Gen_Forecast_DA_Total",)]
    if gen:
        total = df[gen].sum(axis=1, min_count=1)
        ef_d, ef_l = dict(EMISSION_FACTORS), dict(EMISSION_FACTORS_LCA)
        if P.get("other_ef"):                     # country-specific assumption (IT)
            ef_d["Other"], ef_l["Other"] = P["other_ef"]
        emis = sum(df[f"Gen_{k}"].fillna(0) * ef for k, ef in ef_d.items()
                   if f"Gen_{k}" in df.columns)
        if not isinstance(emis, int):
            df["CO2_Intensity_approx_g_per_kWh"] = (emis / total).where(total > 0)
        lca = sum(df[f"Gen_{k}"].fillna(0) * ef for k, ef in ef_l.items()
                  if f"Gen_{k}" in df.columns)
        if not isinstance(lca, int):
            df["CO2_Intensity_LCA_AR5_g_per_kWh"] = (lca / total).where(total > 0)
        # Some TSOs report only part of the generation mix in early years
        # (e.g. Sweden: only wind until 12/2021). A CO2 intensity from such a
        # partial mix is meaningless -> blank months where fewer than half of
        # the production types report. Gen_Types_Reporting documents coverage.
        month = pd.Series(df.index.tz_localize(None).to_period("M"), index=df.index)
        n_types = df[gen].notna().groupby(month.array).any().sum(axis=1)
        df["Gen_Types_Reporting"] = month.map(n_types).astype("Int16").values
        # reference = 75th percentile of months (robust against single months
        # in which a TSO reports unusually many types)
        weak = n_types[n_types < 0.5 * n_types.quantile(0.75)].index
        if len(weak):
            mask = month.isin(weak).values
            for col in ["CO2_Intensity_approx_g_per_kWh", "CO2_Intensity_LCA_AR5_g_per_kWh"]:
                if col in df.columns:
                    df.loc[mask, col] = np.nan

    # Türkiye: SMF in EUR with the day's TRY/EUR ratio implied by EPİAŞ's PTF
    if {"P_SMF_TRY", "P_DA", "P_DA_TRY"} <= set(df.columns):
        ratio = (df["P_DA"] / df["P_DA_TRY"]).replace([np.inf, -np.inf], np.nan)
        daily = ratio.groupby(df.index.date).transform("median")
        df["P_SMF"] = df["P_SMF_TRY"] * daily
        df["P_Imbalance"] = df["P_SMF"]
        for col in ["TR_FCR_Capacity_Price_TRY", "TR_aFRR_Capacity_Price_TRY"]:
            if col in df.columns:
                df[col[:-4] + "_EUR"] = df[col] * daily

    # calendar
    idx = df.index
    df["Hour"] = idx.hour
    df["Weekday"] = idx.day_of_week
    df["Month"] = idx.month
    df["Is_Weekend"] = pd.Series(idx.day_of_week >= 5, index=idx).astype("Int8")
    try:
        import holidays
        de_hol = getattr(holidays, P["holidays"])(years=range(idx.min().year, idx.max().year + 1))
        df["Is_Public_Holiday"] = pd.Series(idx.date, index=idx).map(
            lambda d: d in de_hol).astype("Int8")
    except ImportError:
        pass
    return df


# =============================================================================
#  MAIN
# =============================================================================

def main():
    global API_KEY, RAW_DIR, OUT_DIR
    base = resolve_base_dir()
    RAW_DIR = os.path.join(base, "raw")
    OUT_DIR = os.path.join(base, "output")
    if OFFLINE and not (os.path.isdir(RAW_DIR) and os.listdir(RAW_DIR)):
        print(f"   {COUNTRY}: no raw files -> nothing to rebuild (download it first with run(['{COUNTRY}']))")
        return None, None
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(os.path.join(OUT_DIR, "raw_combined"), exist_ok=True)

    API_KEY = get_api_key()
    now = pd.Timestamp.now(tz=TZ)

    # smoke test: fail fast on a bad key instead of 6000 failed requests
    if not P.get("entsoe", True) or OFFLINE:
        pass
    else:
      try:
        get_client().query_day_ahead_prices(P["zone"](now), start=now.normalize() - pd.Timedelta(days=3),
                                            end=now.normalize() - pd.Timedelta(days=2))
      except Exception as ex:
        code = getattr(getattr(ex, "response", None), "status_code", None)
        if code in (401, 403):
            body = getattr(getattr(ex, "response", None), "text", "") or ""
            raise ApiBlocked(f"ENTSO-E answered {code} to a simple price request -> the API "
                             f"key is blocked or not valid right now. Server says: {scrub(body)[:300]}")
        print(f"   (smoke test warning: {type(ex).__name__}: {scrub(ex)[:150]})")
    if COUNTRY == "TR" and not OFFLINE and not (EPIAS_USERNAME and EPIAS_PASSWORD):
        print("   !! Türkiye skipped: Colab secrets EPIAS_USERNAME / EPIAS_PASSWORD missing "
              "(see credentials_check()).")
        return None, None

    active = [d for d in DATASETS if d["group"] in GROUPS]
    tasks = [(d, s, e) for d in active for s, e in segments(d["segment"])]
    print(f">>> {DATASET_NAME}: {len(active)} datasets, {len(tasks)} segments, "
          f"{START:%Y-%m-%d} -> {END:%Y-%m-%d}, base folder: {base}")
    if OFFLINE:
        print("   offline rebuild: nothing is downloaded, tables are rebuilt from raw files")
        try:
            old_failures = _read_json(os.path.join(OUT_DIR, "manifest.json")).get("failures", [])
        except Exception:
            old_failures = []
    else:
        if COUNTRY == "DE" and not (NTP_CLIENT_ID and NTP_CLIENT_SECRET):
            n0 = len(tasks)
            tasks = [t for t in tasks if not t[0]["name"].startswith("NTP_")]
            print(f"   !! netztransparenz credentials missing: {n0 - len(tasks)} NTP segments not updated "
                  "(stored data is kept) -> see credentials_check()")
        run_tasks(tasks, now)

    print(">>> Assembling tables ...")
    master, hourly, manifest, ds_cols = assemble(active, now)

    # ---------------- gap audit (+ one-time re-fetch) ----------------
    gaps = gap_audit(master, ds_cols)
    if REFETCH_GAPS and not OFFLINE and not gaps.empty:
        retried_path = os.path.join(OUT_DIR, "gap_retried.json")
        try:
            retried = set(_read_json(retried_path))
        except Exception:
            retried = set()
        by_name = {d["name"]: d for d in active}
        month_seg = {f"{s:%Y-%m}": (s, e) for s, e in segments("month")}
        todo = []
        for (name, month), _ in gaps.groupby(["dataset", "month"]):
            key = f"{name}|{month}"
            d = by_name.get(name)
            if key in retried or d is None or d["segment"] != "month" or month not in month_seg:
                continue
            s_, e_ = month_seg[month]
            folder = os.path.join(RAW_DIR, d["cache"])
            for f in glob.glob(os.path.join(folder, f"{seg_key(s_, e_)}__*.parquet")) + \
                    glob.glob(os.path.join(folder, f"{seg_key(s_, e_)}.empty")):
                os.replace(f, f + ".bak")        # keep old data until the new one is in
            todo.append((d, s_, e_))
            retried.add(key)
        if todo:
            print(f"\n>>> Gap audit: re-fetching {len(todo)} month(s) with gaps once ...")
            run_tasks(todo, now)
            restored = 0
            for d, s_, e_ in todo:
                folder = os.path.join(RAW_DIR, d["cache"])
                k = seg_key(s_, e_)
                baks = glob.glob(os.path.join(folder, f"{k}*.bak"))
                if glob.glob(os.path.join(folder, f"{k}__*.parquet")):
                    for b in baks:                # new download succeeded
                        os.remove(b)
                else:                             # nothing new -> restore old data
                    for f in glob.glob(os.path.join(folder, f"{k}.empty")):
                        os.remove(f)
                    for b in baks:
                        os.replace(b, b[:-4])
                    restored += bool(baks)
            if restored:
                print(f"   ({restored} month(s) returned nothing new; previous data kept)")
            with open(retried_path, "w") as f:
                json.dump(sorted(retried), f, indent=0)
            print(">>> Re-assembling tables ...")
            master, hourly, manifest, ds_cols = assemble(active, now)
            gaps = gap_audit(master, ds_cols)
    gaps.to_csv(os.path.join(OUT_DIR, "gap_report.csv"), index=False)

    # ---------------- quality report ----------------
    q = pd.DataFrame({
        "missing_pct": (master.isna().mean() * 100).round(2),
        "first_valid": master.apply(lambda c: c.first_valid_index()),
        "last_valid": master.apply(lambda c: c.last_valid_index()),
    })
    q.to_csv(os.path.join(OUT_DIR, "quality_report.csv"))
    manifest["failures"] = old_failures if OFFLINE else FAILURES
    manifest["rejected_by_source"] = sorted(
        f"{os.path.basename(os.path.dirname(f))} {os.path.basename(f)[:-9]}"
        for f in glob.glob(os.path.join(RAW_DIR, "*", "*.rejected")))
    manifest["gap_months_remaining"] = int(gaps[["dataset", "month"]].drop_duplicates().shape[0]) if not gaps.empty else 0
    with open(os.path.join(OUT_DIR, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    if VERBOSE:
        print("\n========== QUALITY REPORT (15-min master) ==========")
        print(f"Rows: {len(master)}   Columns: {master.shape[1]}")
        with pd.option_context("display.max_rows", 500, "display.width", 140):
            print(q)

        print("\n========== RAW-ONLY TABLES (output/raw_combined) ==========")
        for name, info in manifest["datasets"].items():
            if not info.get("merged_into_master"):
                print(f"   {name:24s} rows: {info.get('rows', 0):>8}   "
                      f"{str(info.get('first', '-'))[:10]} -> {str(info.get('last', '-'))[:10]}")

    print("\n========== GAP AUDIT (gaps INSIDE each series' own range) ==========")
    if gaps.empty:
        print("   No month with more than "
              f"{GAP_THRESHOLD_PCT:.0f}% missing inside any series. ")
    else:
        summary = (gaps.groupby("dataset")
                   .agg(months=("month", "nunique"), first=("month", "min"),
                        last=("month", "max"), worst_pct=("missing_pct", "max")))
        print("   Remaining gaps after one re-fetch are most likely gaps AT THE SOURCE.")
        print("   Details: output/gap_report.csv")
        with pd.option_context("display.width", 140):
            print(summary.to_string())

    if FAILURES:
        print(f"\n!! {len(FAILURES)} failed segments (re-run the script to retry):")
        fs = pd.DataFrame(FAILURES)
        for name, grp in fs.groupby("dataset"):
            print(f"   {name}: {len(grp)} segments ({grp['segment'].min()[:7]} .. "
                  f"{grp['segment'].max()[:7]})  e.g. {grp['errors'].iloc[0][0][:140]}")
    elif OFFLINE:
        print(f"\nOffline rebuild: failure list of the last download kept "
              f"({len(manifest['failures'])} segments).")
    else:
        print("\nNo failed segments.")
    print(f"\n>>> Outputs in: {OUT_DIR}")
    return master, hourly


def run_tasks(tasks, now):
    counts = {"cached": 0, "ok": 0, "empty": 0, "failed": 0, "rejected": 0}
    blocked = None
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(run_task, d, s, e, now): (d["name"], seg_key(s, e))
                   for d, s, e in tasks}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                status = fut.result()
            except Exception as ex:          # e.g. disk/parquet errors
                name, key = futures[fut]
                if isinstance(ex, ApiBlocked):
                    blocked = ex
                    for other in futures:
                        other.cancel()                 # stop: the key is refused now
                    break
                with FAIL_LOCK:
                    FAILURES.append({"dataset": name, "segment": key,
                                     "errors": [f"{type(ex).__name__}: {scrub(ex)[:300]}"]})
                status = "failed"
            counts[status] += 1
            if i % 100 == 0 or i == len(tasks):
                print(f"   {i}/{len(tasks)}  {counts}  ({(time.time() - t0) / 60:.1f} min)")
    if blocked is not None:
        raise blocked
    return counts


def assemble(active, now):
    master = pd.DataFrame(index=pd.date_range(START, END, freq="15min", inclusive="left"))
    master.index.name = "timestamp"
    manifest = {"created": now.isoformat(), "start": str(START), "end_exclusive": str(END),
                "entsoe_py": getattr(entsoe, "__version__", "?"), "pandas": pd.__version__,
                "emission_factors_g_per_kWh": EMISSION_FACTORS,
                "emission_factors_lca_ar5_g_per_kWh": EMISSION_FACTORS_LCA,
                "timezone": TZ, "datasets": {}}
    ds_cols = {}
    for d in active:
        df, areas_used, missing = load_dataset(d)
        info = {"group": d["group"], "unit": d["unit"], "description": d["desc"],
                "areas_used_segments": areas_used, "missing_segments": missing,
                "merged_into_master": d["merge"]}
        if df is None:
            info["rows"] = 0
            manifest["datasets"][d["name"]] = info
            print(f"   {d['name']:24s} EMPTY")
            continue
        df.to_parquet(os.path.join(OUT_DIR, "raw_combined", f"{d['name']}.parquet"))
        info.update(rows=len(df), columns=list(df.columns),
                    first=str(df.index.min()), last=str(df.index.max()))
        manifest["datasets"][d["name"]] = info
        # "DE_LU-AT" (border pair) and "DE_LU" are the same home zone; the
        # 2018 switch DE_AT_LU -> DE_LU is expected -> only report real mixes
        normalized = {a.split("-")[0].replace("DE_AT_LU", "DE_LU") for a in areas_used}
        if len(normalized) > 1:
            print(f"   note: {d['name']} uses several areas: {areas_used}")
        if d["merge"]:
            part = to_15min(master_columns(df))
            ds_cols[d["name"]] = [c for c in part.columns if c not in master.columns]
            master = master.join(part, how="left")

    # derived columns are computed separately on each resolution
    # (hourly = mean of the four quarter-hours, then indicators re-derived)
    master = apply_structural_zeros(master)
    master = fill_a03_price_gaps(master)
    master = clean_load(master)
    master = drop_empty_short_price(master, manifest)
    master = harmonise_currency(master, manifest)
    master = apply_price_bounds(master, manifest)
    hourly = add_derived(master.resample("1h").mean())
    master = add_derived(master)

    master.to_parquet(os.path.join(OUT_DIR, f"{DATASET_NAME}_15min.parquet"))
    hourly.to_parquet(os.path.join(OUT_DIR, f"{DATASET_NAME}_1h.parquet"))
    if WRITE_CSV_1H:
        hourly.to_csv(os.path.join(OUT_DIR, f"{DATASET_NAME}_1h.csv"))
    if WRITE_CSV_15MIN:
        master.to_csv(os.path.join(OUT_DIR, f"{DATASET_NAME}_15min.csv"))
    return master, hourly, manifest, ds_cols


def gap_audit(master, ds_cols):
    """Missing share per series and month, only between the series' first and
    last valid value (structural periods before/after are not gaps)."""
    month = master.index.tz_localize(None).to_period("M")
    rows = []
    for name, cols in ds_cols.items():
        for c in cols:
            if c not in master.columns:
                continue
            col = master[c]
            fv, lv = col.first_valid_index(), col.last_valid_index()
            if fv is None:
                continue
            inside = (master.index >= fv) & (master.index <= lv)
            if col[inside].isna().mean() * 100 > SPARSE_SERIES_PCT:
                continue
            miss = col.isna()[inside].groupby(month[inside]).mean() * 100
            for m, pct in miss[miss > GAP_THRESHOLD_PCT].items():
                rows.append({"dataset": name, "column": c, "month": str(m),
                             "missing_pct": round(float(pct), 1)})
    return pd.DataFrame(rows, columns=["dataset", "column", "month", "missing_pct"])


LAST_RESULTS = {}


class ApiBlocked(Exception):
    pass


def api_check():
    """Tests the ENTSO-E key with ONE request and prints the server's answer."""
    s_ = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=3)).strftime("%Y%m%d0000")
    e_ = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=2)).strftime("%Y%m%d0000")
    r = requests.get("https://web-api.tp.entsoe.eu/api", timeout=60, params={
        "securityToken": ENTSOE_API_KEY or get_api_key(), "documentType": "A44",
        "in_Domain": "10Y1001A1001A82H", "out_Domain": "10Y1001A1001A82H",
        "periodStart": s_, "periodEnd": e_})
    print("HTTP status:", r.status_code)
    print(r.text[:800])
    if r.status_code == 200:
        print("\n-> ENTSO-E access works.")
    elif r.status_code in (401, 403):
        print("\n-> Key blocked/invalid. 1) Wait ~1 h (temporary ban after too many requests);"
              " 2) check https://transparency.entsoe.eu -> My Account Settings -> Web API "
              "Security Token (regenerate if needed, then set ENTSOE_API_KEY).")
    elif r.status_code == 429:
        print("\n-> Too many requests: temporary ban, wait and try again.")
    return r.status_code


def _api_reason(text):
    m = re.search(r"<text>(.*?)</text>", text or "", re.S)
    return " ".join((m.group(1) if m else (text or "")[:200]).split())


OFFLINE = False      # True = rebuild tables from the raw files only (no downloads)
VERBOSE = True       # False = no per-zone quality table in the log (rebuild/finalize)


def rebuild(countries=None):
    """Re-assemble the output tables from the raw files WITHOUT downloading
    (after a change of cleaning or currency rules). ~1-2 min per zone."""
    global OFFLINE, VERBOSE
    prev = (OFFLINE, VERBOSE)
    OFFLINE, VERBOSE = True, False
    try:
        run(countries)
    finally:
        OFFLINE, VERBOSE = prev


def _run_state_path():
    return os.path.join(root_dir(), "audit", "run_state.json")


def _run_state():
    try:
        return _read_json(_run_state_path())
    except Exception:
        return {}


def _mark_done(code):
    st = _run_state()
    st[code] = {"finished": pd.Timestamp.now(tz="UTC").isoformat(), "code_version": CODE_VERSION}
    os.makedirs(os.path.dirname(_run_state_path()), exist_ok=True)
    tmp = _run_state_path() + ".tmp"
    with open(tmp, "w") as f:
        json.dump(st, f, indent=1)
    os.replace(tmp, _run_state_path())


def run(countries=None, force=False):
    """Download/update the given countries one after another, e.g.
    run(NORDIC), run(["DE", "TR"]). Default: COUNTRIES.
    After a disconnect simply call run() again: zones finished within the last
    RESUME_HOURS are skipped, the interrupted zone continues where it stopped.
    force=True processes every zone again."""
    countries = list(countries or COUNTRIES)
    unknown = [c for c in countries if c not in PROFILES]
    if unknown:
        raise ValueError(f"unknown country codes: {unknown}; known: {sorted(PROFILES)}")
    results = {}
    scrub_files()
    state = {} if (force or OFFLINE) else _run_state()
    now_utc = pd.Timestamp.now(tz="UTC")
    done = [c for c in countries if c in state
            and now_utc - pd.Timestamp(state[c]["finished"]) < pd.Timedelta(hours=RESUME_HOURS)]
    if done:
        print(f">>> already updated in the last {RESUME_HOURS} h, skipped ({len(done)}): {' '.join(done)}"
              f"\n    (run(force=True) processes them again)")
    todo = [c for c in countries if c not in done]
    # the day-ahead currency check of a zone compares it with a coupled euro zone
    # (EUR_REFERENCE): process that zone first if both are in this batch
    def _depth(c, seen=()):
        r = EUR_REFERENCE.get(c)
        return 0 if r not in todo or r in seen else 1 + _depth(r, seen + (c,))
    todo = sorted(todo, key=_depth)
    for i, _c in enumerate(todo, 1):
        configure(_c)
        print("\n" + "#" * 78 + f"\n#  {P['name']} ({_c})   zone {i}/{len(todo)}\n" + "#" * 78)
        try:
            results[_c] = main()
            if not OFFLINE and results[_c] and results[_c][0] is not None and not FAILURES:
                _mark_done(_c)                       # with failed months: retried at the next run()
        except ApiBlocked as ex:                     # no point trying the other countries
            print(f"\n!! STOPPED: {ex}\n   Nothing was lost. Run api_check() for details; "
                  "re-run this batch once access works again.")
            break
        except Exception as ex:                      # one country must not stop the rest
            print(f"!! {P['name']} aborted: {type(ex).__name__}: {ex}")
    print("\n>>> Batch finished. Run status() for an overview.")
    global LAST_RESULTS
    LAST_RESULTS = results          # tables of this batch: sb.LAST_RESULTS["TR"][0]


def _keeps_zone(fn):
    """Project-level functions switch zones internally; afterwards the zone
    that was active before is restored (no hidden global side effects)."""
    import functools

    @functools.wraps(fn)
    def wrapper(*a, **k):
        prev, failures = COUNTRY, list(FAILURES)
        try:
            return fn(*a, **k)
        finally:
            configure(prev)
            FAILURES[:] = failures            # configure() clears them; keep the last run's list
    return wrapper


@_keeps_zone
def status(countries=None):
    """Overview WITHOUT downloading: one row per country from its manifest."""
    rows = []
    for c in (countries or NORDIC + CEE + OTHER + TURKEY + CORE):
        configure(c)
        base = resolve_base_dir()
        man_path = os.path.join(base, "output", "manifest.json")
        row = {"code": c, "name": P["name"]}
        if not os.path.exists(man_path):
            row["state"] = "not downloaded yet"
            rows.append(row)
            continue
        man = _read_json(man_path)
        ds = man.get("datasets", {})
        p_da = ds.get("P_DA") or ds.get("EPIAS_MCP") or {}
        row.update(state="ok" if not man.get("failures") else "has failures",
                   updated=str(man.get("created", ""))[:16],
                   data_until=str(p_da.get("last", ""))[:10],
                   datasets_with_data=sum(1 for d in ds.values() if d.get("rows")),
                   failed_segments=len(man.get("failures", [])),
                   gap_months=man.get("gap_months_remaining", 0))
        rows.append(row)
    df = pd.DataFrame(rows).set_index("code")
    return df.fillna("")


# =============================================================================
#  PROJECT LEVEL: one audit + one combined panel over all zones
# =============================================================================
#  Quarterwatt/
#     README.md
#     code/                         quarterwatt.py (+ notebook copy)
#     data/zones/<CODE>_<Country>/  raw/ + output/ of each of the 35 zones
#     data/panel/                   panel_core_1h.parquet (all zones, hourly, long)
#                                   panel_monthly.csv, panel_yearly.csv,
#                                   zones.csv, data_dictionary.csv
#     audit/                        audit_summary.csv, audit_issues.csv, ...
#     fx_rates/                     exchange rates used for currency conversion
#     website/                      (later)
# =============================================================================

ALL_ZONES = CORE + NORDIC + CEE + OTHER + TURKEY

# columns of the combined hourly panel (only those a zone has are filled)
PANEL_CORE_COLS = [
    "P_DA", "P_IDA1", "P_ID_AEP", "P_Imbalance", "Load_Actual", "Load_Forecast_DA",
    "Gen_Total", "VRE_Generation", "Residual_Load", "VRE_Share_of_Load",
    "Gen_Solar", "Gen_Wind_Onshore", "Gen_Wind_Offshore",
    "Net_Import_Physical", "Net_Position",
    "CO2_Intensity_approx_g_per_kWh", "CO2_Intensity_LCA_AR5_g_per_kWh",
    "Gen_Types_Reporting", "WX_Temp_C", "WX_GHI_Wm2", "WX_Wind100m_kmh",
    "Flag_Negative_DA", "Is_Cheapest_Energy_Window", "Is_Public_Holiday", "Is_Weekend",
    "Flag_P_DA_Filled",
]


def panel_dir():
    return os.path.join(root_dir(), "data", "panel")


def _zone_file(code):
    """Path of the hourly table of one zone (None if not downloaded yet)."""
    configure(code)
    path = os.path.join(resolve_base_dir(), "output", f"{DATASET_NAME}_1h.parquet")
    return path if os.path.exists(path) else None


@_keeps_zone
def load_zone(code, columns=None, utc=False):
    """Full hourly table of one zone, e.g. load_zone("BE") or
    load_zone("DE", ["P_DA", "Load_Actual"]). Index: local time (utc=True: UTC)."""
    path = _zone_file(code)
    if path is None:
        raise FileNotFoundError(f"{code}: no hourly table yet, run run(['{code}']) first")
    if columns is not None:
        import pyarrow.parquet as pq
        have = set(pq.read_schema(path).names)
        columns = [c for c in columns if c in have]
    df = pd.read_parquet(path, columns=columns)
    if utc:
        df.index = df.index.tz_convert("UTC")
    return df


def _gen_total(df):
    """Sum of all production types; blank in months in which only part of the
    mix is reported (same rule as the CO2 intensity, e.g. Sweden < 12/2021)."""
    gen = [c for c in df.columns if c.startswith("Gen_") and not c.endswith("_Consumption")
           and c not in ("Gen_Forecast_DA_Total", "Gen_Types_Reporting", "Gen_Total")]
    if not gen:
        return None
    tot = df[gen].sum(axis=1, min_count=1)
    if "Gen_Types_Reporting" in df.columns:
        n = df["Gen_Types_Reporting"].astype("float")
        tot = tot.where(~(n < 0.5 * n.quantile(0.75)))
    return tot


def _longest_run(s):
    """Longest run of identical consecutive non-NaN values (hours)."""
    s = s.dropna()
    if s.empty:
        return 0
    grp = (s != s.shift()).cumsum()
    return int(grp.value_counts().max())


# source limitations that are known and documented -> reported as INFO
KNOWN_LIMITS = {
    ("GR", "coverage", "P_Imbalance"): "ENTSO-E imbalance prices for GR are incomplete",
    ("IE_SEM", "coverage", "Load_Actual"): "EirGrid load on ENTSO-E has gaps and ends 2025-10",
    ("IE_SEM", "freshness", "Load_Actual"): "EirGrid load on ENTSO-E ends 2025-10",
    ("RO", "currency", "P_Imb_Short"): "Romanian deficit prices are structurally far above day-ahead",
    ("RO", "currency", "P_Imb_Long"): "dual pricing until 2021-02: the surplus price is close to zero",
    ("RO", "currency", "P_Imbalance"): "dual pricing until 2021-02: P_Imbalance = surplus price, close to zero",
    ("RO", "currency", "P_BalE_aFRR_Up"): "until 2024-06 upward aFRR prices sit at the regulatory cap "
                                          "(2,000-3,500 RON/MWh, checked on the XML)",
    ("RO", "currency", "P_BalE_mFRR_Up"): "until 2024-06 upward mFRR prices are far above day-ahead (RON, checked on the XML)",
}


def _audit_zone(code):
    """All checks for one zone -> (list of issues, list of yearly rows, info dict)."""
    issues, yearly = [], []

    def add(level, check, msg):
        known = next((t for (z, c, col), t in KNOWN_LIMITS.items()
                      if z == code and c == check and msg.startswith(col)), None)
        if known and level == "WARN":
            level, msg = "INFO", f"{msg} [known: {known}]"
        issues.append({"zone": code, "level": level, "check": check, "message": msg})

    path = _zone_file(code)
    info = {"zone": code, "name": P["name"], "tz": TZ}
    if path is None:
        add("WARN", "file", "no hourly table found (not downloaded yet?) -> not in the panel")
        return issues, yearly, info
    df = pd.read_parquet(path)
    info.update(rows=len(df), columns=df.shape[1],
                first=str(df.index.min())[:16], last=str(df.index.max())[:16])

    # --- 1) time index ------------------------------------------------------
    if df.index.tz is None:
        add("FAIL", "index", "index has no time zone")
        return issues, yearly, info
    utc = df.index.tz_convert("UTC")
    if utc.duplicated().any():
        add("FAIL", "index", f"{int(utc.duplicated().sum())} duplicated hours")
    if not utc.is_monotonic_increasing:
        add("FAIL", "index", "index not sorted")
    full = pd.date_range(utc.min(), utc.max(), freq="1h")
    missing = len(full.difference(utc))
    if missing:
        add("WARN", "index", f"{missing} hours missing in the time index")

    # --- 2) coverage of key series (inside each series' own range) ----------
    def cov(col):
        if col not in df.columns:
            return None
        s = df[col]
        fv, lv = s.first_valid_index(), s.last_valid_index()
        if fv is None:
            return 0.0, None, None
        inner = s.loc[fv:lv]
        return float(inner.notna().mean()), fv, lv

    key = ["P_DA", "Load_Actual", "P_Imbalance", "WX_Temp_C", "CO2_Intensity_LCA_AR5_g_per_kWh"]
    for col in key:
        c = cov(col)
        if c is None:
            lvl = "FAIL" if col == "P_DA" else "INFO"
            add(lvl, "coverage", f"{col} not available")
            continue
        share, fv, lv = c
        info[f"{col}_from"] = str(fv)[:10] if fv is not None else ""
        info[f"{col}_until"] = str(lv)[:10] if lv is not None else ""
        info[f"{col}_coverage"] = round(share * 100, 1)
        if share < 0.5:
            add("FAIL" if col == "P_DA" else "WARN", "coverage",
                f"{col}: only {share:.0%} of hours filled between {str(fv)[:10]} and {str(lv)[:10]}")
        elif share < 0.95:
            add("WARN", "coverage", f"{col}: {share:.1%} filled between {str(fv)[:10]} and {str(lv)[:10]}")
    # stale series: price should reach (almost) today
    if "P_DA" in df.columns and df["P_DA"].last_valid_index() is not None:
        lag = (pd.Timestamp.now(tz="UTC") - df["P_DA"].last_valid_index().tz_convert("UTC")).days
        if lag > 7:
            add("WARN", "freshness", f"P_DA ends {lag} days ago")
    if "Load_Actual" in df.columns and df["Load_Actual"].last_valid_index() is not None:
        lag = (pd.Timestamp.now(tz="UTC") - df["Load_Actual"].last_valid_index().tz_convert("UTC")).days
        if lag > 30:
            add("WARN", "freshness", f"Load_Actual ends {lag} days ago")

    # --- 3) plausibility of values -----------------------------------------
    if "P_DA" in df.columns:
        p = df["P_DA"]
        bad = int(((p < -500) | (p > 5000)).sum())
        if bad:
            add("WARN", "range", f"P_DA outside [-500, 5000] EUR/MWh in {bad} hours "
                                 f"(min {p.min():.0f}, max {p.max():.0f})")
        run_ = _longest_run(p.round(2))
        if run_ > 72:
            add("WARN", "stuck", f"P_DA identical for {run_} consecutive hours")
    # currency: every other price column must have the level of P_DA
    if "P_DA" in df.columns:
        mon = df.index.tz_localize(None).to_period("M")
        ref = df["P_DA"].abs().groupby(mon).median()
        ref = ref.where(ref >= REF_MIN_LEVEL)          # months with near-zero prices: not judged
        # balancing energy prices are far from P_DA by nature (down ~0, up spikes):
        # they get their own, wider check below
        price_cols = [c for c in df.columns if (c.startswith("P_") and c not in
                      ("P_DA", "P_DA_TRY", "P_SMF_TRY") and not c.endswith("_TRY")
                      and not c.startswith("P_BalE_"))]
        for col in price_cols:
            lev = df[col].abs().groupby(mon).median()
            r = (lev / ref).dropna()
            low = r[(r < 0.15) & ((ref - lev).reindex(r.index) > 40)]
            if len(low) >= max(6, 0.1 * len(r)):
                add("WARN", "currency", f"{col}: level <0.15x P_DA in {len(low)} months "
                    f"({low.index.min()} .. {low.index.max()}) -> converted twice / wrong unit?")
            bad = r[(r > 3) & ((lev - ref).reindex(r.index) > 40)]
            if len(bad):
                # a few months = extreme imbalance prices; many = other currency
                add("WARN" if len(bad) >= max(6, 0.1 * len(r)) else "INFO", "currency", f"{col}: level >3x P_DA in {len(bad)} months "
                    f"({bad.index.min()} .. {bad.index.max()}, max {bad.max():.1f}x) -> other currency?")
    if "P_DA" in df.columns:
        mon = df.index.tz_localize(None).to_period("M")
        ref = df["P_DA"].abs().groupby(mon).median()
        ref = ref.where(ref >= REF_MIN_LEVEL)
        for col in [c for c in df.columns if c.startswith("P_BalE_") and c.endswith("_Up")]:
            r = (df[col].abs().groupby(mon).median() / ref).dropna()
            bad = r[r > 8]
            if len(bad) >= max(6, 0.25 * len(r)):
                add("WARN", "currency", f"{col}: median >8x P_DA in {len(bad)} of {len(r)} months "
                    f"({bad.index.min()} .. {bad.index.max()}) -> other currency?")
    if code == "TR" and {"P_DA", "P_DA_TRY"} <= set(df.columns):
        rates = ecb_rates("TRY")
        if rates is not None:
            mon = df.index.tz_localize(None).to_period("M")
            implied = (df["P_DA_TRY"] / df["P_DA"]).replace([np.inf, -np.inf], np.nan).groupby(mon).median()
            dev = (implied / rates.reindex(implied.index) - 1).abs().dropna()
            bad = dev[dev > 0.1]
            if len(bad):
                add("WARN", "currency", f"TR: EUR price implies a TRY rate >10% off the ECB rate in "
                    f"{len(bad)} months ({bad.index.min()} .. {bad.index.max()})")
    if "Load_Actual" in df.columns:
        L = df["Load_Actual"]
        nonpos = int((L <= 0).sum())
        if nonpos:
            add("WARN", "range", f"Load_Actual <= 0 in {nonpos} hours")
        med = L.median()
        jumps = int((L.diff().abs() > 0.5 * med).sum()) if med and med > 0 else 0
        if jumps > 10:                    # spikes are cleaned; what remains are level steps
            add("INFO", "jumps", f"Load_Actual jumps >50% of median in {jumps} hours")
    if "WX_Temp_C" in df.columns:
        t = df["WX_Temp_C"]
        if ((t < -45) | (t > 48)).any():
            add("WARN", "range", f"WX_Temp_C outside [-45, 48] (min {t.min():.1f}, max {t.max():.1f})")
    for col in ["CO2_Intensity_approx_g_per_kWh", "CO2_Intensity_LCA_AR5_g_per_kWh"]:
        if col in df.columns:
            c = df[col]
            if ((c < 0) | (c > 1300)).any():
                add("WARN", "range", f"{col} outside [0, 1300] (min {c.min():.0f}, max {c.max():.0f})")
    if "P_Imbalance" in df.columns and "P_DA" in df.columns:
        both = df[["P_Imbalance", "P_DA"]].dropna()
        if len(both) > 1000:
            corr = both.corr().iloc[0, 1]
            info["corr_Imbalance_DA"] = round(float(corr), 2)
            if corr < 0.3:                # weak in several markets by design (dual pricing)
                add("INFO", "consistency", f"P_Imbalance hardly correlated with P_DA (r={corr:.2f})")

    # --- 4) time-zone sanity: solar must peak around midday ------------------
    # (all zones are indexed in Europe/Berlin time except TR, so western zones
    #  such as PT/IE peak ~14:00 and eastern ones ~11:00 -> tolerance 9-15)
    sol_prof = None
    if "Gen_Solar" in df.columns and df["Gen_Solar"].notna().sum() > 24 * 60:
        sol_prof = df["Gen_Solar"].groupby(df.index.hour).mean()
        # zones with (almost) no solar have no daily shape -> use irradiation
        if not (sol_prof.max() > 1 and sol_prof.min() < 0.2 * sol_prof.max()):
            sol_prof = None
    if sol_prof is not None:
        prof = sol_prof
        peak = int(prof.idxmax())
        info["solar_peak_hour"] = peak
        if not 9 <= peak <= 15:
            add("WARN", "timezone", f"solar generation peaks at {peak}:00 index time (expected 9-15)")
    elif "WX_GHI_Wm2" in df.columns and df["WX_GHI_Wm2"].notna().sum() > 24 * 60:
        peak = int(df["WX_GHI_Wm2"].groupby(df.index.hour).mean().idxmax())
        info["solar_peak_hour"] = peak
        if not 9 <= peak <= 15:
            add("WARN", "timezone", f"irradiation peaks at {peak}:00 index time (expected 9-15)")

    # --- 5) yearly key figures + generation/load balance ---------------------
    year = df.index.year
    gt = _gen_total(df)
    for y, part in df.groupby(year):
        row = {"zone": code, "year": int(y), "hours": len(part)}
        if "P_DA" in part.columns:
            row.update(P_DA_mean=part["P_DA"].mean(), P_DA_hours=int(part["P_DA"].notna().sum()),
                       neg_hours=int((part["P_DA"] < 0).sum()))
        if "Load_Actual" in part.columns:
            row["Load_TWh"] = part["Load_Actual"].sum(min_count=1) / 1e6
            row["Load_hours"] = int(part["Load_Actual"].notna().sum())
        if gt is not None:
            row["Gen_TWh"] = gt.loc[part.index].sum(min_count=1) / 1e6
            row["Gen_hours"] = int(gt.loc[part.index].notna().sum())
        if "Gen_Types_Reporting" in part.columns:
            row["gen_types"] = part["Gen_Types_Reporting"].max()
        if "CO2_Intensity_LCA_AR5_g_per_kWh" in part.columns:
            row["CO2_LCA_mean"] = part["CO2_Intensity_LCA_AR5_g_per_kWh"].mean()
        yearly.append(row)
    yr = pd.DataFrame(yearly)
    if {"Gen_TWh", "Load_TWh"} <= set(yr.columns):
        full_years = yr[(yr["Load_hours"] > 8000) & (yr["Gen_hours"] > 8000)].copy()
        full_years["ratio"] = full_years["Gen_TWh"] / full_years["Load_TWh"]
        odd = full_years[(full_years["ratio"] < 0.2) | (full_years["ratio"] > 4)]
        if len(odd):
            add("WARN", "balance", "generation/load ratio implausible in years "
                + ", ".join(f"{int(r.year)} ({r.ratio:.2f})" for r in odd.itertuples()))
        # sudden level shifts of load between full years (reporting changes)
        ly = yr[yr["Load_hours"] > 8000].set_index("year")["Load_TWh"].dropna()
        ch = ly.pct_change().abs()
        if (ch > 0.25).any():
            add("WARN", "break", "Load changes >25% between years "
                + ", ".join(f"{int(y)}" for y in ch[ch > 0.25].index) + " (check reporting)")

    # --- 6) download log --------------------------------------------------------
    man_path = os.path.join(resolve_base_dir(), "output", "manifest.json")
    if os.path.exists(man_path):
        man = _read_json(man_path)
        nf = len([f for f in man.get("failures", [])        # removed datasets don't count
                  if not str(f.get("dataset", "")).startswith("BalE_Volume")])
        info["failed_segments"] = nf
        info["gap_months"] = man.get("gap_months_remaining", 0)
        if nf:
            add("INFO", "download", f"{nf} segments failed at the source (see manifest.json)")
        nr = len(man.get("rejected_by_source", []))
        if nr:
            add("INFO", "download", f"{nr} months refused by the source (HTTP 400, not asked again; "
                                    f"see manifest.json 'rejected_by_source')")
        for col in man.get("currency_check_skipped", []):
            add("WARN", "currency", f"{col}: currency check skipped at build time "
                f"-> run finalize(['{code}'])")
        for col in man.get("currency_unconverted", []):
            add("FAIL", "currency", f"{col}: currency check/conversion could not run (no exchange "
                "rates at build time) -> rebuild this zone")
        for cv in man.get("currency_conversions", []):
            if cv.get("months_without_rate") or cv.get("values_without_rate_set_NaN"):
                add("WARN", "currency", f"{cv['column']}: values in months without an exchange rate "
                    "were set to NaN -> check fx_rates, then finalize(['" + code + "'])")
            add("INFO", "currency", f"{cv['column']} converted from {cv['currency']} "
                f"{cv['from']} .. {cv['to']} ({cv['how']})")
    return issues, yearly, info


@_keeps_zone
def audit(zones=None):
    """Checks every downloaded zone and writes the audit files into the
    project folder. Changes no data. Returns the summary table."""
    zones = list(zones or ALL_ZONES)
    out = os.path.join(root_dir(), "audit")
    os.makedirs(out, exist_ok=True)
    all_issues, all_yearly, infos, prices = [], [], [], {}
    for code in zones:
        print(f"   checking {code} ...", end="\r")
        try:
            iss, yr, info = _audit_zone(code)
        except Exception as ex:
            iss, yr, info = ([{"zone": code, "level": "FAIL", "check": "crash",
                               "message": f"{type(ex).__name__}: {ex}"}], [], {"zone": code})
        all_issues += iss
        all_yearly += yr
        infos.append(info)
        path = _zone_file(code)
        if path is not None:
            try:
                s = pd.read_parquet(path, columns=["P_DA"])["P_DA"]
                s.index = s.index.tz_convert("UTC")
                prices[code] = s
            except Exception:
                pass

    # cross-zone: two zones with (almost) the same price every hour -> mapping error?
    codes = sorted(prices)
    for i, a in enumerate(codes):
        for b in codes[i + 1:]:
            both = pd.concat([prices[a], prices[b]], axis=1, join="inner").dropna()
            if len(both) > 10000:
                same = float((both.iloc[:, 0].round(2) == both.iloc[:, 1].round(2)).mean())
                if same > 0.995:
                    all_issues.append({"zone": f"{a}+{b}", "level": "INFO", "check": "identical",
                                       "message": f"day-ahead prices identical in {same:.1%} of hours "
                                                  "(normal for coupled zones, otherwise a mapping error)"})

    # cross-zone: monthly price level far above the European median -> wrong
    # currency or wrong zone mapping (catches what a single-zone view cannot)
    level_detail = []
    if prices:
        med = pd.DataFrame({z: s_.groupby(s_.index.tz_localize(None).to_period("M")).median()
                            for z, s_ in prices.items()})
        cont = [c for c in med.columns if c in CORE + CEE]
        europe = med[cont].median(axis=1) if cont else med.median(axis=1)
        for z in med.columns:
            if z == "TR":
                continue
            hi = med[z][(med[z] > 1.7 * europe.clip(lower=10)) & (med[z] - europe > 25)]
            if len(hi):
                nbs = [n for n in auto_neighbours(z) if n in med.columns]
                for m_ in hi.index:
                    level_detail.append({"zone": z, "month": str(m_), "zone_median": round(med.at[m_, z], 1),
                                         "continental_median": round(europe[m_], 1),
                                         **{f"nb_{n}": round(med.at[m_, n], 1) for n in nbs}})
                all_issues.append({"zone": z, "level": "WARN" if len(hi) >= 3 else "INFO", "check": "level",
                                   "message": f"P_DA >1.7x continental median in {len(hi)} months "
                                              f"({hi.index.min()} .. {hi.index.max()}) -> currency/mapping?"})
        med.round(2).to_csv(os.path.join(out, "monthly_median_prices.csv"))
        if level_detail:
            ld = pd.DataFrame(level_detail)
            ld.to_csv(os.path.join(out, "audit_level_detail.csv"), index=False)
            with pd.option_context("display.width", 220, "display.max_columns", 30, "display.max_rows", 200):
                print("\nLEVEL DETAIL (zone vs continental median and neighbours, monthly medians EUR/MWh):")
                print(ld.dropna(axis=1, how="all").to_string(index=False))

    issues = pd.DataFrame(all_issues, columns=["zone", "level", "check", "message"])
    order = {"FAIL": 0, "WARN": 1, "INFO": 2}
    summ = pd.DataFrame(infos).set_index("zone")
    lvl = issues[issues["zone"].isin(summ.index)].groupby("zone")["level"].apply(
        lambda s: min(s, key=order.get))
    summ.insert(0, "result", summ.index.map(lambda z: {"FAIL": "FAIL", "WARN": "WARN"}.get(lvl.get(z), "PASS")))
    summ.insert(1, "n_warn", summ.index.map(lambda z: int(((issues.zone == z) & (issues.level == "WARN")).sum())))
    summ.to_csv(os.path.join(out, "audit_summary.csv"))
    issues.sort_values(["level", "zone"], key=lambda c: c.map(order) if c.name == "level" else c) \
          .to_csv(os.path.join(out, "audit_issues.csv"), index=False)
    pd.DataFrame(all_yearly).round(3).to_csv(os.path.join(out, "audit_yearly.csv"), index=False)

    print(" " * 40)
    show = ["result", "n_warn", "first", "last", "P_DA_coverage", "Load_Actual_coverage",
            "P_Imbalance_from", "solar_peak_hour", "failed_segments"]
    show = [c for c in show if c in summ.columns]
    with pd.option_context("display.max_rows", 100, "display.width", 220, "display.max_columns", 20,
                           "display.max_colwidth", 200):
        print(summ[show].fillna(""))
        print("\nFINDINGS (FAIL / WARN):")
        w = issues[issues.level.isin(["FAIL", "WARN"])].sort_values(
            ["level", "zone"], key=lambda c: c.map(order) if c.name == "level" else c)
        print(w.to_string(index=False) if len(w) else "   none")
        i = issues[issues.level == "INFO"]
        if len(i):
            print(f"\nINFO ({len(i)}, for the record):")
            print(i.to_string(index=False))
    print(f"\n>>> audit files: {out}")
    return summ, issues


# --------------------------------------------------------------------------- #
#  combined panel
# --------------------------------------------------------------------------- #

def _monthly(df, code, freq="M"):
    """Indicators of one zone per local calendar month (freq="M") or year
    (freq="Y"), always computed from the hourly data (correct min/max/std,
    load-weighted means)."""
    m = df.index.tz_localize(None).to_period(freq)
    g = df.groupby(m)
    out = pd.DataFrame(index=g.size().index)
    out["hours"] = g.size()
    if "P_DA" in df.columns:
        p = df["P_DA"]
        out["P_DA_hours"] = g["P_DA"].count()
        out["P_DA_mean"] = g["P_DA"].mean()
        out["P_DA_std"] = g["P_DA"].std()
        out["P_DA_min"] = g["P_DA"].min()
        out["P_DA_max"] = g["P_DA"].max()
        out["neg_price_hours"] = (p < 0).groupby(m).sum()
        peak = (df.index.day_of_week < 5) & (df.index.hour >= 8) & (df.index.hour < 20)
        out["P_DA_peak"] = p.where(peak).groupby(m).mean()
        out["P_DA_offpeak"] = p.where(~peak).groupby(m).mean()
        day = df.index.tz_localize(None).normalize()
        spread = p.groupby(day).agg(lambda s: s.max() - s.min() if s.notna().sum() >= 20 else np.nan)
        out["daily_spread_mean"] = spread.groupby(spread.index.to_period(freq)).mean()
        if "Is_Cheapest_Energy_Window" in df.columns:
            out["P_DA_cheapest15_mean"] = p.where(df["Is_Cheapest_Energy_Window"] == 1).groupby(m).mean()
        if "Load_Actual" in df.columns:
            ok = p.notna() & df["Load_Actual"].notna()
            out["P_DA_loadweighted"] = ((p * df["Load_Actual"]).where(ok).groupby(m).sum()
                                        / df["Load_Actual"].where(ok).groupby(m).sum())
    if "P_Imbalance" in df.columns:
        out["P_Imbalance_mean"] = g["P_Imbalance"].mean()
        if "P_DA" in df.columns:
            out["Imb_minus_DA_abs_mean"] = (df["P_Imbalance"] - df["P_DA"]).abs().groupby(m).mean()
    if "Load_Actual" in df.columns:
        out["Load_mean_MW"] = g["Load_Actual"].mean()
        out["Load_TWh"] = g["Load_Actual"].sum(min_count=1) / 1e6
    if "VRE_Generation" in df.columns and "Load_Actual" in df.columns:
        ok = df["VRE_Generation"].notna() & df["Load_Actual"].notna()
        out["VRE_share"] = (df["VRE_Generation"].where(ok).groupby(m).sum()
                            / df["Load_Actual"].where(ok).groupby(m).sum())
    for col, name in [("CO2_Intensity_LCA_AR5_g_per_kWh", "CO2_LCA"),
                      ("CO2_Intensity_approx_g_per_kWh", "CO2_direct")]:
        if col in df.columns:
            out[f"{name}_mean"] = g[col].mean()
            if "Load_Actual" in df.columns:
                ok = df[col].notna() & df["Load_Actual"].notna()
                out[f"{name}_loadweighted"] = ((df[col] * df["Load_Actual"]).where(ok).groupby(m).sum()
                                               / df["Load_Actual"].where(ok).groupby(m).sum())
    if "Net_Import_Physical" in df.columns:
        out["Net_Import_TWh"] = g["Net_Import_Physical"].sum(min_count=1) / 1e6
    if "WX_Temp_C" in df.columns:
        out["Temp_mean_C"] = g["WX_Temp_C"].mean()
    out = out.replace([np.inf, -np.inf], np.nan)
    out.index = out.index.astype(str)
    out.index.name = "month" if freq == "M" else "year"
    out.insert(0, "zone", code)
    out.insert(1, "name", P["name"])
    return out.reset_index()


COLUMN_DOCS = [  # (prefix or exact name, meaning, unit)  - first match wins
    ("P_DA", "Day-ahead price of the zone (SDAC / EPİAŞ PTF for TR)", "EUR/MWh"),
    ("P_DA_", "Day-ahead price of a neighbouring zone", "EUR/MWh"),
    ("P_DA_TRY", "Day-ahead price in Turkish lira", "TRY/MWh"),
    ("P_IDA", "Intraday auction price", "EUR/MWh"),
    ("P_ID_AEP", "Intraday average price (netztransparenz ID-AEP)", "EUR/MWh"),
    ("P_Imbalance", "Imbalance price, single continuous series (reBAP / Elia / ENTSO-E / SMF)", "EUR/MWh"),
    ("P_reBAP", "German reBAP (netztransparenz)", "EUR/MWh"),
    ("P_Imb_", "Imbalance price from ENTSO-E", "EUR/MWh"),
    ("P_SMF", "Türkiye system marginal price", "EUR/MWh"),
    ("P_BalE_", "Price of activated balancing energy (ENTSO-E 17.1.F), product and direction in the name", "EUR/MWh"),
    ("Elia_", "Elia open data (Belgian imbalance)", "EUR/MWh or MW"),
    ("Load_Actual", "Actual total load", "MW"),
    ("Load_Forecast", "Load forecast", "MW"),
    ("Load_Forecast_Error", "Actual minus day-ahead forecast load", "MW"),
    ("Gen_Types_Reporting", "Number of production types reported in that month", "count"),
    ("Gen_Forecast_DA_Total", "Day-ahead generation forecast, total", "MW"),
    ("Gen_Total", "Sum of all reported generation types", "MW"),
    ("Gen_", "Actual generation of that production type", "MW"),
    ("Solar_Forecast", "Solar forecast", "MW"),
    ("Wind_", "Wind forecast or forecast error", "MW"),
    ("VRE_Generation", "Solar + wind generation", "MW"),
    ("VRE_Share_of_Load", "VRE generation / load", "share"),
    ("Residual_Load", "Load minus solar and wind", "MW"),
    ("Net_Import_Physical", "Physical imports minus exports over all reporting borders", "MW"),
    ("N_Borders_Reporting", "Borders with flow data in that hour", "count"),
    ("Net_Position", "Net position (market based)", "MW"),
    ("Flow_Import_", "Physical import from neighbour", "MW"),
    ("Flow_Export_", "Physical export to neighbour", "MW"),
    ("CO2_Intensity_approx", "Direct CO2 intensity of domestic generation (approx.)", "g/kWh"),
    ("CO2_Intensity_LCA", "Life-cycle CO2 intensity (IPCC AR5 medians)", "g/kWh"),
    ("Imbalance_Vol", "System imbalance volume", "MW"),
    ("Res_", "Reserve (balancing) data", "MW or EUR/MW"),
    ("RL_", "regelleistung.net auction results", "EUR/MW or EUR/MWh"),
    ("NTP_", "netztransparenz.de", "various"),
    ("WX_Temp_C", "Population-weighted air temperature (ERA5)", "°C"),
    ("WX_GHI", "Population-weighted global horizontal irradiation (ERA5)", "W/m²"),
    ("WX_Wind", "Population-weighted wind speed at 100 m (ERA5)", "km/h"),
    ("WX_Cloud", "Population-weighted cloud cover (ERA5)", "%"),
    ("ES_", "Eurostat non-household electricity price (nrg_pc_205), half-yearly", "EUR/kWh"),
    ("ESC_", "Eurostat price components (nrg_pc_205_c), half-yearly", "EUR/kWh"),
    ("Gas_", "Natural gas price (FRED, monthly)", "see name"),
    ("Coal_", "Coal price (FRED, monthly)", "see name"),
    ("Brent", "Brent crude (FRED, monthly)", "see name"),
    ("USD_per_EUR", "Exchange rate (FRED)", "USD/EUR"),
    ("TR_", "EPİAŞ (Türkiye) market data", "see name"),
    ("Flag_Negative_DA", "1 if day-ahead price < 0", "0/1"),
    ("Flag_P_DA_Filled", "Share of the interval whose P_DA was filled from the last published price (short gaps, curveType A03)", "0-1"),
    ("Is_Cheapest_Energy_Window", "1 if hour is among the cheapest 15% of the day", "0/1"),
    ("Is_", "Calendar flag", "0/1"),
    ("Hour", "Hour of day, local time", ""), ("Weekday", "0 = Monday", ""), ("Month", "Month", ""),
]


# Licence / redistribution status per column prefix (longest match wins).
# raw = may the hourly values themselves be republished?  Conservative:
# anything not clearly open is "derived only" until verified.
LICENCE_RULES = [
    ("", "unknown - check source", False),
    ("P_DA", "ENTSO-E / power exchange prices - derived indicators only", False),
    ("P_IDA", "ENTSO-E / power exchange prices - derived indicators only", False),
    ("P_ID_AEP", "netztransparenz.de - derived only, TSO consent required", False),
    ("P_reBAP", "netztransparenz.de - derived only, TSO consent required", False),
    ("NTP_", "netztransparenz.de - derived only, TSO consent required", False),
    ("RL_", "regelleistung.net - derived only, TSO consent required", False),
    ("P_Imb", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("P_BalE_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("Load_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("Gen_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("Flow_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("Net_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("Imbalance_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("Solar_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("Wind_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("Elia_", "Elia Open Data, CC BY 4.0", True),
    ("WX_", "ERA5 via Open-Meteo, CC BY 4.0 (free API: non-commercial use)", True),
    ("ES_", "Eurostat, reuse with attribution", True),
    ("ESC_", "Eurostat, reuse with attribution", True),
    ("Gas_", "FRED / IMF, attribution", True), ("Coal_", "FRED / IMF, attribution", True),
    ("Brent", "FRED / IMF, attribution", True), ("USD_per_EUR", "FRED, attribution", True),
    ("TR_", "EPİAŞ, attribution (to verify)", False), ("P_SMF", "EPİAŞ, attribution (to verify)", False),
    ("Imb_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("Res_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("Cap_", "ENTSO-E (check free re-use list) - derived only until verified", False),
    ("N_Borders_Reporting", "Quarterwatt bookkeeping", True),
    ("CO2_", "Quarterwatt derived (CC BY 4.0, cite sources)", True),
    ("VRE_", "sum/ratio of ENTSO-E series - aggregated indicators only", False),
    ("Residual_Load", "difference of ENTSO-E series - aggregated indicators only", False),
    ("Flag_", "Quarterwatt derived (CC BY 4.0, cite sources)", True),
    ("Is_", "calendar", True), ("Hour", "calendar", True), ("Weekday", "calendar", True),
    ("Month", "calendar", True),
]


def licence_of(col):
    """(licence text, raw values republishable?) for a column name."""
    best = max((r for r in LICENCE_RULES if col.startswith(r[0])), key=lambda r: len(r[0]))
    return best[1], best[2]


def _describe(col):
    best = None
    for key, txt, unit in COLUMN_DOCS:
        if col == key:
            return txt, unit
        if col.startswith(key) and (best is None or len(key) > len(best[0])):
            best = (key, txt, unit)
    return (best[1], best[2]) if best else ("", "")


@_keeps_zone
def build_panel(zones=None):
    """Combines all downloaded zones into ONE project folder (see header).
    Original per-country files are not touched. Returns (hourly panel, monthly)."""
    import pyarrow.parquet as pq
    zones = list(zones or ALL_ZONES)
    out = panel_dir()
    os.makedirs(out, exist_ok=True)
    hourly, monthly, yearly, zinfo, colzones = [], [], [], [], {}
    for code in zones:
        path = _zone_file(code)
        if path is None:
            print(f"   {code}: not downloaded, skipped")
            continue
        print(f"   {code} ...", end="\r")
        for c in pq.read_schema(path).names + ["Gen_Total"]:
            if not c.startswith("__index_level"):
                colzones.setdefault(c, []).append(code)
        df = pd.read_parquet(path)
        gt = _gen_total(df)
        if gt is not None:
            df["Gen_Total"] = gt
        monthly.append(_monthly(df, code, "M"))
        yearly.append(_monthly(df, code, "Y"))
        # yearly = from monthly sums / hour-weighted means
        h = df[[c for c in PANEL_CORE_COLS if c in df.columns]].astype("float32")
        h.index = h.index.tz_convert("UTC")
        h.index.name = "timestamp_utc"
        h = h.reset_index()
        h.insert(1, "zone", code)
        hourly.append(h)
        zinfo.append({"zone": code, "name": P["name"], "tz": TZ,
                      "folder": f"data/zones/{zone_folder_name(code)}",
                      "first": str(df.index.min())[:10], "last": str(df.index.max())[:10],
                      "price_from": str(df["P_DA"].first_valid_index())[:10] if "P_DA" in df else "",
                      "price_until": str(df["P_DA"].last_valid_index())[:10] if "P_DA" in df else "",
                      "n_columns": df.shape[1],
                      "group": next(n for n, g in [("CORE", CORE), ("NORDIC", NORDIC), ("CEE", CEE),
                                                   ("OTHER", OTHER), ("TURKEY", TURKEY)] if code in g)})
        del df
    if not hourly:
        print("nothing downloaded yet")
        return None, None
    panel = pd.concat(hourly, ignore_index=True)
    panel["zone"] = panel["zone"].astype("category")
    panel.to_parquet(os.path.join(out, "panel_core_1h.parquet"), index=False)

    mon = pd.concat(monthly, ignore_index=True)
    mon.round(4).to_csv(os.path.join(out, "panel_monthly.csv"), index=False)
    yr = pd.concat(yearly, ignore_index=True)
    yr.round(4).to_csv(os.path.join(out, "panel_yearly.csv"), index=False)
    pd.DataFrame(zinfo).to_csv(os.path.join(out, "zones.csv"), index=False)

    dd = []
    for c in sorted(colzones):
        txt, unit = _describe(c)
        zs = colzones[c]
        lic, raw_ok = licence_of(c)
        dd.append({"column": c, "description": txt, "unit": unit, "licence": lic,
                   "raw_republishable": raw_ok, "n_zones": len(zs),
                   "zones": "ALL" if len(zs) == len(zinfo) else " ".join(zs),
                   "in_core_panel": c in PANEL_CORE_COLS})
    pd.DataFrame(dd).to_csv(os.path.join(out, "data_dictionary.csv"), index=False)

    print(" " * 40)
    print(f"hourly panel : {len(panel):,} rows x {panel.shape[1]} columns, {panel.zone.nunique()} zones "
          f"({panel.memory_usage(deep=True).sum() / 1e6:.0f} MB in memory)")
    print(f"monthly      : {len(mon):,} rows | yearly: {len(yr):,} rows | columns documented: {len(dd)}")
    print(f">>> project folder: {out}")
    return panel, mon


def finalize(rebuild_zones=None):
    """One cell for everything after a code change:
    exchange rates -> offline rebuild of `rebuild_zones` (no downloads) ->
    audit -> combined panel (only if the audit has no FAIL)."""
    scrub_files()
    if rebuild_zones:
        print("=" * 78 + "\n1) exchange rates")
        if not fx_check():
            print("!! stopped: exchange rates missing, nothing was changed. Try again later.")
            return None
        print("=" * 78 + f"\n2) rebuild {list(rebuild_zones)} (no downloads)")
        rebuild(rebuild_zones)
    print("=" * 78 + "\n3) audit")
    summary, issues = audit()
    n_fail = int((issues.level == "FAIL").sum())
    if n_fail:
        print(f"\n!! {n_fail} FAIL findings -> panel NOT built. Send this output.")
        return summary, issues, None
    print("=" * 78 + "\n4) combined panel")
    panel, monthly = build_panel()
    if panel is None:
        print("!! nothing to combine (no zone downloaded yet)")
        return summary, issues, None
    write_readme()
    version = {"data_version": pd.Timestamp.now(tz="Europe/Berlin").strftime("%Y.%m"),
               "built": pd.Timestamp.now(tz="Europe/Berlin").isoformat(timespec="minutes"),
               "code_version": CODE_VERSION, "zones": summary["result"].to_dict(),
               "panel_rows": int(len(panel)),
               "data_until": {z: str(v)[:10] for z, v in summary.get("last", pd.Series(dtype=str)).dropna().items()}}
    with open(os.path.join(panel_dir(), "VERSION.json"), "w") as f:
        json.dump(version, f, indent=1)
    res = summary["result"].value_counts().to_dict()
    print("=" * 78 + f"\nDONE  data version {version['data_version']} (code {CODE_VERSION})  zones: {res}"
          f"  | panel rows: {len(panel):,}")
    return summary, issues, monthly


def load_panel(columns=None, zones=None):
    """Combined hourly panel (long format: timestamp_utc, zone, ...).
    e.g. load_panel(["P_DA", "Load_Actual"], zones=["DE", "BE"])"""
    path = os.path.join(panel_dir(), "panel_core_1h.parquet")
    cols = None if columns is None else ["timestamp_utc", "zone"] + list(columns)
    filt = None if zones is None else [("zone", "in", list(zones))]
    return pd.read_parquet(path, columns=cols, filters=filt)


# =============================================================================
#  README of the main folder
# =============================================================================
@_keeps_zone
def write_readme():
    root = root_dir()
    rows = []
    for code in ALL_ZONES:
        folder, prefix = zone_location(code)
        ok = os.path.exists(os.path.join(folder, "output", f"{prefix}_1h.parquet"))
        rows.append(f"| {code} | {PROFILES[code]['name']} | data/zones/{zone_folder_name(code)}/ | "
                    f"{'yes' if ok else 'no'} |")
    text = f"""# Quarterwatt - European electricity market data (2015 onwards)

Hourly and 15-minute data for 35 bidding zones (EU, Norway, Switzerland, Türkiye):
day-ahead, intraday and imbalance prices, load, generation by type, flows,
reserves, weather, CO2 intensity, Eurostat end-user prices, fuel prices.
Built by `code/quarterwatt.py` from public sources (ENTSO-E, netztransparenz,
regelleistung.net, Elia, EPİAŞ, Open-Meteo/ERA5, Eurostat, FRED, ECB).

Last update of this file: {pd.Timestamp.now():%Y-%m-%d %H:%M}

## Where is what

| Folder | Content |
|---|---|
| `code/` | `quarterwatt.py` (all logic) and the Colab notebook |
| `data/zones/<CODE>_<Country>/raw/` | raw month files per data set (data lake, never edited) |
| `data/zones/<CODE>_<Country>/output/` | `Quarterwatt_<CODE>_1h.parquet/.csv`, `_15min.parquet`, `manifest.json`, quality and gap reports |
| `data/panel/` | **start here**: `panel_core_1h.parquet` (all zones, hourly), `panel_monthly.csv`, `panel_yearly.csv`, `zones.csv`, `data_dictionary.csv` (meaning, unit, licence of every column), `VERSION.json` (data version) |
| `data/reference/` | static reference data: BDEW standard load profiles, 50 German SME load profiles (Braeuer et al. 2020, CC BY 4.0), German levies 2015-2026 |
| `audit/` | data checks: `audit_summary.csv` (PASS/WARN/FAIL per zone), `audit_issues.csv` |
| `fx_rates/` | exchange rates used to convert local-currency prices to EUR |
| `website/` | website exports (later) |

## Use in Python

```python
import quarterwatt as qw
df = qw.load_panel(["P_DA", "Load_Actual"], zones=["DE", "BE"])   # all zones, core columns
be = qw.load_zone("BE")                                            # one zone, all columns
```

## Zones

| Code | Name | Folder | Hourly table |
|---|---|---|---|
""" + "\n".join(rows) + """

## Notes
* Time index: Europe/Berlin (CET/CEST) for all zones except TR (Europe/Istanbul); panel in UTC.
* Prices in EUR/MWh. Local-currency values delivered by ENTSO-E (PLN, RON, BGN, CZK, HUF, HRK)
  are converted with ECB monthly rates; every conversion is logged in the zone's manifest.json.
* Licences: see each source's terms (ENTSO-E, EPEX/EEX price data are not for redistribution).
"""
    with open(os.path.join(root, "README.md"), "w") as f:
        f.write(text)
    return os.path.join(root, "README.md")


# =============================================================================
#  STORAGE CLEAN-UP
# =============================================================================
ROOT_ENTRIES = {"README.md", "code", "data", "audit", "fx_rates", "website"}
ZONE_ENTRIES = {"raw", "output"}


def _size(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs)


_ALL_ON = {"USE_ELIA": True, "USE_REGELLEISTUNG": True, "USE_WEATHER": True, "USE_EUROSTAT": True,
           "USE_FRED": True}


def _all_caches(code):
    """Raw folder names of ALL data sets a zone can have (independent of the
    current USE_* switches), so clean_storage never removes data that is
    only switched off."""
    g = globals()
    saved = {k: g[k] for k in _ALL_ON}
    try:
        g.update(_ALL_ON)
        configure(code)
        return {d["cache"] for d in DATASETS}
    finally:
        g.update(saved)


@_keeps_zone
def clean_storage(delete=False):
    """Lists (delete=False) or removes (delete=True) everything in Quarterwatt/
    that the code no longer uses:
      * entries in the main folder outside the standard layout (see README)
      * zone folders of unknown zones, files/folders in a zone besides raw/ output/
      * raw/<cache> folders no data set of that zone uses (old cache versions)
      * *.bak files left by an interrupted gap re-fetch (restored if the
        original file is missing, otherwise removed)
    Raw data in use, outputs, audit, panel and fx rates are never touched."""
    import shutil
    root = root_dir()
    plan = []                                              # (path, reason)
    for e in sorted(os.listdir(root)):
        if e not in ROOT_ENTRIES:
            plan.append((os.path.join(root, e), "not part of the layout"))
    zones_dir = os.path.join(root, "data", "zones")
    known = {zone_folder_name(c): c for c in ALL_ZONES}
    restore = []
    for z in sorted(os.listdir(zones_dir)) if os.path.isdir(zones_dir) else []:
        zpath = os.path.join(zones_dir, z)
        if z not in known:
            plan.append((zpath, "unknown zone folder"))
            continue
        for e in sorted(os.listdir(zpath)):
            if e not in ZONE_ENTRIES:
                plan.append((os.path.join(zpath, e), "not raw/ or output/"))
        used = _all_caches(known[z])
        raw = os.path.join(zpath, "raw")
        for e in sorted(os.listdir(raw)) if os.path.isdir(raw) else []:
            if e not in used:
                plan.append((os.path.join(raw, e), "raw cache not used by any data set"))
            else:
                for f in glob.glob(os.path.join(raw, e, "*.bak")):
                    if os.path.exists(f[:-4]):
                        plan.append((f, "backup of a re-fetched month"))
                    else:
                        restore.append(f)
    total = 0
    print(f">>> {'DELETING' if delete else 'would delete'} ({len(plan)} items):")
    for path, why in plan:
        sz = _size(path)
        total += sz
        print(f"   {sz / 1e6:9.1f} MB  {os.path.relpath(path, root)}   [{why}]")
    if restore:
        print(f">>> {len(restore)} *.bak files without original -> {'restored' if delete else 'would be restored'}")
    print(f">>> total {total / 1e6:.1f} MB")
    if not delete:
        print(">>> nothing changed. Run clean_storage(delete=True) to remove these items.")
        return plan
    for f in restore:
        os.replace(f, f[:-4])
    for path, _ in plan:
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)
    print(">>> done")
    return plan


# =============================================================================
#  REFERENCE DATA (static, not per zone): load profiles, German levies
# =============================================================================
#  Quarterwatt/data/reference/
#     load_profiles/bdew_slp.parquet      BDEW standard load profiles (demandlib),
#                                         15 min, Europe/Berlin, 2015-2027,
#                                         normalised: mean kW per 1 MWh/year
#     load_profiles/braeuer2020_raw/      50 German SME plants (Zenodo 3899018, CC BY 4.0)
#     load_profiles/braeuer2020.parquet   same, long format (plant, timestamp, kW)
#     tariffs/DE_levies.csv               German levies and electricity tax 2015-2026
BRAEUER_FILES = {"2016": "LoadProfile_20IPs_2016.csv", "2017": "LoadProfile_30IPs_2017.csv"}
BRAEUER_URL = "https://zenodo.org/records/3899018/files/{}?download=1"
SLP_TYPES = ["g0", "g1", "g2", "g3", "g4", "g5", "g6", "l0", "l1", "l2", "h0"]
SLP_YEARS = range(2015, 2028)

# German levies (ct/kWh, net). Category A = first 1 GWh per offtake point.
# Sources: wattline "Steuern, Abgaben und Umlagen 2024" (values 2015-2024, Stand 25.10.2023);
# Erdgas Südwest "Strom Steuern Abgaben 2025 / 2026"; evm "Entwicklung der Umlagen" (11/2018);
# Vattenfall "Stromsteuer für Unternehmen 2026". EEG: 3.723 ct/kWh 01-06/2022, 0 from 01.07.2022.
_W = "wattline 2024 (Stand 25.10.2023)"
_E25, _E26 = "Erdgas Suedwest 2025", "Erdgas Suedwest 2026"
DE_LEVIES = (
    [("EEG", "all", y, v, _W, "") for y, v in zip(range(2015, 2022), [6.170, 6.354, 6.880, 6.792, 6.405, 6.756, 6.500])]
    + [("EEG", "all", 2022, 3.723, _W, "valid 01.01.-30.06.2022"),
       ("EEG", "all", 2022, 0.0, _W, "valid 01.07.-31.12.2022 (EEG-Entlastungsgesetz)"),
       ("EEG", "all", 2023, 0.0, _W, "abolished (federal budget)"), ("EEG", "all", 2024, 0.0, _W, ""),
       ("EEG", "all", 2025, 0.0, _E25, ""), ("EEG", "all", 2026, 0.0, _E26, "")]
    + [("KWKG", "A", y, v, _W, "") for y, v in zip(range(2015, 2025),
       [0.254, 0.445, 0.438, 0.345, 0.280, 0.226, 0.254, 0.378, 0.357, 0.275])]
    + [("KWKG", "A", 2025, 0.277, _E25, "reduction for privileged companies on application (§27 KWKG)"),
       ("KWKG", "A", 2026, 0.446, _E26, "reduction for privileged companies on application (§27 KWKG)")]
    + [("StromNEV19", "A", y, v, _W, "first 1 GWh per offtake point") for y, v in zip(range(2015, 2025),
       [0.237, 0.378, 0.388, 0.370, 0.305, 0.358, 0.432, 0.437, 0.417, 0.643])]
    + [("StromNEV19", "A", 2025, 1.558, _E25, "Aufschlag fuer besondere Netznutzung, first 1 GWh"),
       ("StromNEV19", "A", 2026, 1.559, _E26, "Aufschlag fuer besondere Netznutzung, first 1 GWh"),
       ("StromNEV19", "B", 2019, 0.050, "evm 11/2018", "> 1 GWh"),
       ("StromNEV19", "B", 2025, 0.050, _E25, "> 1 GWh"), ("StromNEV19", "B", 2026, 0.050, _E26, "> 1 GWh"),
       ("StromNEV19", "C", 2025, 0.025, _E25, "> 1 GWh, manufacturing with electricity costs > 4 % of turnover"),
       ("StromNEV19", "C", 2026, 0.025, _E26, "> 1 GWh, manufacturing with electricity costs > 4 % of turnover")]
    + [("Offshore", "A", y, v, _W, "Offshore-Haftungsumlage until 2018" if y < 2019 else "")
       for y, v in zip(range(2015, 2025), [-0.051, 0.040, -0.028, 0.037, 0.416, 0.416, 0.395, 0.419, 0.591, 0.656])]
    + [("Offshore", "A", 2025, 0.816, _E25, "reduction for privileged companies on application"),
       ("Offshore", "A", 2026, 0.941, _E26, "reduction for privileged companies on application")]
    + [("AbLaV", "all", y, v, _W, "year assignment from a scrambled table - verify")
       for y, v in zip(range(2015, 2023), [0.006, 0.000, 0.006, 0.011, 0.005, 0.007, 0.009, 0.003])]
    + [("AbLaV", "all", 2026, 0.000, _E26, "")]
    + [("Stromsteuer", "standard", y, 2.050, _W if y < 2025 else _E25 if y == 2025 else _E26, "")
       for y in range(2015, 2027)]
    + [("Stromsteuer", "manufacturing", y, 1.537, "§9b StromStG (verify)", "reduced rate before 2024; "
        "additional Spitzenausgleich until 2023 not included") for y in range(2015, 2024)]
    + [("Stromsteuer", "manufacturing", y, 0.050, "Vattenfall 2026", "EU minimum, on application; "
        "permanent from 2026") for y in (2024, 2025, 2026)]
)


def reference_dir():
    return os.path.join(root_dir(), "data", "reference")


_TIME_FORMATS = ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                 "%d/%m/%Y %H:%M", "%Y-%m-%dT%H:%M:%S")


def _parse_times(col):
    """Explicit formats first (fast, no warnings); dateutil only as last resort."""
    txt = col.astype(str).str.strip()
    for fmt in _TIME_FORMATS:
        idx = pd.to_datetime(txt, format=fmt, errors="coerce")
        if idx.notna().mean() > 0.95:
            return idx
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return pd.to_datetime(txt, dayfirst=True, errors="coerce")


def _localise_profile(idx):
    """Naive load-profile timestamps -> interval START, tz-aware (Europe/Berlin).
    - interval end (first stamp hh:15, grid starts at 00:15): shift by -15 min
    - no DST in the file (the spring-forward hour 02:00-02:45 exists and no stamp
      is duplicated): constant UTC+1 (Etc/GMT-1); otherwise local time with DST."""
    idx = pd.DatetimeIndex(idx)
    if len(idx) and idx.min().minute == 15 and idx.min().hour == 0:
        idx = idx - pd.Timedelta("15min")
    years = sorted(set(idx.year))
    spring = [pd.Timestamp(f"{y}-03-31") - pd.Timedelta(days=(pd.Timestamp(f"{y}-03-31").weekday() + 1) % 7)
              for y in years]                                    # last Sunday of March
    has_gap_hour = any(((idx >= d + pd.Timedelta("2h")) & (idx < d + pd.Timedelta("3h"))).any() for d in spring)
    if has_gap_hour and not idx.duplicated().any():
        return idx.tz_localize("Etc/GMT-1").tz_convert("Europe/Berlin")
    return idx.tz_localize("Europe/Berlin", ambiguous="infer" if idx.duplicated().any() else "NaT",
                           nonexistent="NaT")


def _parse_load_csv(path):
    """Tolerant reader for the Braeuer CSVs: separator / decimal sign and the
    time column are detected; plants = all other numeric columns (kW).
    Returns long format (timestamp = interval start, Europe/Berlin; plant; kW)."""
    with open(path, encoding="utf-8", errors="replace") as fh:
        head = fh.read(4000)
    sep = ";" if head.count(";") > head.count(",") else ","
    dec = "," if sep == ";" else "."
    df = pd.read_csv(path, sep=sep, decimal=dec, encoding="utf-8", encoding_errors="replace",
                     low_memory=False)
    tcol = next((c for c in df.columns if re.search(r"time|date|zeit|datum|stamp", str(c), re.I)), None)
    if tcol is None and df[df.columns[0]].dtype == object:     # text column that parses as dates
        first = df.columns[0]
        parsed = _parse_times(df[first].head(200))
        if parsed.notna().mean() > 0.9 and parsed.dt.year.min() > 2000:
            tcol = first
    plants = [c for c in df.columns if c != tcol]
    num = df[plants].apply(pd.to_numeric, errors="coerce")
    num = num.loc[:, num.notna().mean() > 0.5]
    if tcol is not None:
        idx = _parse_times(df[tcol])
        keep = idx.notna().to_numpy()
        num, idx = num[keep], idx[keep]
        num.index = _localise_profile(idx)
    else:                                   # no time column: 15-min grid of the file's year (UTC+1)
        year = int(re.search(r"(20\d\d)", os.path.basename(path)).group(1))
        num.index = pd.date_range(f"{year}-01-01", periods=len(df), freq="15min",
                                  tz="Etc/GMT-1").tz_convert("Europe/Berlin")
    num = num[num.index.notna()]
    num = num[~num.index.duplicated()]
    long = num.stack().dropna().rename("kW").reset_index()     # pandas 3 keeps NaN in stack()
    long.columns = ["timestamp", "plant", "kW"]
    return long


def _bdew_slp():
    from demandlib import bdew
    import holidays as _hol
    frames = []
    for y in SLP_YEARS:
        # demandlib calls warnings.simplefilter("error") itself, which turns every
        # pandas deprecation warning into a crash -> keep the filters as they are
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            _sf, warnings.simplefilter = warnings.simplefilter, (lambda *a, **k: None)
            try:
                e = bdew.ElecSlp(y, holidays={d: n for d, n in _hol.Germany(years=y).items()})
                prof = e.get_scaled_power_profiles({t: 1000.0 for t in SLP_TYPES})   # 1 MWh/a = 1000 kWh
            finally:
                warnings.simplefilter = _sf
        prof.index = prof.index.tz_localize("Europe/Berlin", ambiguous="NaT", nonexistent="NaT")
        prof = prof[prof.index.notna()]
        grid = pd.date_range(pd.Timestamp(f"{y}-01-01", tz="Europe/Berlin"),
                             pd.Timestamp(f"{y + 1}-01-01", tz="Europe/Berlin"), freq="15min", inclusive="left")
        frames.append(prof.reindex(grid).ffill(limit=8))       # DST hours: carry the value
    out = pd.concat(frames)
    out.index.name = "timestamp"
    return out


def update_reference(force=False):
    """Downloads / builds the static reference data (once; force=True rebuilds)."""
    ref = reference_dir()
    lp = os.path.join(ref, "load_profiles")
    os.makedirs(os.path.join(lp, "braeuer2020_raw"), exist_ok=True)
    os.makedirs(os.path.join(ref, "tariffs"), exist_ok=True)
    # 1) German levies
    lev = pd.DataFrame(DE_LEVIES, columns=["component", "category", "year", "ct_per_kWh", "source", "note"])
    lev["valid_from"] = pd.to_datetime(lev["year"].astype(str) + "-01-01")
    lev["valid_to"] = pd.to_datetime(lev["year"].astype(str) + "-12-31")
    h1 = lev["note"].str.startswith("valid 01.01.-30.06."); h2 = lev["note"].str.startswith("valid 01.07.-31.12.")
    lev.loc[h1, "valid_to"] = pd.to_datetime(lev.loc[h1, "year"].astype(str) + "-06-30")
    lev.loc[h2, "valid_from"] = pd.to_datetime(lev.loc[h2, "year"].astype(str) + "-07-01")
    # category A (first 1 GWh) is complete 2015-2026; B/C (large / privileged consumers)
    # only where a source was found - extend before using them
    lev.to_csv(os.path.join(ref, "tariffs", "DE_levies.csv"), index=False)
    print(f"   DE levies: {len(lev)} rows -> data/reference/tariffs/DE_levies.csv")
    # 2) BDEW standard load profiles
    path = os.path.join(lp, "bdew_slp.parquet")
    if force or not os.path.exists(path):
        try:
            import importlib.util
            if importlib.util.find_spec("demandlib") is None:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "demandlib"])
            slp = _bdew_slp()
            slp.to_parquet(path)
            print(f"   BDEW SLP: {slp.shape[1]} profiles x {len(slp):,} quarter-hours -> load_profiles/bdew_slp.parquet")
        except Exception as ex:
            print(f"   !! BDEW SLP not built: {type(ex).__name__}: {ex}")
    # 3) Braeuer et al. 50 SME plants
    path = os.path.join(lp, "braeuer2020.parquet")
    stale = False
    if os.path.exists(path) and not force:
        try:                                  # older table with naive interval-end stamps -> rebuild
            stale = pd.read_parquet(path, columns=["timestamp"])["timestamp"].dt.tz is None
        except Exception:
            stale = True
    if force or stale or not os.path.exists(path):
        parts = []
        for year, fname in BRAEUER_FILES.items():
            raw = os.path.join(lp, "braeuer2020_raw", fname)
            try:
                if not os.path.exists(raw):
                    r = requests.get(BRAEUER_URL.format(fname), timeout=180)
                    r.raise_for_status()
                    with open(raw, "wb") as f:
                        f.write(r.content)
                part = _parse_load_csv(raw)
                part["plant"] = f"{year}_" + part["plant"].astype(str)
                parts.append(part)
                print(f"   Braeuer {year}: {part['plant'].nunique()} plants, {len(part):,} values")
            except Exception as ex:
                print(f"   !! Braeuer {year} not loaded: {type(ex).__name__}: {ex}")
        if parts:
            pd.concat(parts, ignore_index=True).to_parquet(path, index=False)
    return ref


def load_reference(name):
    """'levies' | 'slp' | 'braeuer' -> DataFrame."""
    ref = reference_dir()
    files = {"levies": ("tariffs", "DE_levies.csv"), "slp": ("load_profiles", "bdew_slp.parquet"),
             "braeuer": ("load_profiles", "braeuer2020.parquet")}
    path = os.path.join(ref, *files[name])
    if path.endswith(".csv"):
        return pd.read_csv(path, parse_dates=["valid_from", "valid_to"])
    return pd.read_parquet(path)


if __name__ == "__main__":
    run(COUNTRIES)
