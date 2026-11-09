"""Quarterwatt test suite: offline, no API keys needed (all sources are mocked).

    python tests/run_tests.py            # all tests
    python tests/run_tests.py v14 bal    # only tests whose file name contains one of the words

Each test runs in its own empty temporary folder with a fresh copy of quarterwatt.py
and checks the printed results against the expected lines below."""
import subprocess, shutil, os, sys, tempfile, time
HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(os.path.dirname(HERE), "quarterwatt.py")
TESTS = [
 ("download pipeline + offline rebuild", "test_pipeline.py", "", [
   "TR P_SMF (3300*90/3000=99): [99.]", "DE P_ID_AEP: [1234.56] NaN 0", "(10/1.1*3.412=31.02): 31.02",
   "TR LCA CO2: 441.3 | direct: 469.1", "calls unchanged: True | rows equal: True"]),
 ("Poland end-to-end (PLN, A03 gaps, imbalance)", "test_poland.py", "", [
   "PL P_DA monthly median : [44.0, 44.0, 44.0, 44.0, 44.0, 44.0, 44.0]",
   "PL imbalance (Long)     : [45.0, 45.0, 45.0, 45.0, 45.0, 45.0, 45.0]",
   "DE P_DA_PL monthly      : [44.0, 44.0, 44.0, 44.0, 44.0, 44.0, 44.0]",
   "unconverted             : None", "rebuild identical       : True True"]),
 ("currency rules BG/NO/HU/HR + NTP parser", "test_cur2.py", "", [
   "BG yearly median: [33.7", "NO_1: {}", "HU imb max ratio month: 1.12",
   "HR imb/DA by year: [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]", "45.67", "1234.56"]),
 ("Bulgaria single switch, realistic level gap", "test_bg.py", "", [
   "BG months still ~1.96x too high: 0", "2016-10-31 2022-01-20 1908"]),
 ("A03 gap fill", "test_a03.py", "", [
   "Jan 2 00-05 (CET day starts 00:00, previous = none) : [23.0, 23.0, 23.0, 23.0, 23.0, 29.0]",
   "Jan 3 full NaN stays: True | end stays NaN: True"]),
 ("FX fallback frankfurter / saved copy / none", "test_fx.py", "", [
   "frankfurter: 4.4 117 True", "saved copy: 4.4", "none: None", "values kept: 200.0"]),
 ("FX fallback FRED", "test_fred.py", "", ["4.28 4.29", "RON: None"]),
 ("CZ rule, price bounds, generation columns, IT CO2, NTP 400", "test_fix2.py", "", [
   "CZ imb/DA level: 1.0", "4500 in 2023 kept: 1", "'Gen_Fossil_Gas': [1.0, 2.0]",
   "['Gen_Biomass', 'Gen_Hydro_Pumped_Storage_Consumption', 'Gen_Solar']", "= 269.0", "DE unchanged: 24.0", "ntp: 200"]),
 ("audit detects wrong currency / missing zone", "test_panel.py", "", [
   "P_DA >1.7x continental median in 3 months", "no hourly table found (not downloaded yet?) -> not in the panel"]),
 ("35-zone stress: audit + panel + finalize", "test_stress.py", "", [
   "hourly panel : 3,632,159 rows", "{'DE': 103776, 'TR': 103775}", "   none", "zones: {'PASS': 35}"]),
 ("edge cases: BG switch, FX gaps, yearly aggregation, state", "test_review.py", "", [
   "R4 hours still in BGN: 0 | hours wrongly divided: 0", "R5 skipped logged: ['P_DA (reference SK missing)']",
   "R6 imb 2017 NaN: True | 2019 value: 50.0 | logged: [36]", "R7 yearly max: 999.0 | loadweighted: 31.489 expected 31.489 | col: True",
   "R9 zone after status: BE Europe/Berlin", "R9 offline undownloaded: (None, None) | folder created: False",
   "R10 wrong hours with spike: 0 | to: 2021-05-10", "R11 BGN check: True", "R12 months without rate: [36]", "R13 failures kept: 1"]),
 ("clean_storage: only unused items, switches off, .bak restore", "test_clean.py", "", [
   "PLAN: ['archive', 'data/zones/DE_Germany/diagnose_output.txt', 'data/zones/DE_Germany/raw/Net_Position_old', 'data/zones/DE_Germany/raw/P_DA/2016-01.parquet.bak', 'data/zones/XX_Nowhere']",
   "UNCHANGED BEFORE DELETE: True", "AFTER: weather kept True | P_DA kept True | restored True | archive gone True | audit kept True | bak removed True",
   "SWITCH RESTORED: False"]),
 ("secrets, balancing energy, IDA2/3, generation-forecast fallback, licences, reference data", "test_sources.py", "", [
   "S1 key from env: True | no credentials in code: True", "S1 ntp error: True", "S1 NTP datasets still registered: True",
   "S1 EPIAS datasets registered: True", "S2 cols: ['P_BalE_aFRR_Down', 'P_BalE_aFRR_Up'] | rows: 8 | first up: 110.0 | 4th up: 110.0",
   "S2 IDA areas before/after start: [] ['DE_LU']",
   "S3 fallback hours: 96", "S4 licences: False False True True False False unknown",
   "S5 levies: 87 | EEG 2022: 3.723 | Offshore 2026: 0.941", "S5 SLP: 11 profiles | 2024 kWh per MWh/a: 1000.0 | tz: Europe/Berlin | quarters 2024: 35136 | NaN: 0",
   "S5 braeuer: ['2016_IP1', '2016_IP2', '2017_P1', '2017_P2']", "first time: 2016-01-01 00:00:00",
   "F1 IDA June 2024: ['DE_LU'] | BalE 2018/2019: [] ['DE_LU']",
   "F8 EEG 2022 periods: [('2022-01-01', '2022-06-30', 3.723), ('2022-07-01', '2022-12-31', 0.0)]",
   "F7 licences: False ENTSO-E True", "F2 BalE currency WARNs: []", "F2 BalE in CZK detected: True"]),
 ("balancing energy end-to-end incl. HUF detection", "test_balancing.py", "", [
   "E DE cols: ['P_BalE_aFRR_Down', 'P_BalE_aFRR_Up', 'P_BalE_mFRR_Down', 'P_BalE_mFRR_Up'] | up price: 80.0",
   "E HU cols: ['P_BalE_aFRR_Down', 'P_BalE_aFRR_Up', 'P_BalE_mFRR_Down', 'P_BalE_mFRR_Up'] | up price: 80.0",
   "E HU conversions: ['P_BalE_aFRR_*', 'P_BalE_mFRR_*']"]),
 ("Germany without netztransparenz secrets keeps stored data", "test_ntp_missing.py", "", [
   "netztransparenz credentials missing", "N kept: True True | failures: 0"]),
 ("secrets clean-up, no volume dataset, Braeuer time axis, stale reference rebuild", "test_reference.py", "", [
   "K1 secrets: 'abc-123' 'xyz' ''", "K2 volume registered: False | price registered: True",
   "K3 2016 first: 2016-01-01 00:00:00+01:00 | last: 2016-12-31 23:45:00+01:00 | n per plant: [35136, 35136]",
   "K3 utc steps: [Timedelta('0 days 00:15:00')] | midnight value: 0.0 | IP2: 3.5", "summer 00:00 local = row of 23:00 UTC+1: 92.0",
   "K4 2017 n: {'P1': 35039, 'P2': 35040} | unique: True | tz: Europe/Berlin | utc steps: [Timedelta('0 days 00:15:00')]",
   "K5 rebuilt: True 4",
   "K7 version: 1.1 | no credentials: True"]),
 ("v1.1: regelleistung harmonisation, quarter-hour panel indicators, dictionary units and licences", "test_v11.py", "", [
   "H1 old per hour: [np.float64(10.0)] | new: [np.float64(12.0)] | raw kept: True",
   "H2 before: 15.0 | DST 00-04 (3 h): 20.0 | after German end: 20.0",
   "'Elia_netregulationvolume': 'MW', 'Elia_systemimbalance': 'MW', 'Elia_ace': 'MW', 'Elia_imbalanceprice': 'EUR/MWh'",
   "'P_SMF_TRY': 'TRY/MWh', 'P_SMF': 'EUR/MWh', 'P_DA_TRY': 'TRY/MWh', 'P_DA_USD': 'USD/MWh', 'P_DA_FR': 'EUR/MWh'",
   "H3 unknown licences: [] | empty units: []",
   "H4 Oct neg hours hourly / qh: 0 7.75 | spread hourly / qh: 55.0 220.0", "H4 without p15 no qh cols: True",
   "H5 raw tables: [['DE', 'raw_combined/RL_aFRR.parquet', 'regelleistung.net - derived only, TSO consent required']]",
   "H5 timestamp listed: False | qh cols: True | DE Oct qh neg: 7.75", "H6 version: 1.1"]),
 ("resume after a disconnect (zones and recent months)", "test_resume.py", "", [
   "T1 first run calls > 50: True | state: ['BE', 'NL'] True", "T2 second run calls: 0",
   "T3 resume NL calls (smoke test only): 1 | NL back in state: True", "T4 .empty for recent months: 0 | .checked: True",
   "T5 force refetches (smoke only, files fresh): 1", "T6 stale -> full refetch: True", "T7 data kept: True",
   "T8 rebuild ignores state, no calls: 0"]),
 ("balancing price currency from the XML, refetch of files without currency, HTTP 400 kept, single-price systems, CH excluded", "test_balancing_currency.py", "", [
   "X1 cols: ['P_BalE_aFRR_Down', 'P_BalE_aFRR_Up', 'Cur_BalE_aFRR'] | up: 700.0 | cur: 946.0 | rows: 8",
   "X2 CZ up: 80.0 | down: 20.0 | Cur cols: [] | how: ['XML currency_Unit']",
   "X3 rejected files: 2 | reason kept: True", "| manifest: 2", "X3 second run Net_Position calls: 0",
   "X4 legacy refetched: True | has Cur now: True", "X4 third run calls: 0",
   "X5 RO legacy up (400 RON/5): 80.0",
   "X6 empty months: ['2024-01', '2024-02'] | March kept: 120.0",
   "X7 CH BalE: False | DE BalE: ['BalE_Price_aFRR', 'BalE_Price_mFRR'] | probe: False | volume fn: False"]),
]
TESTS.append(("robustness: open month rebuild, key masking, resume with failures, zone order, API ban, unknown currency, parser, weather", "test_robustness.py", "", [
    "R1 Nov hours after download / next day / next month: 144 144 144", "R10 newest file used: 77.0",
    "R2 key in error: False | masked: True", "R2 files fixed: 1 | key left: False | second pass: 0",
    "R3 NL marked done despite failure: False", "R7 order: ['SK', 'GR', 'BG', 'CZ']",
    "R9 stopped early: True | AT marked done: False", "R4 PL legacy up: True | logged: True",
    "R5 down points: 3 | sym (A03 curve, 1 point -> 4): 4 | up over two periods: 8 | cur: 985.0",
    "R6 missing weather hours: {'TR': 0, 'FR': 0}", "R11 Jan emptied: True | Feb kept: True"]))
TESTS.append(("handbook recipes run on synthetic data", "test_handbook.py", "",
              ["handbook recipes run: 9/9", "lwaec plausible: True | profile factor: True | capture years: [2023, 2024, 2025] | joined rows: 200 | wide cols: ['DE', 'FR', 'PL']"]))
TESTS.append(("API contract: every ENTSO-E call matches the installed entsoe-py", "test_contract.py", "",
              ["signature errors: [] 0", "other errors: 0"]))

only = sys.argv[1:]
res = []
for name, script, _mod, expect in TESTS:
    if only and not any(o in script for o in only):
        continue
    d = tempfile.mkdtemp(prefix="qw_")
    for f in os.listdir(HERE):
        if f.endswith(".py") and f != "run_tests.py":
            shutil.copy(os.path.join(HERE, f), d)
    shutil.copy(os.path.join(os.path.dirname(HERE), "docs", "HANDBOOK.md"), d)
    shutil.copy(CODE, os.path.join(d, "quarterwatt.py"))
    t0 = time.time()
    p = subprocess.run([sys.executable, "-W", "ignore::ResourceWarning", script], cwd=d,
                       capture_output=True, text=True, timeout=2400)
    out = p.stdout + p.stderr
    missing = [e for e in expect if e not in out]
    ok = not missing and p.returncode == 0
    res.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}  ({time.time() - t0:.0f}s)", flush=True)
    if not ok:
        print("   missing:", missing, "| returncode", p.returncode)
        print("   " + "\n   ".join(out.splitlines()[-15:]))
    shutil.rmtree(d, ignore_errors=True)
print(f"\n{sum(res)}/{len(res)} tests passed")
sys.exit(0 if all(res) else 1)
