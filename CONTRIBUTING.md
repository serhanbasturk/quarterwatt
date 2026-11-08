# Contributing to Quarterwatt

Thank you for helping. Questions, error reports and improvements are all welcome.

## Reporting a problem

Open an [issue](https://github.com/serhanbasturk/quarterwatt/issues/new/choose) and choose:

* **Bug report:** the code crashes or behaves unexpectedly;
* **Data issue:** a value, gap or currency looks wrong in a zone.

Please include the code version (`qw.CODE_VERSION`), the data version (`data/panel/VERSION.json`), the zone and the period. Paste the relevant output, but **remove your API keys**. Quarterwatt masks them, but check anyway.

## Changing the code

1. Fork the repository and create a branch.
2. Make your change. Keep the rules in [Handbook § 14](docs/HANDBOOK.md#14-for-developers-how-the-code-works-and-how-to-extend-it):
   * never change raw files or the meaning of an existing column;
   * every new rule comes with a test.
3. Run the offline tests (no API key needed): `python tests/run_tests.py`. All tests must pass.
4. Add a line to `CHANGELOG.md` and open a pull request that describes what changed and why.

## Data and licences

Never commit downloaded data, API keys or credentials. Raw market data are subject to the licences of their sources (see [Handbook § 11](docs/HANDBOOK.md#11-licences-and-citation)).
