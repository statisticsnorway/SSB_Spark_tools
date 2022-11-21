# SSB Spark Tools

> A collection of data processing Spark functions for the use in Statistics Norway (SSB)

[![PyPI version](https://img.shields.io/pypi/v/ssb_spark_tools.svg)](https://pypi.python.org/pypi/ssb_spark_tools/)
[![Status](https://img.shields.io/pypi/status/ssb_spark_tools.svg)](https://pypi.python.org/pypi/ssb_spark_tools/)
[![License](https://img.shields.io/pypi/l/ssb_spark_tools.svg)](https://pypi.python.org/pypi/ssb_spark_tools/)

The SSB Spark Tools Library is a colection of Data processing functions for the use in Data processing in Statistics Norway

## Installation

```python
pip install ssb-spark-tools
```

## Development setup

This repo uses `poetry` for dependency management and publishing to PyPi.
Install poetry as described on the [poetry install page](https://python-poetry.org/docs/#installation).

```
poetry install                 Install required tools for build/dev
poetry run pytest              Run tests
poetry build                   Build dist
poetry publish                 Publish to PyPi
```

### Testing

Run tests for all python distributions using GitHub Actions,
see https://github.com/statisticsnorway/SSB_Spark_tools/actions

## Releasing

_Prerequisites:_
You will need to register accounts on [PyPI](https://pypi.org/account/register/) and [TestPyPI](https://test.pypi.org/account/register/).

Before releasing:

- Make sure you're working on a "new" version number.
- Make sure to update release notes.
- Make sure the GitHub repo has a secret with the name `PYPI_API_TOKEN`
  and contains the PyPi access token.

To release and publish a new version to PyPI:

- Create a new release in the GitHub repo.
- The `Upload Python Package` GitHub Action will start and publish the new version to PyPi.

Manually:

```sh
poetry publish
```

For a dress rehearsal, you can do a test release to the [TestPyPI index](https://test.pypi.org/). TestPyPI is very useful, as you can try all the steps of publishing a package without any consequences if you mess up. Read more about TestPyPI [here](https://packaging.python.org/guides/using-testpypi/).

You should see the new release appearing [here](https://pypi.org/project/ssb-spark-tools/) (it might take a couple of minutes for the index to update).

## Release History

- 0.0.1
  - Initial version with functions as in use on initiaition

## Meta

Statistics Norway – https://github.com/statisticsnorway

Distributed under the MIT license. See `LICENSE` for more information.

<https://github.com/statisticsnorway/SSB_Spark_tools>

## Contributing

1. Fork it (<https://github.com/statisticsnorway/SSB_Spark_tools/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request
