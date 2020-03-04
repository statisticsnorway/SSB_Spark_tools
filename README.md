# SSB Spark Tools
> A collection of data processing Spark functions for the use in Statistics Norway (SSB)

[![PyPI version](https://img.shields.io/pypi/v/ssb_spark_tools.svg)](https://pypi.python.org/pypi/ssb_spark_tools/)
[![Status](https://img.shields.io/pypi/status/ssb_spark_tools.svg)](https://pypi.python.org/pypi/ssb_spark_tools/)
[![License](https://img.shields.io/pypi/l/ssb_spark_tools.svg)](https://pypi.python.org/pypi/ssb_spark_tools/)

The SSB Spark Tools Library is a colection of Data processing functions for the use in Data processing in Statistics Norway



## Installation

```python
pip install ssb_spark_tools
```



## Development setup

Run `make help` to see common development commands.

```
install-build-tools            Install required tools for build/dev
build                          Build dist
test                           Run tests
clean                          Clean all build artifacts
release-validate               Validate that a distribution will render properly on PyPI
release-test                   Release a new version, uploading it to PyPI Test
release                        Release a new version, uploading it to PyPI
bump-version-patch             Bump patch version, e.g. 0.0.1 -> 0.0.2
bump-version-minor             Bump minor version, e.g. 0.0.1 -> 0.1.0
```

Refer to the `Makefile` to see details about the different tasks.


### Testing

Run tests for all python distributions using
```sh
make test
```

This will require that your dev machine has the required python distributions installed locally.
(You can install python distributions using [pyenv](https://realpython.com/intro-to-pyenv/).)


## Releasing

*Prerequisites:*
You will need to register accounts on [PyPI](https://pypi.org/account/register/) and [TestPyPI](https://test.pypi.org/account/register/).

Before releasing, make sure you're working on a "new" version number. You can bump the version using the [bumpversion tool](https://medium.com/@williamhayes/versioning-using-bumpversion-4d13c914e9b8).

Also, make sure to update release notes.

To release and publish a new version to PyPI:
```sh
make release-validate
```

This will run tests, build distribution packages and perform some rudimentary PyPI compliancy checking.

For a dress rehearsal, you can do a test release to the [TestPyPI index](https://test.pypi.org/). TestPyPI is very useful, as you can try all the steps of publishing a package without any consequences if you mess up. Read more about TestPyPI [here](https://packaging.python.org/guides/using-testpypi/).

```sh
make release-test
```

To perform the actual release, run:
```sh
make release
```

You should see the new release appearing [here](https://pypi.org/project/ssb-pseudonymization) (it might take a couple of minutes for the index to update).


## Release History

* 0.0.1
    * Initial version with functions as in use on initiaition


## Meta

Statistics Norway – https://github.com/statisticsnorway

Distributed under the MIT license. See ``LICENSE`` for more information.

[https://github.com/statisticsnorway/ssb-pseudonymization-py]


## Contributing

1. Fork it (<https://github.com/statisticsnorway/ssb-pseudonymization-py/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request