import codecs
import os
import re

from setuptools import setup, find_packages

# Acknowledgement: This setup.py was adapted from Hynek Schlawack's Python
#                  Packaging Guide
# https://hynek.me/articles/sharing-your-labor-of-love-pypi-quick-and-dirty

###################################################################

NAME = "brad"
PACKAGES = find_packages(where="src")
META_PATH = os.path.join("src", "brad", "__init__.py")
README_PATH = "README.md"
PYTHON_REQUIRES = ">=3.11"

PACKAGE_DATA = {"": ["brad/planner/scoring/data/*.json", "brad/daemon/*.json"]}
PACKAGE_DIR = {"": "src"}

ENTRY_POINTS = {
    "console_scripts": [
        "brad = brad.__main__:main",
    ],
}

INSTALL_REQUIRES = [
    "boto3",
    "grpcio",
    "pyodbc",
    "pyyaml",
    "sqlglot",
    "pandas",
    "scikit-learn",
    "types-pytz",
    "numpy",
    "imbalanced-learn",
    "redshift_connector",
    "tabulate",
    "PyAthena",
]

DEV_REQUIRES = [
    "absl-py",
    "black",
    "grpcio-tools",
    "mypy",
    "pylint",
    "pytest",
    "types-PyYAML",
    "types-protobuf",
    "absl-py",
    "immutables",
    "attrs",
    "typing-extensions",
    "types-tabulate",
]

KEYWORDS = []

CLASSIFIERS = [
    "Private :: Do Not Upload",
]

###################################################################

HERE = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    """
    Build an absolute path from *parts* and return the contents of the
    resulting file. Assume UTF-8 encoding.
    """
    with codecs.open(os.path.join(HERE, *parts), "rb", "utf-8") as f:
        return f.read()


META_FILE = read(META_PATH)


def find_meta(meta):
    """
    Extract __*meta*__ from META_FILE.
    """
    meta_match = re.search(
        r"^__{meta}__ = ['\"]([^'\"]*)['\"]".format(meta=meta), META_FILE, re.M
    )
    if meta_match:
        return meta_match.group(1)
    raise RuntimeError("Unable to find __{meta}__ string.".format(meta=meta))


if __name__ == "__main__":
    setup(
        name=NAME,
        description=find_meta("description"),
        version=find_meta("version"),
        author=find_meta("author"),
        author_email=find_meta("email"),
        maintainer=find_meta("author"),
        maintainer_email=find_meta("email"),
        url=find_meta("url"),
        long_description=read(README_PATH),
        long_description_content_type="text/markdown",
        packages=PACKAGES,
        package_data=PACKAGE_DATA,
        package_dir=PACKAGE_DIR,
        python_requires=PYTHON_REQUIRES,
        install_requires=INSTALL_REQUIRES,
        extras_require={
            "dev": DEV_REQUIRES,
        },
        entry_points=ENTRY_POINTS,
        classifiers=CLASSIFIERS,
        keywords=KEYWORDS,
    )
