"""Check that basic features work before publishing to pypi.

Catch cases where e.g. files are missing so the import doesn't work. It is
recommended to check that e.g. assets are included."""

# tests/smoke_test.py
import sys

import wadoh_raccoon  # replace with your actual package name
from wadoh_raccoon.utils import helpers

def test_basic_import():
    """Check that the package can be imported."""
    assert wadoh_raccoon is not None

def test_basic_functionality():
    """Optionally check one key function."""
    result = helpers.clean_name(col="test")
    assert result is not None

if __name__ == "__main__":
    test_basic_import()
    test_basic_functionality()
    print("Smoke test passed!")
