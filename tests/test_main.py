import pytest
from neurodb.main import my_function

def test_my_function():
    assert my_function() == "Hello from my_package!"