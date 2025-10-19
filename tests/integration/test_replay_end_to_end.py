import os

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS") is None,
    reason="Integration services not provisioned.",
)
def test_integration_placeholder():
    assert True
