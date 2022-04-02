import pytest


@pytest.fixture(scope="function", autouse=True)
def mlenv():
    from pyalink.alink import useLocalEnv, resetEnv
    useLocalEnv(2, config={"debug_mode": True})
    yield
    resetEnv()
