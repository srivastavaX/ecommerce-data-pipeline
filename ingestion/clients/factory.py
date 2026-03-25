from ..config import USE_MOCK_API
from . import api_client
from . import mock_client


def get_client():
    if USE_MOCK_API:
        print("USING MOCK API CLIENT")
        return mock_client
    else:
        print("USING REAL API CLIENT")
        return api_client