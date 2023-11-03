"""Base client tests."""
from aiohttp import ClientSession
import pytest

from pytraccar import ApiClient


@pytest.mark.asyncio
async def test_client_init(client_session: ClientSession):
    """Test client init."""
    client_params = {
        "host": "127.0.0.1",
        "username": "test",
        "password": "test",
        "port": 8080,
        "client_session": client_session,
    }

    assert ApiClient(**client_params)._base_url == "http://127.0.0.1:8080/api"
    assert ApiClient(**{**client_params, "port": None})._base_url == "http://127.0.0.1:8082/api"
    assert ApiClient(**{**client_params, "ssl": True})._base_url == "https://127.0.0.1:8080/api"