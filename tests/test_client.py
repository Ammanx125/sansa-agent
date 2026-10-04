from sansa_agent.client import SansaClient


def test_headers_include_bearer_credential():
    client = SansaClient(
        base_url="http://127.0.0.1:8000",
        credential="agent-secret",
    )

    assert client._headers() == {
        "Authorization": "Bearer agent-secret",
    }


def test_headers_are_empty_without_credential():
    client = SansaClient(base_url="http://127.0.0.1:8000")

    assert client._headers() == {}
