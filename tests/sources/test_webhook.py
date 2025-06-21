import pytest
from fastapi.testclient import TestClient
from sqlflow.sources.webhook import WebhookSource, HMACConfig

@pytest.fixture
def client_no_hmac():
    source = WebhookSource()
    return TestClient(source._app)

@pytest.fixture
def client_with_hmac():
    hmac_config = HMACConfig(header="X-HMAC-Signature", sig_key="test_key", secret="test_secret")
    source = WebhookSource(hmac_config=hmac_config)
    return TestClient(source._app), hmac_config

def test_receive_events_no_hmac(client_no_hmac):
    response = client_no_hmac.post("/events", data=b'{"key": "value"}')
    assert response.status_code == 200
    assert response.json() == {"status": "received"}

def test_receive_events_hmac_no_header(client_with_hmac):
    client, _ = client_with_hmac
    response = client.post("/events", data=b'{"key": "value"}')
    assert response.status_code == 400
    assert response.json() == {"detail": "Missing HMAC signature"}

def test_receive_events_hmac_invalid_signature(client_with_hmac):
    client, hmac_config = client_with_hmac
    headers = {hmac_config.header: "invalid_signature"}
    response = client.post("/events", data=b'{"key": "value"}', headers=headers)
    assert response.status_code == 403
    assert response.json() == {"detail": "Invalid HMAC signature"}

def test_receive_events_hmac_valid_signature(client_with_hmac):
    import hmac
    import hashlib

    client, hmac_config = client_with_hmac
    body = b'{"key": "value"}'
    mac = hmac.new(hmac_config.secret.encode(), body, hashlib.sha256)
    valid_signature = f"sha256={mac.hexdigest()}"
    headers = {hmac_config.header: valid_signature}

    response = client.post("/events", data=body, headers=headers)
    assert response.status_code == 200
    assert response.json() == {"status": "received"}