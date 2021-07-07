from unittest.mock import patch
from fastapi.testclient import TestClient


def get_app():
    with patch('osiris.core.configuration.Configuration') as _:
        from app.main import app

        return TestClient(app)


client = get_app()


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "OK"}
