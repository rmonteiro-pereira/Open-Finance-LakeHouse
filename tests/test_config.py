"""The "no secrets in code" claim, enforced rather than asserted.

``Settings`` ships MinIO's *published* factory credentials as defaults so a scratch
localhost stack needs no setup. These tests pin the boundary: those defaults may
never be sent to a non-loopback endpoint, because that means either the real
credentials were never wired up or the remote store is still on its factory root
user — and both are findings, not configurations.
"""

from __future__ import annotations

import inspect

import pytest

from ofl import config
from ofl.config import Settings


def test_defaults_are_allowed_against_loopback() -> None:
    s = Settings(S3_ENDPOINT="http://localhost:9000")
    assert s.s3_access_key_id and s.s3_secret_access_key


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://127.0.0.1:9000",
        "http://[::1]:9000",
        "http://0.0.0.0:9000",
    ],
)
def test_other_loopback_forms_are_allowed(endpoint: str) -> None:
    assert Settings(S3_ENDPOINT=endpoint).s3_endpoint == endpoint


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://minio.minio.svc.cluster.local:9000",
        "https://minio-api.example.invalid",
    ],
)
def test_factory_defaults_are_refused_off_loopback(endpoint: str) -> None:
    with pytest.raises(ValueError, match="factory defaults"):
        Settings(S3_ENDPOINT=endpoint)


def test_real_credentials_are_accepted_off_loopback() -> None:
    s = Settings(
        S3_ENDPOINT="https://minio-api.example.invalid",
        S3_ACCESS_KEY_ID="a-real-user",
        S3_SECRET_ACCESS_KEY="a-real-password",
    )
    assert s.s3_access_key_id == "a-real-user"


def test_half_configured_credentials_are_still_refused() -> None:
    """A real user with the default password is not "configured"."""
    with pytest.raises(ValueError, match="factory defaults"):
        Settings(
            S3_ENDPOINT="https://minio-api.example.invalid",
            S3_ACCESS_KEY_ID="a-real-user",
        )


def test_no_credential_literal_survives_in_the_source() -> None:
    """The module must not grow a second hardcoded credential by copy-paste."""
    source = inspect.getsource(config)
    # exactly one definition, referenced by name everywhere else
    assert source.count('"minioadmin"') == 1
