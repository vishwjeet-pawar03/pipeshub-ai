"""SECRET_KEY: the key every service derives its encryption key from.

It encrypts the credentials PipesHub stores (AI model keys, connector
credentials, signing secrets, sync state). The Node API applies the same
rules in backend/nodejs/apps/src/modules/configuration_manager/config/config.ts;
keep the two placeholder lists identical.
"""

import functools
import hashlib
import logging
import os

# Example values published in this repository's templates and docs. A
# deployment that runs on one of them encrypts its stored credentials with a
# key anyone can read.
PLACEHOLDER_SECRET_KEYS = frozenset({
    "your_random_encryption_secret_key",
    "my-secret",
    "your-32-char-secret-key-here",
})

ALLOW_PLACEHOLDER_ENV = "PIPESHUB_ALLOW_INSECURE_SECRET_KEY"

_FIX = "Generate one with `openssl rand -hex 32`, set SECRET_KEY to it in your .env (or secret store) and restart."


@functools.cache
def _warn_placeholder_once() -> None:
    logging.getLogger(__name__).warning(
        "SECRET_KEY is a published example value; running anyway because %s is true. "
        "Stored credentials can be decrypted by anyone who can read the KV store or its backups.",
        ALLOW_PLACEHOLDER_ENV,
    )


def get_hashed_secret_key() -> str:
    """Return the hex SHA-256 of SECRET_KEY, refusing a missing or placeholder value."""
    secret_key = os.getenv("SECRET_KEY")
    if not secret_key or not secret_key.strip():
        raise ValueError(f"SECRET_KEY environment variable is required. {_FIX}")
    if secret_key.strip() in PLACEHOLDER_SECRET_KEYS:
        if os.getenv(ALLOW_PLACEHOLDER_ENV, "").strip().lower() != "true":
            raise ValueError(
                "SECRET_KEY is set to an example value published in the PipesHub templates, "
                "so anyone can decrypt the credentials this deployment stores. "
                f"New deployment: {_FIX} "
                "Deployment that already stored data with this value: changing SECRET_KEY makes "
                f"that data unreadable, so set {ALLOW_PLACEHOLDER_ENV} to true to keep running on it "
                "and see 'SECRET_KEY' in deployment/docker-compose/ADVANCED_DEPLOYMENT.md."
            )
        _warn_placeholder_once()
    return hashlib.sha256(secret_key.encode()).hexdigest()
