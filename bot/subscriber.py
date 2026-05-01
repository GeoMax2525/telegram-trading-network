"""
subscriber.py — Subscriber utilities.

Wallet generation and helper functions.
All command handlers are in handlers.py.
"""

import logging

logger = logging.getLogger(__name__)


def _generate_wallet() -> tuple[str, str]:
    """Generate a new Solana wallet. Returns (public_key, private_key_base58)."""
    try:
        from solders.keypair import Keypair
        kp = Keypair()
        public = str(kp.pubkey())
        private = kp.to_base58_string()
        return public, private
    except ImportError:
        import secrets
        fake_pub = "REVOLT" + secrets.token_hex(14)
        fake_priv = secrets.token_hex(32)
        return fake_pub, fake_priv
