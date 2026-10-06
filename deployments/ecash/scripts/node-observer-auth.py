#!/usr/bin/env python3
"""Create a dedicated read-only RPC identity; never print the credential.

The general node cookie is neither read nor changed. The caller installs these
files in the private data root and assigns the existing extractor group.
"""
import hashlib
import hmac
import os
from pathlib import Path
import secrets
import sys

root = Path(sys.argv[1])
auth = root / 'node-observer-auth'
config = root / 'node-observer-rpcauth'
if not auth.exists():
    fd = os.open(auth, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(fd, 'w') as stream:
        stream.write('bip300_observer:' + secrets.token_hex(32) + '\n')
username, password = auth.read_text().strip().split(':', 1)
if username != 'bip300_observer' or len(password) != 64:
    raise SystemExit('invalid observer RPC credential file')
if config.exists():
    line = config.read_text().strip()
    prefix = 'rpcauth=bip300_observer:'
    if not line.startswith(prefix):
        raise SystemExit('invalid observer rpcauth config')
    salt, expected = line[len(prefix):].split('$', 1)
    actual = hmac.new(salt.encode(), password.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, actual):
        raise SystemExit('observer RPC credential and rpcauth config do not match')
else:
    salt = secrets.token_hex(16)
    digest = hmac.new(salt.encode(), password.encode(), hashlib.sha256).hexdigest()
    fd = os.open(config, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(fd, 'w') as stream:
        stream.write(f'rpcauth={username}:{salt}${digest}\n')
