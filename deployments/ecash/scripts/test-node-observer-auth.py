#!/usr/bin/env python3
"""Prove RPC least privilege against an isolated local regtest node."""
import base64
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

binary = sys.argv[1]
with tempfile.TemporaryDirectory(prefix='observer-auth-test-') as directory:
    root = Path(directory)
    helper = Path(__file__).with_name('node-observer-auth.py')
    subprocess.run([sys.executable, str(helper), directory], check=True)
    original = (root / 'node-observer-auth').read_bytes()
    subprocess.run([sys.executable, str(helper), directory], check=True)
    assert (root / 'node-observer-auth').read_bytes() == original
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    config = root / 'bitcoin.conf'
    config.write_text('regtest=1\nserver=1\nlisten=0\ndnsseed=0\ndisablewallet=1\n'
                      'rpcwhitelistdefault=0\nrpcwhitelist=bip300_observer:getblockchaininfo,getblockhash,getblockheader,getblock\n'
                      + (root / 'node-observer-rpcauth').read_text()
                      + f'[regtest]\nconnect=0\nrpcbind=127.0.0.1\nrpcport={port}\n')
    log = (root / 'node.log').open('w')
    process = subprocess.Popen([binary, f'-datadir={directory}', f'-conf={config}'], stdout=log, stderr=log)
    def rpc(auth, method, params=()):
        request = urllib.request.Request(f'http://127.0.0.1:{port}', data=json.dumps({'id':1,'method':method,'params':list(params)}).encode(), headers={'Authorization':'Basic '+base64.b64encode(auth.strip()).decode(),'Content-Type':'application/json'})
        with urllib.request.urlopen(request, timeout=3) as response:
            return json.load(response)
    try:
        for _ in range(100):
            try:
                assert rpc(original, 'getblockchaininfo')['result']['chain'] == 'regtest'
                break
            except (OSError, AssertionError):
                if process.poll() is not None:
                    diagnostic=(root / 'node.log').read_text().replace(original.decode().strip().split(':',1)[1], '[redacted]')
                    raise AssertionError('isolated node exited during startup: '+diagnostic[-1500:])
                time.sleep(0.1)
        else:
            raise AssertionError('isolated node readiness timeout')
        for method in ['stop', 'getbestblockhash']:
            try:
                rpc(original, method)
            except urllib.error.HTTPError as error:
                assert error.code == 403, method
            else:
                raise AssertionError('observer RPC escaped its whitelist')
        cookie = (root / 'regtest/.cookie')
        assert cookie.stat().st_mode & 0o077 == 0, 'general cookie permissions were broadened'
        assert rpc(cookie.read_bytes(), 'getblockcount')['result'] == 0
        rpc(cookie.read_bytes(), 'stop')
        process.wait(timeout=15)
        print('PASS dedicated observer RPC reads; admin and unlisted methods denied; owner cookie unchanged')
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=15)
        log.close()
