#!/usr/bin/env python3
"""Fail visibly in systemd/journal when verified off-host coverage is stale."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
verified = []
for receipt in root.glob('daily-*/OFFHOST_RESTORE_OK'):
    try:
        value = json.loads(receipt.read_text())
        assert value['validation']['schema'] >= 7
        when = datetime.fromisoformat((receipt.parent/'COMPLETE').read_text().strip().replace('Z','+00:00'))
        if when.tzinfo is None or when > datetime.now(timezone.utc):
            continue
        verified.append(when)
    except (ValueError, TypeError, KeyError, AssertionError, OSError):
        continue
age = (datetime.now(timezone.utc)-max(verified)).total_seconds() if verified else float('inf')
if age > 48*3600:
    sys.exit(f'CRITICAL: verified off-host backup is {age/3600:.1f} hours old; operator computer may be offline')
print(f'Off-host backup freshness: {age/3600:.1f} hours')
