"""Supply-chain checks for the reviewed normalizer specification."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from recovery_control_plane.profiles import (
    MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST,
)


ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "normalizers" / "marketplace_v3_gross_1_0_1.json"


def test_normalizer_digest_matches_the_canonical_reviewed_spec() -> None:
    """Prevent a manually copied digest from drifting away from its artifact."""

    document = json.loads(SPEC.read_text(encoding="utf-8"))
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    assert MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST == (
        "sha256:" + hashlib.sha256(canonical).hexdigest()
    )
    assert document["normalizer_key"] == "MARKETPLACE_V3_GROSS"
    assert document["normalizer_version"] == "1.0.1"
    assert document["dedupe_key"] == ["partner", "order_id", "source_sequence"]
