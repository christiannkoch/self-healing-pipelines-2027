"""Content-addressed mapping-profile definitions."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


def digest_profile_spec(spec: dict[str, Any]) -> str:
    """Return a stable SHA-256 digest for JSON-compatible profile metadata."""

    payload = json.dumps(spec, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class MappingProfile(BaseModel):
    """Immutable registry artifact naming one pre-reviewed normalizer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    profile_id: str = Field(min_length=1, max_length=200)
    partner: str = Field(pattern=r"^[a-z0-9_]{1,64}$")
    payload_version: str = Field(min_length=1, max_length=40)
    source_fingerprint: str = Field(min_length=1, max_length=200)
    normalizer_key: str = Field(min_length=1, max_length=100)
    normalizer_version: str = Field(min_length=1, max_length=40)
    normalizer_source_digest: str = Field(min_length=1, max_length=200)
    currency_code: str = Field(pattern=r"^[A-Z]{3}$")
    currency_exponent: int = Field(ge=0, le=9)
    dedupe_key: tuple[str, ...] = Field(min_length=1, max_length=10)
    profile_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    def digest_payload(self) -> dict[str, Any]:
        """Return exactly the fields covered by ``profile_digest``."""

        return self.model_dump(exclude={"profile_digest"}, mode="json")

    @model_validator(mode="after")
    def validate_digest(self) -> "MappingProfile":
        if digest_profile_spec(self.digest_payload()) != self.profile_digest:
            raise ValueError("profile digest does not match its content")
        return self


def build_profile(**spec: Any) -> MappingProfile:
    """Construct a profile and compute its digest in one deterministic step."""

    canonical_spec = dict(spec)
    canonical_spec["dedupe_key"] = tuple(canonical_spec["dedupe_key"])
    return MappingProfile(
        **canonical_spec,
        profile_digest=digest_profile_spec(
            {**canonical_spec, "dedupe_key": list(canonical_spec["dedupe_key"])}
        ),
    )

