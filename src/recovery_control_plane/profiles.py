"""Content-addressed mapping-profile definitions."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


MARKETPLACE_V3_NORMALIZER_KEY = "MARKETPLACE_V3_GROSS"
MARKETPLACE_V3_NORMALIZER_VERSION = "1.0.1"
MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST = (
    "sha256:deefca6e8c07abf7e1acb6c7a890d54bc0aed343774b2a5c3a4b1ccd6a784ac1"
)

# A profile may name only an implementation that is actually shipped by this
# package.  The model therefore cannot be used to bless an invented function
# name, version, or source artifact merely by recomputing ``profile_digest``.
SUPPORTED_NORMALIZER_MANIFESTS = frozenset(
    {
        (
            MARKETPLACE_V3_NORMALIZER_KEY,
            MARKETPLACE_V3_NORMALIZER_VERSION,
            MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST,
        )
    }
)

SUPPORTED_DEDUPE_FIELDS = frozenset(
    {"partner", "order_id", "source_sequence", "transport_id"}
)
StrictExponent = Annotated[int, Field(strict=True, ge=0, le=9)]


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
    normalizer_source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    currency_code: str = Field(pattern=r"^[A-Z]{3}$")
    currency_exponent: StrictExponent
    legacy_payload_versions: tuple[str, ...] = Field(
        default=("v2",), min_length=1, max_length=10
    )
    dedupe_key: tuple[str, ...] = Field(min_length=1, max_length=10)
    profile_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("legacy_payload_versions")
    @classmethod
    def validate_legacy_versions(cls, versions: tuple[str, ...]) -> tuple[str, ...]:
        if versions != ("v2",):
            raise ValueError("the shipped normalizer supports exactly legacy payload version v2")
        if len(set(versions)) != len(versions):
            raise ValueError("legacy payload versions must be unique")
        if any(not version or len(version) > 40 for version in versions):
            raise ValueError("legacy payload versions must be non-empty and at most 40 characters")
        return versions

    @field_validator("dedupe_key")
    @classmethod
    def validate_dedupe_key(cls, fields: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(fields)) != len(fields):
            raise ValueError("dedupe fields must be unique")
        unsupported = set(fields) - SUPPORTED_DEDUPE_FIELDS
        if unsupported:
            raise ValueError(f"unsupported dedupe fields: {sorted(unsupported)}")
        return fields

    def digest_payload(self) -> dict[str, Any]:
        """Return exactly the fields covered by ``profile_digest``."""

        return self.model_dump(exclude={"profile_digest"}, mode="json")

    @model_validator(mode="after")
    def validate_digest(self) -> "MappingProfile":
        manifest = (
            self.normalizer_key,
            self.normalizer_version,
            self.normalizer_source_digest,
        )
        if manifest not in SUPPORTED_NORMALIZER_MANIFESTS:
            raise ValueError("profile names a normalizer implementation that is not allowlisted")
        if (
            self.partner != "marketplace_it"
            or self.payload_version != "v3"
            or self.currency_code != "EUR"
            or self.currency_exponent != 2
        ):
            raise ValueError(
                "the shipped MARKETPLACE_V3_GROSS normalizer supports only "
                "marketplace_it/v3 EUR with exponent 2"
            )
        if self.payload_version in self.legacy_payload_versions:
            raise ValueError("profile payload version cannot also be a legacy version")
        if digest_profile_spec(self.digest_payload()) != self.profile_digest:
            raise ValueError("profile digest does not match its content")
        return self


def build_profile(**spec: Any) -> MappingProfile:
    """Construct a profile and compute its digest in one deterministic step."""

    canonical_spec = dict(spec)
    canonical_spec["dedupe_key"] = tuple(canonical_spec["dedupe_key"])
    canonical_spec["legacy_payload_versions"] = tuple(
        canonical_spec.get("legacy_payload_versions", ("v2",))
    )
    return MappingProfile(
        **canonical_spec,
        profile_digest=digest_profile_spec(
            {
                **canonical_spec,
                "dedupe_key": list(canonical_spec["dedupe_key"]),
                "legacy_payload_versions": list(
                    canonical_spec["legacy_payload_versions"]
                ),
            }
        ),
    )
