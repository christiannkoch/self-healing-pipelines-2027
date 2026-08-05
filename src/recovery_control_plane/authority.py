"""Authenticated authority envelopes for plans and human approvals.

The interfaces in this module are deliberately narrower than a particular key
service.  Production implementations can verify signatures with KMS, an HSM,
or an approval service.  :class:`HmacRecoveryAuthority` is the local reference
implementation; callers must inject key material and must never commit it.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal, Mapping, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

if TYPE_CHECKING:  # Avoid a runtime import cycle with policy.py.
    from .coordinator import VerificationReceipt
    from .policy import CompiledPlan


def _canonical_json(payload: Mapping[str, object]) -> bytes:
    """Serialize one authority payload identically at signing and verification."""

    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


class AuthoritySeal(BaseModel):
    """Detached signature metadata carried by a compiled plan."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    key_id: str = Field(min_length=1, max_length=200)
    issuer: str = Field(min_length=1, max_length=200)
    algorithm: Literal["HMAC-SHA256"] = "HMAC-SHA256"
    signature: str = Field(pattern=r"^[0-9a-f]{64}$")


class ExecutionApproval(BaseModel):
    """Authenticated, action-bound approval record from an authority service."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["execution-approval/v1"] = "execution-approval/v1"
    action_id: str = Field(pattern=r"^rpr-[0-9a-f]{60}$")
    record_id: str = Field(min_length=1, max_length=200)
    approved_by: str = Field(min_length=1, max_length=200)
    issuer: str = Field(min_length=1, max_length=200)
    approved_at: datetime
    expires_at: datetime
    key_id: str = Field(min_length=1, max_length=200)
    algorithm: Literal["HMAC-SHA256"] = "HMAC-SHA256"
    signature: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_timestamps(self) -> "ExecutionApproval":
        if self.approved_at.tzinfo is None or self.expires_at.tzinfo is None:
            raise ValueError("approval timestamps must include a timezone")
        if self.expires_at <= self.approved_at:
            raise ValueError("approval expiry must be after approval time")
        return self

    def authority_payload(self) -> dict[str, object]:
        """Return exactly the approval fields covered by its signature."""

        return {
            "schema_version": self.schema_version,
            "action_id": self.action_id,
            "record_id": self.record_id,
            "approved_by": self.approved_by,
            "issuer": self.issuer,
            "approved_at": self.approved_at.astimezone(timezone.utc).isoformat(),
            "expires_at": self.expires_at.astimezone(timezone.utc).isoformat(),
            "key_id": self.key_id,
            "algorithm": self.algorithm,
        }

    def validate_scope(
        self,
        plan: "CompiledPlan",
        *,
        now: datetime,
        require_current: bool = True,
    ) -> None:
        """Validate temporal and action scope after signature authentication."""

        if now.tzinfo is None:
            raise ValueError("approval verification time must include a timezone")
        if self.action_id != plan.action_id:
            raise ValueError("approval belongs to a different recovery action")
        current = now.astimezone(timezone.utc)
        approved = self.approved_at.astimezone(timezone.utc)
        expires = self.expires_at.astimezone(timezone.utc)
        deadline = plan.execution_deadline.astimezone(timezone.utc)
        if approved > current:
            raise ValueError("approval timestamp cannot be in the future")
        if expires > deadline:
            raise ValueError("approval cannot outlive compiled execution authority")
        if approved >= deadline:
            raise ValueError("approval was issued after execution authority expired")
        if require_current and current >= expires:
            raise PermissionError("human approval has expired")


class PlanAuthoritySigner(Protocol):
    """Sign a compiler-produced, content-addressed plan payload."""

    def sign_plan(
        self,
        *,
        action_id: str,
        authority_payload: Mapping[str, object],
    ) -> AuthoritySeal: ...


class PlanAuthorityVerifier(Protocol):
    """Authenticate a compiled plan before any effect or promotion."""

    def verify_plan(self, plan: "CompiledPlan") -> None: ...


class ApprovalAuthorityVerifier(Protocol):
    """Authenticate an approval record and validate its live action scope."""

    def verify_approval(
        self,
        approval: ExecutionApproval,
        plan: "CompiledPlan",
        *,
        now: datetime,
        require_current: bool = True,
    ) -> None: ...


class VerificationReceiptAuthoritySigner(Protocol):
    """Authenticate deterministic evidence proof before it enters the journal."""

    def sign_verification_receipt(
        self,
        *,
        proof_payload: Mapping[str, object],
        proof_digest: str,
    ) -> AuthoritySeal: ...


class VerificationReceiptAuthorityVerifier(Protocol):
    """Authenticate a reloaded proof immediately before it can authorize promotion."""

    def verify_verification_receipt(
        self,
        receipt: "VerificationReceipt",
    ) -> None: ...


class HmacRecoveryAuthority:
    """Local symmetric-key signer/verifier with caller-supplied key material.

    This is suitable for the offline reference implementation and tests.  A
    production control plane can use a centrally governed KMS-managed HMAC key
    or authoritative lookup service. Supporting an asymmetric algorithm also
    requires a separately reviewed extension to ``AuthoritySeal.algorithm``.
    """

    _PLAN_DOMAIN = b"compiled-plan-authority/v1\x00"
    _APPROVAL_DOMAIN = b"execution-approval/v1\x00"
    _RECEIPT_DOMAIN = b"verification-receipt-authority/v1\x00"

    def __init__(self, *, key_id: str, issuer: str, secret: bytes) -> None:
        if not key_id.strip() or not issuer.strip():
            raise ValueError("authority key ID and issuer must be non-empty")
        if len(secret) < 32:
            raise ValueError("HMAC authority secret must contain at least 32 bytes")
        self.key_id = key_id
        self.issuer = issuer
        self._secret = bytes(secret)

    def _sign(self, domain: bytes, payload: Mapping[str, object]) -> str:
        return hmac.new(
            self._secret,
            domain + _canonical_json(payload),
            hashlib.sha256,
        ).hexdigest()

    def sign_plan(
        self,
        *,
        action_id: str,
        authority_payload: Mapping[str, object],
    ) -> AuthoritySeal:
        signed = {"action_id": action_id, "authority": dict(authority_payload)}
        return AuthoritySeal(
            key_id=self.key_id,
            issuer=self.issuer,
            signature=self._sign(self._PLAN_DOMAIN, signed),
        )

    def verify_plan(self, plan: "CompiledPlan") -> None:
        seal = plan.authority_seal
        if seal.key_id != self.key_id or seal.issuer != self.issuer:
            raise PermissionError("compiled plan was issued by an untrusted authority")
        signed = {"action_id": plan.action_id, "authority": plan.authority_payload()}
        expected = self._sign(self._PLAN_DOMAIN, signed)
        if not hmac.compare_digest(seal.signature, expected):
            raise PermissionError("compiled plan authority signature is invalid")

    def issue_approval(
        self,
        *,
        plan: "CompiledPlan",
        record_id: str,
        approved_by: str,
        approved_at: datetime,
        expires_at: datetime,
    ) -> ExecutionApproval:
        """Sign an envelope after an external workflow authenticates the human.

        HMAC authenticates this configured issuer and the envelope's integrity;
        it does *not* authenticate ``approved_by`` or decide whether that human
        has an authorized role. Production callers must obtain both facts from
        a governed identity/approval service rather than caller-supplied text.
        """

        unsigned = ExecutionApproval(
            action_id=plan.action_id,
            record_id=record_id,
            approved_by=approved_by,
            issuer=self.issuer,
            approved_at=approved_at,
            expires_at=expires_at,
            key_id=self.key_id,
            signature="0" * 64,
        )
        signature = self._sign(self._APPROVAL_DOMAIN, unsigned.authority_payload())
        return unsigned.model_copy(update={"signature": signature})

    def verify_approval(
        self,
        approval: ExecutionApproval,
        plan: "CompiledPlan",
        *,
        now: datetime,
        require_current: bool = True,
    ) -> None:
        if approval.key_id != self.key_id or approval.issuer != self.issuer:
            raise PermissionError("approval was issued by an untrusted authority")
        expected = self._sign(self._APPROVAL_DOMAIN, approval.authority_payload())
        if not hmac.compare_digest(approval.signature, expected):
            raise PermissionError("approval signature is invalid")
        approval.validate_scope(plan, now=now, require_current=require_current)

    def sign_verification_receipt(
        self,
        *,
        proof_payload: Mapping[str, object],
        proof_digest: str,
    ) -> AuthoritySeal:
        """Sign the complete verifier proof under a receipt-specific domain."""

        signed = {
            "proof_digest": proof_digest,
            "proof": dict(proof_payload),
        }
        return AuthoritySeal(
            key_id=self.key_id,
            issuer=self.issuer,
            signature=self._sign(self._RECEIPT_DOMAIN, signed),
        )

    def verify_verification_receipt(
        self,
        receipt: "VerificationReceipt",
    ) -> None:
        seal = receipt.authority_seal
        if seal.key_id != self.key_id or seal.issuer != self.issuer:
            raise PermissionError(
                "verification receipt was issued by an untrusted authority"
            )
        signed = {
            "proof_digest": receipt.proof_digest,
            "proof": receipt.proof_payload(),
        }
        expected = self._sign(self._RECEIPT_DOMAIN, signed)
        if not hmac.compare_digest(seal.signature, expected):
            raise PermissionError("verification receipt authority signature is invalid")
