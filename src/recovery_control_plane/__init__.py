"""Governed recovery-control-plane reference implementation.

The default package path is deliberately side-effect free. Optional LangGraph
and Databricks integrations are constructed explicitly by callers.
"""

from .contracts import (
    BackfillWindow,
    IncidentEvidence,
    IntentReview,
    RepairAction,
    RepairIntent,
)
from .authority import (
    ApprovalAuthorityVerifier,
    ExecutionApproval,
    HmacRecoveryAuthority,
    PlanAuthoritySigner,
    PlanAuthorityVerifier,
    VerificationReceiptAuthoritySigner,
    VerificationReceiptAuthorityVerifier,
)
from .coordinator import RecoveryCoordinator, VerificationReceipt
from .policy import CompiledPlan, PolicyDenied, compile_intent

__all__ = [
    "BackfillWindow",
    "ApprovalAuthorityVerifier",
    "CompiledPlan",
    "IncidentEvidence",
    "IntentReview",
    "ExecutionApproval",
    "HmacRecoveryAuthority",
    "PlanAuthoritySigner",
    "PlanAuthorityVerifier",
    "VerificationReceiptAuthoritySigner",
    "VerificationReceiptAuthorityVerifier",
    "PolicyDenied",
    "RepairAction",
    "RepairIntent",
    "RecoveryCoordinator",
    "VerificationReceipt",
    "compile_intent",
]
