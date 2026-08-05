from datetime import timedelta, timezone

import pytest

from recovery_control_plane.policy import PolicyDenied, RecoveryPolicy, compile_intent

from conftest import BASE_TIME


def test_action_id_ignores_model_prose_and_normalizes_timezones(
    evidence,
    repair_intent,
    policy,
) -> None:
    first = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    offset = timezone(timedelta(hours=2))
    revised = repair_intent.model_copy(
        update={
            "confidence": 0.31,
            "rationale": "Different model prose cannot create a second authority-bearing action.",
            "backfill_window": repair_intent.backfill_window.model_copy(
                update={
                    "start": repair_intent.backfill_window.start.astimezone(offset),
                    "end": repair_intent.backfill_window.end.astimezone(offset),
                }
            ),
        }
    )
    second = compile_intent(evidence, revised, policy, now=BASE_TIME)
    assert first.action_id == second.action_id


def test_scope_mismatch_is_denied(evidence, repair_intent, policy) -> None:
    wrong_scope = evidence.model_copy(update={"partner": "marketplace_de"})
    with pytest.raises(PolicyDenied, match="different scope"):
        compile_intent(wrong_scope, repair_intent, policy, now=BASE_TIME)


def test_job_id_changes_the_idempotency_identity(
    evidence,
    repair_intent,
    policy,
    approved_profile,
) -> None:
    first = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    changed_profile = approved_profile.model_copy(update={"recovery_job_id": 43})
    changed_policy = RecoveryPolicy(
        **{
            **policy.model_dump(),
            "profiles": {changed_profile.profile_id: changed_profile},
        }
    )
    second = compile_intent(evidence, repair_intent, changed_policy, now=BASE_TIME)
    assert first.action_id != second.action_id


def test_evidence_expiry_is_bound_to_plan_and_action_identity(
    evidence,
    repair_intent,
    policy,
) -> None:
    first = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    changed_evidence = evidence.model_copy(
        update={"evidence_expires_at": evidence.evidence_expires_at + timedelta(minutes=1)}
    )
    second = compile_intent(changed_evidence, repair_intent, policy, now=BASE_TIME)

    assert first.evidence_expires_at == evidence.evidence_expires_at
    assert first.execution_deadline == min(
        evidence.evidence_expires_at,
        policy.valid_until,
    )
    assert first.action_id != second.action_id
