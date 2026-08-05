from datetime import timedelta, timezone

import pytest

from recovery_control_plane.policy import (
    ApprovedProfile,
    PolicyDenied,
    RecoveryPolicy,
    _action_id,
    compile_intent,
)

from conftest import BASE_TIME


def test_action_id_ignores_model_prose_and_normalizes_timezones(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    first = compile_intent(
        evidence, repair_intent, policy, now=BASE_TIME, authority_signer=authority
    )
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
    second = compile_intent(
        evidence, revised, policy, now=BASE_TIME, authority_signer=authority
    )
    assert first.action_id == second.action_id


def test_scope_mismatch_is_denied(evidence, repair_intent, policy, authority) -> None:
    wrong_scope = evidence.model_copy(update={"partner": "marketplace_de"})
    with pytest.raises(PolicyDenied, match="different scope"):
        compile_intent(
            wrong_scope,
            repair_intent,
            policy,
            now=BASE_TIME,
            authority_signer=authority,
        )


def test_job_id_changes_the_idempotency_identity(
    evidence,
    repair_intent,
    policy,
    approved_profile,
    authority,
) -> None:
    first = compile_intent(
        evidence, repair_intent, policy, now=BASE_TIME, authority_signer=authority
    )
    changed_profile = approved_profile.model_copy(update={"recovery_job_id": 43})
    changed_policy = RecoveryPolicy(
        **{
            **policy.model_dump(),
            "profiles": {changed_profile.profile_id: changed_profile},
        }
    )
    second = compile_intent(
        evidence,
        repair_intent,
        changed_policy,
        now=BASE_TIME,
        authority_signer=authority,
    )
    assert first.action_id != second.action_id


def test_candidate_dataset_changes_action_identity_and_requires_a_new_seal(
    evidence,
    repair_intent,
    policy,
    approved_profile,
    authority,
) -> None:
    first = compile_intent(
        evidence, repair_intent, policy, now=BASE_TIME, authority_signer=authority
    )
    changed_profile = approved_profile.model_copy(
        update={"candidate_dataset": "commerce.orders_repaired_shadow"}
    )
    changed_policy = RecoveryPolicy(
        **{
            **policy.model_dump(),
            "profiles": {changed_profile.profile_id: changed_profile},
        }
    )
    second = compile_intent(
        evidence,
        repair_intent,
        changed_policy,
        now=BASE_TIME,
        authority_signer=authority,
    )

    assert first.candidate_dataset == "commerce.orders_repaired"
    assert first.target_dataset == "commerce.orders_normalized"
    assert first.action_id != second.action_id

    # Re-hashing a modified plan changes its identity, but cannot recreate the
    # registry authority's signature over that new identity and payload.
    changed = first.model_copy(
        update={"candidate_dataset": "commerce.orders_repaired_shadow"}
    )
    forged = changed.model_copy(
        update={"action_id": _action_id(changed.authority_payload())}
    )
    with pytest.raises(PermissionError, match="signature is invalid"):
        authority.verify_plan(forged)


def test_profile_currency_changes_action_identity_and_requires_a_new_seal(
    evidence,
    repair_intent,
    policy,
    approved_profile,
    authority,
) -> None:
    first = compile_intent(
        evidence, repair_intent, policy, now=BASE_TIME, authority_signer=authority
    )
    changed_profile = approved_profile.model_copy(update={"currency_code": "USD"})
    changed_policy = RecoveryPolicy(
        **{
            **policy.model_dump(),
            "profiles": {changed_profile.profile_id: changed_profile},
        }
    )
    second = compile_intent(
        evidence,
        repair_intent,
        changed_policy,
        now=BASE_TIME,
        authority_signer=authority,
    )

    assert first.currency_code == "EUR"
    assert first.action_id != second.action_id

    changed = first.model_copy(update={"currency_code": "USD"})
    forged = changed.model_copy(
        update={"action_id": _action_id(changed.authority_payload())}
    )
    with pytest.raises(PermissionError, match="signature is invalid"):
        authority.verify_plan(forged)


def test_evidence_expiry_is_bound_to_plan_and_action_identity(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    first = compile_intent(
        evidence, repair_intent, policy, now=BASE_TIME, authority_signer=authority
    )
    changed_evidence = evidence.model_copy(
        update={"evidence_expires_at": evidence.evidence_expires_at + timedelta(minutes=1)}
    )
    second = compile_intent(
        changed_evidence,
        repair_intent,
        policy,
        now=BASE_TIME,
        authority_signer=authority,
    )

    assert first.evidence_expires_at == evidence.evidence_expires_at
    assert first.execution_deadline == min(
        evidence.evidence_expires_at,
        policy.valid_until,
    )
    assert first.action_id != second.action_id


def test_replay_window_larger_than_absolute_policy_limit_is_denied(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    narrow_policy = policy.model_copy(
        update={"maximum_replay_duration": timedelta(minutes=5)}
    )

    with pytest.raises(PolicyDenied, match="maximum replay duration"):
        compile_intent(
            evidence,
            repair_intent,
            narrow_policy,
            now=BASE_TIME,
            authority_signer=authority,
        )


def test_profile_registry_key_must_match_embedded_profile_id(
    policy,
    approved_profile,
) -> None:
    payload = policy.model_dump(mode="json")
    payload["profiles"] = {"misleading-key": approved_profile.model_dump(mode="json")}

    with pytest.raises(ValueError, match="registry key"):
        RecoveryPolicy.model_validate(payload)


def test_registry_cannot_alias_candidate_to_production_dataset(
    approved_profile,
) -> None:
    payload = approved_profile.model_dump(mode="json")
    payload["candidate_dataset"] = payload["dataset"]

    with pytest.raises(ValueError, match="candidate dataset must be separate"):
        ApprovedProfile.model_validate(payload)
