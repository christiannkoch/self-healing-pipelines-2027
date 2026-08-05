import pytest
from pydantic import ValidationError

from recovery_control_plane.contracts import IncidentEvidence


def test_failure_passport_requires_explicit_redaction_attestation(evidence) -> None:
    payload = evidence.model_dump(mode="json")
    del payload["redaction_attested"]

    with pytest.raises(ValidationError, match="redaction_attested"):
        IncidentEvidence.model_validate(payload)


def test_failure_passport_rejects_obvious_unredacted_sample(evidence) -> None:
    payload = evidence.model_dump(mode="json")
    payload["redacted_samples"] = ["customer email is person@example.com"]

    with pytest.raises(ValidationError, match="unredacted identifiers"):
        IncidentEvidence.model_validate(payload)


def test_failure_passport_bounds_business_check_shape(evidence) -> None:
    payload = evidence.model_dump(mode="json")
    payload["business_checks"] = {f"check_{index}": index for index in range(21)}

    with pytest.raises(ValidationError):
        IncidentEvidence.model_validate(payload)
