import pytest

from recovery_control_plane.normalization import NormalizationError, units_nanos_to_minor


def test_negative_units_and_nanos_preserve_refund_sign() -> None:
    assert units_nanos_to_minor(-8, -500_000_000, 2) == -850


def test_mixed_sign_money_is_rejected() -> None:
    with pytest.raises(NormalizationError, match="compatible signs"):
        units_nanos_to_minor(-8, 500_000_000, 2)


def test_sub_cent_value_is_rejected_instead_of_rounded() -> None:
    with pytest.raises(NormalizationError, match="precision below"):
        units_nanos_to_minor(1, 1, 2)


@pytest.mark.parametrize("malformed", [True, "8", 8.0])
def test_money_wire_values_are_strict_integers(malformed) -> None:
    with pytest.raises(NormalizationError, match="strict integers"):
        units_nanos_to_minor(malformed, 0, 2)


def test_negative_sub_unit_refund_is_exact() -> None:
    assert units_nanos_to_minor(0, -500_000_000, 2) == -50
