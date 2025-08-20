import pytest

from helpers import stora_helper


@pytest.mark.parametrize(
    "duration_input, start_time_input, raise_error, expected_output",
    [
        ("25", "02:25:00", False, "02:50:00"),
        ("m", "02:25:00", True, ValueError),
        ("25", "28:15:00", True, ValueError),
        ("5", "0:15:0", False, "00:20:00"),
    ],
)
def test_calculate_transmission_stoptime(
    duration_input, start_time_input, raise_error, expected_output
):
    """
    Test 'calculate_transmission_stoptime function' from stora_helper.py

    This test validates that the function correctly computesthe transmission stop time
    based on the given start time and durations. It also verifies that the approiate 
    exceptions are raised for invalid input formates or out-of-range values.

    Parameters:
    -----------
    duration_input: str
        duration in minutes
    start_time_input: str
        start time in 'HH:MM:SS'
    raise_error: bool
        flag whether the test expect an exception.
    expected_output: str | None
        Expected stop time (as a string) or the expected
        exception type if 'raise_error' is True.
    """

    if raise_error:
        with pytest.raises(expected_output):
            stora_helper.calculate_transmission_stoptime(
                duration_input, start_time_input
            )
    else:
        end_time = stora_helper.calculate_transmission_stoptime(
            duration_input, start_time_input
        )

        assert expected_output == end_time
