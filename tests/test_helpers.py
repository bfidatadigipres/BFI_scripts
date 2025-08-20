from helpers import stora_helper
import pytest

@pytest.mark.parametrize(
    "duration_input, start_time_input, raise_error, expected_output",
    [
        ("25", "02:25:00", False, "02:50:00"),
        ("m", "02:25:00", True, ValueError),
        ('25', '28:15:00', True, ValueError)
        ("5", "0:15:0", False, "00:20:00"),
    ],
)
def test_calculate_transmission_stoptime(duration_input, start_time_input, raise_error, expected_output):

    if raise_error:
        with pytest.raises(expected_output):
            stora_helper.calculate_transmission_stoptime(duration_input, start_time_input)
    else:
        end_time  = stora_helper.calculate_transmission_stoptime(duration_input, start_time_input)

        assert expected_output == end_time