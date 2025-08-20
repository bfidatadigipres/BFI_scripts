from datetime import datetime, timedelta


def calculate_transmission_stoptime(duration: str, start_time: str) -> str | None:
    """
    Function that gets the estimated finish time for a given show.

    Parameters:
    -----------
    duration: str
        Duration time for a given show.

    start_time: str
        Start time for the show

    Returns:
    ---------
    end_time: string | none
        estimated end time (return None if the inputs are wrong)

    """
    TIME_FORMAT = "%H:%M:%S"
    try:
        duration_int = int(duration)
    except TypeError as e:
        print(e)
        return None

    start_time = datetime.strptime(start_time, time_format)
    end_time = start_time + timedelta(minutes=duration_int)

    return end_time.strftime("%H:%M:%S")
