from datetime import datetime, timedelta

def calculate_transmission_stoptime(duration: str, time: str) -> str:
    time_format = '%H:%M:%S'
    try:
        duration_int = int(duration)
    except TypeError as e:
        print(e)
        return 'no endtime in sight'

    start_time = datetime.strptime(time, time_format)
    end_time = start_time + timedelta(minutes=duration_int)

    return end_time.strftime("%H:%M:%S")