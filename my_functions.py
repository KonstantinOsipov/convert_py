from datetime import datetime

def find_last_index(text):
    start = text.find('Impulse_')
    end = start + len('Impulse_')
    timestamp = text[len(text)-18:len(text)-4]
    date_parts = timestamp.split("_")
    timestamp = date_parts[0] + ".2023_" + date_parts[1]
    dt = datetime.strptime(timestamp, "%m.%d.%Y_%H.%M.%S")
    iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S")
    return text[end:len(text)-4], iso_timestamp