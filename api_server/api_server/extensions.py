from datetime import datetime

epoch = datetime.utcfromtimestamp(0)
def get_millis_since_epoch(dt):
    return round((dt - epoch).total_seconds() * 1000.0)
