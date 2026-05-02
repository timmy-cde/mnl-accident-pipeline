import re
from datetime import datetime, date
from utils.CleanString import location_string_clean
from utils.LocationFunctions import get_locations_from_bq

word_2_num = {
    'ZERO': 0,
    'ONE': 1,
    'TWO': 2,
    'THREE': 3,
    'FOUR': 4,
    'FIVE': 5,
    'SIX': 6,
    'SEVEN': 7,
    'EIGHT': 8,
    'NINE': 9,
    'TEN': 10
}

directions = ['NB', 'SB', 'EB', 'WB']

def strip_direction(text):
    for dir in [' NB', ' EB', ' SB', ' WB', ' NB ', ' EB ', ' SB ', ' WB ']:
        text = text.replace(dir, ' ')
    return text.strip()


def get_time(tweet_text):
    tweet_text = tweet_text.upper().replace(';', ':').replace('.', '')

    # Match times like 4:30 PM, 04:30PM, 4 PM, 04PM
    pattern = re.compile(r'\b(\d{1,2})(?::(\s*\d{2}))?\s*(AM|PM)\b')
    match = pattern.search(tweet_text)
    if not match:
        return None

    hour = int(match.group(1))
    minute = int(match.group(2)) if match.group(2) else 0
    meridiem = match.group(3)
    try:
        time_obj = datetime.strptime(f"{hour:02d}:{minute:02d} {meridiem}", '%I:%M %p')
        return time_obj.strftime('%H:%M')
    except ValueError:
        return None


def get_lanes_blocked(tweet_text):
    pattern = re.compile(r'((\d\s*)|(\w*\s*))(LANE|LANES)')
    matches = pattern.finditer(tweet_text)

    tweet_lanes = ''
    for match in matches:
        tweet_text = match.group(0)
        tweet_lanes = tweet_text.split(' ')[0]

    if not tweet_lanes.strip().isdigit():
        tweet_lanes = word_2_num.get(tweet_lanes.strip(), '')

    return int(tweet_lanes) if str(tweet_lanes).isdigit() else None


def get_inc_type(tweet_text):
    parsed_incident_type = False
    tweet_text = tweet_text.upper()
    pattern = re.compile(r'MMDA ALERT: .* AT ')
    matches = pattern.finditer(tweet_text)

    for match in matches:
        tweet_text = match.group(0)
        tweetType = tweet_text.replace('MMDA ALERT: ', '').replace(' AT ', '')
        parsed_incident_type = True

    if not parsed_incident_type:
        tweetType = ''

    return tweetType


def get_direction(tweet_text):
    parsed_direction = False
    pattern = re.compile(r'( SB | NB | WB | EB | SB| NB| WB| EB)')
    matches = pattern.finditer(tweet_text)
    for match in matches:
        tweetDirection = match.group(0).strip()
        parsed_direction = True

    if not parsed_direction:
        tweetDirection = None

    return tweetDirection


def get_location(tweet_text, strip_dir=True):
    pattern = re.compile(r' AT(.*)((AS OF)|(OF))(.*(AM|PM))')
    matches = pattern.finditer(tweet_text)
    tweet_location = ''
    tweet_location_final = ''

    for match in matches:
        tweet_location = match.group(0)

        if any(direction in tweet_location for direction in directions):
            pattern2 = re.compile(r'AT\s+(.*?)(?:\s(NB|SB|EB|WB)\b|$)')
            matches2 = pattern2.finditer(tweet_location)
            for match2 in matches2:
                tweet_location_final = match2.group(1)
        else:    
            tweet_location = tweet_location.replace('AT ', '').replace(' AS OF', '').strip()
            tweet_location_final = location_string_clean(tweet_location)
            if strip_dir:
                tweet_location_final = strip_direction(tweet_location_final)

        if "INVOLVING" in tweet_location_final:
            pattern3 = re.compile(r'(.*)\s+INVOLVING')
            matches3 = pattern3.finditer(tweet_location_final)
            for match3 in matches3:
                tweet_location_final = match3.group(1)

    return tweet_location_final.replace('Ñ', 'N').strip()


def get_participants(tweet_text):
    if ' INVOLVING' in tweet_text:
        tweet_participant = tweet_text.split(' INVOLVING')[1].split('AS OF')[0].strip()
    else:
        tweet_participant = None
    return tweet_participant


def get_rally_location(tweet_text):
    parsed_rally_location = False
    pattern = re.compile(r' AT\s*(.*)\s*MORE OR')
    matches = pattern.finditer(tweet_text)

    for match in matches:
        tweetLocation = match.group(0).replace(' AT ', '').replace(' MORE OR', '')
        tweetLocation = strip_direction(tweetLocation)

        if any(direction in tweetLocation for direction in directions):
            pattern2 = re.compile(r'AT\s+(.*?)(?:\s(NB|SB|EB|WB)\b|$)')
            matches2 = pattern2.finditer(tweetLocation)
            for match2 in matches2:
                tweetLocation = match2.group(1)

        parsed_rally_location = True

    if not parsed_rally_location:
        tweetLocation = ''

    return tweetLocation


def get_rally_participants(tweet_text):
    parsed_rally_participant = False
    pattern = re.compile(r'MORE OR LESS \d+ PAX')
    matches = pattern.finditer(tweet_text)

    for match in matches:
        tweet_participant = match.group(0).replace('MORE OR LESS ', '')
        parsed_rally_participant = True

    if not parsed_rally_participant:
        tweet_participant = ''

    return tweet_participant


def get_stalled_participants(tweet_text):
    parsed_stalled_participants = False
    pattern = re.compile(r'STALLED [A-Z0-9\-\s]+DUE')
    matches = pattern.finditer(tweet_text)

    for match in matches:
        tweet_text = match.group(0).replace('STALLED ', '').replace(' DUE', '').strip()
        tweet_participants = tweet_text
        parsed_stalled_participants = True

    if not parsed_stalled_participants:
        tweet_participants = ''

    return tweet_participants

def get_date(created_at):
    parsed_date = None

    if isinstance(created_at, date) and not isinstance(created_at, datetime):
        parsed_date = created_at
    elif isinstance(created_at, datetime):
        parsed_date = created_at.date()
    elif isinstance(created_at, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y"):
            try:
                parsed_date = datetime.strptime(created_at.strip(), fmt).date()
                break
            except ValueError:
                continue

    if parsed_date is None:
        return None

    return parsed_date

def create_timestamp(date, time):
    if date is None or time is None:
        return None

    try:
        return datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
    except ValueError:
        return None

def post_parser(content, created_at, source):
    date = get_date(created_at)
    time_24 = get_time(content)
    timestamp = create_timestamp(date, time_24)
    lanes_blocked = get_lanes_blocked(content)
    inc_type = get_inc_type(content)
    direction = get_direction(content)

    if 'RALLY' in content:
        location = get_rally_location(content)
        participants = get_rally_participants(content)
    elif 'STALLED' in content:
        location = get_location(content)
        participants = get_stalled_participants(content)
    else:
        location = get_location(content)
        participants = get_participants(content)

    return (date, time_24, timestamp, location, direction, inc_type, lanes_blocked, participants, content, source)