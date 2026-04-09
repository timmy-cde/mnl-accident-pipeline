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
    """
    Remove direction indicators from text.
    """
    for dir in [' NB', ' EB', ' SB', ' WB', ' NB ', ' EB ', ' SB ', ' WB ']:
        text = text.replace(dir, ' ')
    return text.strip()


def get_time(tweet_text):
    """
    Extract time from MMDA tweet and convert it to 24-hour format.
    """
    tweet_text = tweet_text.upper().replace(';', ':').replace('.', '')
    # print('get_time(): Raw input {}'.format(tweet_text))

    # Match times like 4:30 PM, 04:30PM, 4 PM, 04PM
    pattern = re.compile(r'\b(\d{1,2})(?::(\d{2}))?\s*(AM|PM)\b')
    match = pattern.search(tweet_text)
    if not match:
        print('get_time(): No time found')
        return ''

    hour = int(match.group(1))
    minute = int(match.group(2)) if match.group(2) else 0
    meridiem = match.group(3)
    try:
        time_obj = datetime.strptime(f"{hour:02d}:{minute:02d} {meridiem}", '%I:%M %p')
        time_24 = time_obj.strftime('%H:%M')
        # print('get_time(): Parsed output {}'.format(time_24))
        return time_24
    except ValueError:
        print('get_time(): Failed to parse time {}'.format(match.group(0)))
        return ''


def get_lanes_blocked(tweet_text):
    """
    Extract number of lanes blocked from tweet.
    """
    pattern = re.compile(r'((\d\s*)|(\w*\s*))(LANE|LANES)')
    matches = pattern.finditer(tweet_text)
    # print('get_lanes_blocked(): Raw input {}'.format(tweet_text))

    tweet_lanes = ''
    for match in matches:
        tweet_text = match.group(0)
        # print('get_lanes_blocked(): RegEx Match {}'.format(tweet_text))
        tweet_lanes = tweet_text.split(' ')[0]

    if not tweet_lanes.strip().isdigit():
        tweet_lanes = word_2_num.get(tweet_lanes.strip(), '')

    # print('get_lanes_blocked(): Cleaned output {}'.format(tweet_lanes))
    return tweet_lanes


def get_inc_type(tweet_text):
    """
    Extract incident type from tweet.
    """
    parsed_incident_type = False
    tweet_text = tweet_text.upper()
    pattern = re.compile(r'MMDA ALERT: .* AT ')
    matches = pattern.finditer(tweet_text)
    # print('get_inc_type(): Raw input {}'.format(tweet_text))

    for match in matches:
        tweet_text = match.group(0)
        # print('get_inc_type(): RegEx Match {}'.format(tweet_text))
        tweetType = tweet_text.replace('MMDA ALERT: ', '').replace(' AT ', '')
        # print('get_inc_type(): Cleaned output {}'.format(tweetType))
        parsed_incident_type = True

    if not parsed_incident_type:
        tweetType = ''
        # print('get_inc_type(): Empty output')

    return tweetType


def get_direction(tweet_text):
    """
    Extract direction from tweet.
    """
    parsed_direction = False
    pattern = re.compile(r'( SB | NB | WB | EB | SB| NB| WB| EB)')
    matches = pattern.finditer(tweet_text)
    for match in matches:
        tweetDirection = match.group(0).strip()
        parsed_direction = True

    if not parsed_direction:
        tweetDirection = ''

    return tweetDirection


def get_location(tweet_text, strip_dir=True):
    """
    Extract location from tweet.
    """
    # pattern = re.compile(r' AT\s[a-zA-Z\Ñ\'\.\,\-0-9\/\s]+(AS OF)')
    pattern = re.compile(r' AT(.*)((AS OF)|(OF))(.*(AM|PM))')
    matches = pattern.finditer(tweet_text)
    tweet_location = ''
    tweet_location_final = ''

    for match in matches:
        tweet_location = match.group(0)
        # print('get_location(): RegEx Match {}'.format(tweet_location))

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

    # print('get_location(): Cleaned Location {}'.format(tweet_location_final))
    return tweet_location_final.replace('Ñ', 'N').strip()


def get_participants(tweet_text):
    """
    Extract participants from tweet.
    """
    print('get_participants(): Raw input {}'.format(tweet_text))
    if ' INVOLVING' in tweet_text:
        tweet_participant = tweet_text.split(' INVOLVING')[1].split('AS OF')[0].strip()
        print('get_participants(): Cleaned output {}'.format(tweet_participant))
    else:
        tweet_participant = ''
    return tweet_participant


def get_rally_location(tweet_text):
    """
    Extract rally location from tweet.
    """
    parsed_rally_location = False
    pattern = re.compile(r' AT\s*(.*)\s*MORE OR')
    matches = pattern.finditer(tweet_text)
    # print('get_rally_location(): Raw input {}'.format(tweet_text))

    for match in matches:
        tweetLocation = match.group(0).replace(' AT ', '').replace(' MORE OR', '')
        tweetLocation = strip_direction(tweetLocation)
        # print('get_rally_location(): Cleaned string {}'.format(tweetLocation))

        if any(direction in tweetLocation for direction in directions):
            pattern2 = re.compile(r'AT\s+(.*?)(?:\s(NB|SB|EB|WB)\b|$)')
            matches2 = pattern2.finditer(tweetLocation)
            for match2 in matches2:
                tweetLocation = match2.group(1)
            # print('get_location(): Cleaned Location {}'.format(tweetLocation))

        parsed_rally_location = True

    if not parsed_rally_location:
        tweetLocation = ''
        # print('get_rally_location(): Empty match')

    return tweetLocation


def get_rally_participants(tweet_text):
    """
    Extract rally participants from tweet.
    """
    parsed_rally_participant = False
    pattern = re.compile(r'MORE OR LESS \d+ PAX')
    matches = pattern.finditer(tweet_text)
    # print('get_rally_participants(): Raw input {}'.format(tweet_text))

    for match in matches:
        tweet_participant = match.group(0).replace('MORE OR LESS ', '')
        # print('get_rally_participants(): Cleaned output {}'.format(tweet_participant))
        parsed_rally_participant = True

    if not parsed_rally_participant:
        tweet_participant = ''

    return tweet_participant


def get_stalled_participants(tweet_text):
    """
    Extract stalled participants from tweet.
    """
    parsed_stalled_participants = False
    # print('get_stalled_participants(): Raw input {}'.format(tweet_text))
    pattern = re.compile(r'STALLED [A-Z0-9\-\s]+DUE')
    matches = pattern.finditer(tweet_text)

    for match in matches:
        tweet_text = match.group(0).replace('STALLED ', '').replace(' DUE', '').strip()
        tweet_participants = tweet_text
        # print('get_stalled_participants(): Cleaned String {}'.format(tweet_participants))
        parsed_stalled_participants = True

    if not parsed_stalled_participants:
        tweet_participants = ''
        # print('get_stalled_participants(): Empty Match')

    return tweet_participants

def get_location_details_raw(location):
    latitude, longitude, city, accuracy = get_locations_from_bq(location)

    return (latitude, longitude, city, accuracy)

def get_date_details(created_at):
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

    return (
        parsed_date,                            # full date
        parsed_date.year,                       # year
        parsed_date.month,                      # month
        parsed_date.day,                        # day of month
        parsed_date.isocalendar()[1],           # ISO week number
        parsed_date.weekday()                   # day of week (e.g., Monday)
    )

def post_parser(content, created_at, source):
    date, year, month, day, week, weekday = get_date_details(created_at)
    time = get_time(content)
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

    return (date, time, location, direction, inc_type, lanes_blocked, participants, content, source)