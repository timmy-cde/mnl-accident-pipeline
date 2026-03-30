import re
import logging
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
    Extract time from MMDA tweet.
    """
    tweet_text = tweet_text.upper().replace(';', ':')
    pattern = re.compile(r'\d+:\d\d[\s(AM|PM)]+')
    matches = pattern.finditer(tweet_text)
    logging.info('get_time(): Raw input {}'.format(tweet_text))

    for match in matches:
        tweet_text = match.group(0)
        logging.info('get_time(): RegEx Match {}'.format(tweet_text))
        tweet_text = tweet_text.replace('.', '')

    if len(tweet_text) > 10:
        return ''

    numList = ['1','2','3','4','5','6','7','8','9','0',':']
    pattern = re.compile(r'[0-9]\s[(AM|PM)]')
    matches = pattern.finditer(tweet_text)
    timeCheck = ''
    for match in matches:
        timeCheck = match.group(0)
        logging.info('get_time(): TimeCheck Var {}'.format(timeCheck))

    pattern = re.compile(r'(AM|PM)')
    matches = pattern.finditer(tweet_text)
    timeDay = ''
    for match in matches:
        timeDay = match.group(0)
        logging.info('get_time(): timeDay Var {}'.format(timeDay))

    if len(timeCheck) == 0 and ('AM' in tweet_text or 'PM' in tweet_text):
        stringFix = ''.join([x for x in tweet_text if x in numList])
        tweet_text = stringFix + ' ' + timeDay
        logging.info('get_time(): Cleaned Output {}'.format(tweet_text))

    return tweet_text


def get_lanes_blocked(tweet_text):
    """
    Extract number of lanes blocked from tweet.
    """
    pattern = re.compile(r'((\d\s*)|(\w*\s*))(LANE|LANES)')
    matches = pattern.finditer(tweet_text)
    logging.info('get_lanes_blocked(): Raw input {}'.format(tweet_text))

    tweet_lanes = ''
    for match in matches:
        tweet_text = match.group(0)
        logging.info('get_lanes_blocked(): RegEx Match {}'.format(tweet_text))
        tweet_lanes = tweet_text.split(' ')[0]

    if not tweet_lanes.strip().isdigit():
        tweet_lanes = word_2_num.get(tweet_lanes.strip(), '')

    logging.info('get_lanes_blocked(): Cleaned output {}'.format(tweet_lanes))
    return tweet_lanes


def get_inc_type(tweet_text):
    """
    Extract incident type from tweet.
    """
    parsed_incident_type = False
    tweet_text = tweet_text.upper()
    pattern = re.compile(r'MMDA ALERT: .* AT ')
    matches = pattern.finditer(tweet_text)
    logging.info('get_inc_type(): Raw input {}'.format(tweet_text))

    for match in matches:
        tweet_text = match.group(0)
        logging.info('get_inc_type(): RegEx Match {}'.format(tweet_text))
        tweetType = tweet_text.replace('MMDA ALERT: ', '').replace(' AT ', '')
        logging.info('get_inc_type(): Cleaned output {}'.format(tweetType))
        parsed_incident_type = True

    if not parsed_incident_type:
        tweetType = ''
        logging.info('get_inc_type(): Empty output')

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
        logging.info('get_location(): RegEx Match {}'.format(tweet_location))

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

    logging.info('get_location(): Cleaned Location {}'.format(tweet_location_final))
    return tweet_location_final.replace('Ñ', 'N').strip()


def get_participants(tweet_text):
    """
    Extract participants from tweet.
    """
    logging.info('get_participants(): Raw input {}'.format(tweet_text))
    if ' INVOLVING' in tweet_text:
        tweet_participant = tweet_text.split(' INVOLVING')[1].split('AS OF')[0].strip()
        logging.info('get_participants(): Cleaned output {}'.format(tweet_participant))
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
    logging.info('get_rally_location(): Raw input {}'.format(tweet_text))

    for match in matches:
        tweetLocation = match.group(0).replace(' AT ', '').replace(' MORE OR', '')
        tweetLocation = strip_direction(tweetLocation)
        logging.info('get_rally_location(): Cleaned string {}'.format(tweetLocation))

        if any(direction in tweetLocation for direction in directions):
            pattern2 = re.compile(r'AT\s+(.*?)(?:\s(NB|SB|EB|WB)\b|$)')
            matches2 = pattern2.finditer(tweetLocation)
            for match2 in matches2:
                tweetLocation = match2.group(1)
            logging.info('get_location(): Cleaned Location {}'.format(tweetLocation))

        parsed_rally_location = True

    if not parsed_rally_location:
        tweetLocation = ''
        logging.info('get_rally_location(): Empty match')

    return tweetLocation


def get_rally_participants(tweet_text):
    """
    Extract rally participants from tweet.
    """
    parsed_rally_participant = False
    pattern = re.compile(r'MORE OR LESS \d+ PAX')
    matches = pattern.finditer(tweet_text)
    logging.info('get_rally_participants(): Raw input {}'.format(tweet_text))

    for match in matches:
        tweet_participant = match.group(0).replace('MORE OR LESS ', '')
        logging.info('get_rally_participants(): Cleaned output {}'.format(tweet_participant))
        parsed_rally_participant = True

    if not parsed_rally_participant:
        tweet_participant = ''

    return tweet_participant


def get_stalled_participants(tweet_text):
    """
    Extract stalled participants from tweet.
    """
    parsed_stalled_participants = False
    logging.info('get_stalled_participants(): Raw input {}'.format(tweet_text))
    pattern = re.compile(r'STALLED [A-Z0-9\-\s]+DUE')
    matches = pattern.finditer(tweet_text)

    for match in matches:
        tweet_text = match.group(0).replace('STALLED ', '').replace(' DUE', '').strip()
        tweet_participants = tweet_text
        logging.info('get_stalled_participants(): Cleaned String {}'.format(tweet_participants))
        parsed_stalled_participants = True

    if not parsed_stalled_participants:
        tweet_participants = ''
        logging.info('get_stalled_participants(): Empty Match')

    return tweet_participants

def get_location_details_raw(location):
    latitude, longitude, city, accuracy = get_locations_from_bq(location)

    return (latitude, longitude, city, accuracy)

def normalize_date(created_at):
    if isinstance(created_at, date):
        return created_at
    if isinstance(created_at, datetime):
        return created_at.date()
    if isinstance(created_at, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", ...):
            try:
                return datetime.strptime(created_at.strip(), fmt).date()
            except ValueError:
                pass
    return None

def post_parser(content, created_at, source):
    time = get_time(content)
    date = normalize_date(created_at)
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