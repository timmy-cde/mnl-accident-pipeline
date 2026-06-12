import re
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Tuple

from utils.CleanString import location_string_clean

WORD_2_NUM = {
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
    'TEN': 10,
}

DIRECTION_PATTERN = re.compile(r'\b(NB|SB|EB|WB|NORTHBOUND|SOUTHBOUND|EASTBOUND|WESTBOUND)\b')
TIME_PATTERN = re.compile(r'\b(?P<hour>\d{1,2})(?::\s*(?P<minute>\d{2}))?\s*(?P<meridiem>AM|PM)\b')
LANES_PATTERN = re.compile(r'\b(?P<count>\d+|ZERO|ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN)\s+LANES?\b')
INCIDENT_TYPE_PATTERN = re.compile(r'MMDA ALERT:\s*(?P<inc_type>.*?)\s+(AT|ALONG)\b')
LOCATION_PATTERN = re.compile(
    r'\b(AT|ALONG)\s+'
    r'(?P<location>.*?)'
    r'(?=\s+(?:INVOLVING|STALLED|NB|SB|EB|WB|NORTHBOUND|SOUTHBOUND|EASTBOUND|WESTBOUND|MORE OR|AS OF|$))',
    re.IGNORECASE,
)
PARTICIPANTS_PATTERN = re.compile(
    r'INVOLVING\s+(?P<participants>.*?)\s*(?=\s+(?:AS OF|OF|MORE OR|$))',
    re.IGNORECASE,
)
RALLY_LOCATION_PATTERN = re.compile(r'(AT|ALONG)\s+(?P<location>.*?)\s+MORE OR\b', re.IGNORECASE)
RALLY_PARTICIPANTS_PATTERN = re.compile(r'MORE OR LESS\s+(?P<count>\d+)\s+PAX\b', re.IGNORECASE)
STALLED_PARTICIPANTS_PATTERN = re.compile(r'STALLED\s+(?P<participants>.*?)\s+DUE\b', re.IGNORECASE)


def normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ''
    if not isinstance(value, str):
        return str(value)
    return value.strip()


def strip_direction(text: str) -> str:
    text = DIRECTION_PATTERN.sub(' ', text)
    return ' '.join(text.split())


def get_time(tweet_text: str) -> Optional[str]:
    tweet_text = normalize_text(tweet_text).upper().replace(';', ':').replace('.', '')
    match = TIME_PATTERN.search(tweet_text)
    if not match:
        return None

    hour = int(match.group('hour'))
    minute = int(match.group('minute') or 0)
    meridiem = match.group('meridiem')

    try:
        timestamp = datetime.strptime(f'{hour:02d}:{minute:02d} {meridiem}', '%I:%M %p')
        return timestamp.strftime('%H:%M')
    except ValueError:
        return None


def parse_word_number(value: str) -> Optional[int]:
    if value is None:
        return None

    candidate = value.strip().upper()
    if candidate.isdigit():
        return int(candidate)

    return WORD_2_NUM.get(candidate)


def get_lanes_blocked(tweet_text: str) -> Optional[int]:
    tweet_text = normalize_text(tweet_text).upper()
    match = LANES_PATTERN.search(tweet_text)
    if not match:
        return None

    return parse_word_number(match.group('count'))


def get_inc_type(tweet_text: str) -> str:
    tweet_text = normalize_text(tweet_text).upper()
    match = INCIDENT_TYPE_PATTERN.search(tweet_text)
    if not match:
        return ''
    return match.group('inc_type').strip()


def get_direction(tweet_text: str) -> Optional[str]:
    tweet_text = normalize_text(tweet_text).upper()
    matches = DIRECTION_PATTERN.findall(tweet_text)
    return matches[-1] if matches else None


def get_location(tweet_text: str, strip_dir: bool = True) -> str:
    tweet_text = normalize_text(tweet_text).upper()
    match = LOCATION_PATTERN.search(tweet_text)
    if not match:
        return ''

    location_text = match.group('location').strip()
    location_text = re.sub(r'\bINVOLVING\b.*$', '', location_text, flags=re.IGNORECASE).strip()
    if strip_dir:
        location_text = strip_direction(location_text)

    location_text = location_string_clean(location_text)
    return location_text.replace('Ñ', 'N').strip()


def get_participants(tweet_text: str) -> Optional[str]:
    tweet_text = normalize_text(tweet_text).upper()
    match = PARTICIPANTS_PATTERN.search(tweet_text)
    if not match:
        return None

    participants = match.group('participants').strip().rstrip(',')
    return participants or None


def get_rally_location(tweet_text: str) -> str:
    tweet_text = normalize_text(tweet_text).upper()
    match = RALLY_LOCATION_PATTERN.search(tweet_text)
    if not match:
        return ''

    location_text = match.group('location').strip()
    location_text = strip_direction(location_text)
    location_text = location_string_clean(location_text)
    return location_text.replace('Ñ', 'N').strip()


def get_rally_participants(tweet_text: str) -> Optional[int]:
    tweet_text = normalize_text(tweet_text).upper()
    match = RALLY_PARTICIPANTS_PATTERN.search(tweet_text)
    if not match:
        return None

    return int(match.group('count'))


def get_stalled_participants(tweet_text: str) -> Optional[str]:
    tweet_text = normalize_text(tweet_text).upper()
    match = STALLED_PARTICIPANTS_PATTERN.search(tweet_text)
    if not match:
        return None

    participants = match.group('participants').strip()
    return participants or None


def get_date(value) -> Optional[date]:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        value = value.strip()
        for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y', '%m/%d/%Y %H:%M:%S'):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def create_timestamp(parsed_date: Optional[date], time_text: Optional[str]) -> Optional[datetime]:
    if parsed_date is None or time_text is None:
        return None

    try:
        time_obj = datetime.strptime(time_text, '%H:%M').time()
        return datetime.combine(parsed_date, time_obj)
    except ValueError:
        return None


def post_parser(content: str, created_at, source: str) -> Tuple:
    tweet_text = normalize_text(content)
    parsed_date = get_date(created_at)
    parsed_time = get_time(tweet_text)
    timestamp = create_timestamp(parsed_date, parsed_time)
    lanes_blocked = get_lanes_blocked(tweet_text)
    inc_type = get_inc_type(tweet_text)
    direction = get_direction(tweet_text)

    if 'RALLY' in tweet_text:
        location = get_rally_location(tweet_text)
        participants = get_rally_participants(tweet_text)
    elif 'STALLED' in tweet_text:
        location = get_location(tweet_text)
        participants = get_stalled_participants(tweet_text)
    else:
        location = get_location(tweet_text)
        participants = get_participants(tweet_text)

    # Ensure participants is a string (or None) to match previous schema expectations
    participants_out = str(participants) if participants is not None else None

    # Return a plain tuple matching the expected UDF/StructType order:
    # (date, time, timestamp, location, direction, type, lanes_blocked, involved, post, link)
    return (
        parsed_date,
        parsed_time,
        timestamp,
        location,
        direction,
        inc_type,
        lanes_blocked,
        participants_out,
        tweet_text,
        normalize_text(source),
    )
