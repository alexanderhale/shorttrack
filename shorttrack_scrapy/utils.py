import logging
import re
from os.path import exists

import pandas as pd

from shorttrack_scrapy.constants import RAW_SPLIT_DIR, REGEX_BAD_CHARS, RAW_ROUND_DIR, ROUNDS_FILE, EVENT_NAME_MAPPING, \
    UNTREATABLE_EVENTS


def load_already_scraped(unique_column_set: list) -> pd.DataFrame:
    """
    If some results have already been scraped, load in the details so it isn't downloaded again.
    """
    if exists(ROUNDS_FILE):
        return pd.read_csv(ROUNDS_FILE)[unique_column_set].drop_duplicates()
    else:
        return pd.DataFrame(columns=unique_column_set)


def save_raw_html(html_content, file_name: str, split: bool = False):
    """
    Save the raw HTML content from a scraped page.
    """
    directory = RAW_SPLIT_DIR if split else RAW_ROUND_DIR
    with open(directory + file_name + '.html', 'wb') as f:
        f.write(html_content)
        logging.debug(f'Saved file {f.name}')


def save_parsed_data(df: pd.DataFrame, file_path: str):
    """
    Save df to requested file_path, appending if the file already exists.
    """
    if exists(file_path):
        df.to_csv(file_path, mode='a', header=False, index=False)
        logging.debug(f'Appended to {file_path}.')
    else:
        df.to_csv(file_path, index=False)
        logging.debug(f'Created {file_path}.')


def clean_event_title(event_title):
    """
    Map the event title to a standardized format. If not found, log a warning and return the original.
    """
    try:
        return EVENT_NAME_MAPPING[event_title]
    except KeyError:
        logging.warning(f'{event_title} is not a known event title. Skipping cleaning.')
        return event_title


def regex_replace(s: str, regex_str: str = None, replacement_chars: str = ''):
    regex_str = REGEX_BAD_CHARS if regex_str is None else regex_str
    return re.sub(regex_str, replacement_chars, s)


def parse_time_string(s):
    """
    If fractions of a second are present in a time string, transform it into a Timedelta object.
    """
    if '.' in str(s):
        m = re.match(r'(?P<days>[-\d]+) day[s]*, (?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d[\.\d+]*)', str(s))
        if m is not None:
            return pd.Timedelta(**{key: float(val) for key, val in m.groupdict().iteritems()})
    return s


def detect_event_multiple(event_name):
    """
    Check if the title of the event indicates that the event was raced multiple times in the same competition.
    """
    if '(1)' in event_name:
        return 1
    elif '(2)' in event_name:
        return 2
    return -1


def treatable_event(event_title) -> bool:
    """
    Filter out events which are not treatable.
    """
    if event_title in UNTREATABLE_EVENTS:
        return False
    elif "FINAL" in event_title:
        return False
    elif "TeamChampionship" in event_title:
        return False
    return True
