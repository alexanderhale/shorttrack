REGEX_BAD_CHARS = '\s+'

EVENT_500M = "500m"
EVENT_1000M = "1000m"
EVENT_1500M = "1500m"
EVENT_3000M = "3000m"
EVENT_5000M = "5000m"
EVENT_2000M_RELAY = "2000mRelay"
EVENT_3000M_RELAY = "3000mRelay"
EVENT_5000M_RELAY = "5000mRelay"

EVENT_NAME_MAPPING = {
    EVENT_500M: EVENT_500M,
    EVENT_1000M: EVENT_1000M,
    EVENT_1500M: EVENT_1500M,
    EVENT_3000M: EVENT_3000M,
    EVENT_5000M: EVENT_5000M,
    EVENT_2000M_RELAY: EVENT_2000M_RELAY,
    EVENT_3000M_RELAY: EVENT_3000M_RELAY,
    EVENT_5000M_RELAY: EVENT_5000M_RELAY,
    "500M": EVENT_500M,
    "500m(1)": EVENT_500M,
    "500m(2)": EVENT_500M,
    "1000M": EVENT_1000M,
    "1000m(1)": EVENT_1000M,
    "1000m(2)": EVENT_1000M,
    "1500M": EVENT_1500M,
    "1500m(1)": EVENT_1500M,
    "1500m(2)": EVENT_1500M,
    "1,000m": EVENT_1000M,
    "3000M": EVENT_3000M,
    "1,500m": EVENT_1500M,
    "1500mSuperfinal": EVENT_1500M,
    "1500MSuperFinal": EVENT_1500M,
    "1500mSuperFinal": EVENT_1500M,
    "1500mSF": EVENT_1500M,
    "3000mSF": EVENT_3000M,
    "3000mSuperfinal": EVENT_3000M,
    "3000mSuperFinal": EVENT_3000M,
    "3000MSuperFinal": EVENT_3000M,
    "3000MRelay": EVENT_3000M_RELAY,
    "5000M": EVENT_5000M,
    "5000MRelay": EVENT_5000M_RELAY,
    "3,000mRelay": EVENT_3000M_RELAY,
    "5,000mRelay": EVENT_5000M_RELAY,
    "2000MRelay": EVENT_2000M_RELAY
}

HALF_LAP_EVENTS = [EVENT_500M, EVENT_1500M, EVENT_5000M, EVENT_5000M_RELAY]
UNTREATABLE_EVENTS = ["TeamClassification", "OverallClassification", "BRACKET#1", "BRACKET#2", "REPECHAGE", "", " "]

DATA_DIR = 'data/'
SCRAPED_DIR = f'{DATA_DIR}scraped/'
FULL_DIR = f'{DATA_DIR}full/'
LIGHT_DIR = f'{DATA_DIR}light/'

RAW_DIR = f'{SCRAPED_DIR}raw/'
RAW_ROUND_DIR = f'{RAW_DIR}round/'
RAW_SPLIT_DIR = f'{RAW_DIR}split/'

ROUNDS_FILE = f'{SCRAPED_DIR}all_rounds.csv'
SPLITS_FILE = f'{SCRAPED_DIR}all_splits.csv'

ROUNDS_SPLITS_FILE = f'{FULL_DIR}rounds_with_splits.csv'
LAPTIMES_FILE = f'{FULL_DIR}individual_athlete_lap_data.csv'
COMPRESSED_LAPTIMES_FILE = f'{FULL_DIR}individual_athlete_lap_data.pk'
PREVIOUS_LAPTIMES_FILE = f'{FULL_DIR}individual_athlete_lap_data_PREVIOUS.csv'
PREVIOUS_COMPRESSED_LAPTIMES_FILE = f'{FULL_DIR}individual_athlete_lap_data_PREVIOUS.pk'

LIGHT_ATHLETE_NAMES = ["FrancoisHAMELIN",
                       "KNEGTSjinkie",
                       "KWAKYoon-Gy",
                       "SHAOANGLiu",
                       "SCHULTINGSuzanne",
                       "AriannaFONTANA",
                       "CHRISTIEElise",
                       "Marie-EveDROLET"]
ROUNDS_SPLITS_LIGHT_FILE = f'{LIGHT_DIR}rounds_with_splits.csv'
LAPTIMES_LIGHT_FILE = f'{LIGHT_DIR}individual_athlete_lap_data.csv'

UNIQUE_ROUND_COLUMNS = ['season', 'competition', 'event', 'instance_of_event_in_competition', 'gender', 'round']
UNIQUE_RACE_COLUMNS = ['season', 'competition', 'event', 'instance_of_event_in_competition', 'gender', 'round', 'race']

MAX_ATHLETES_IN_RACE = 12
LONGEST_EVENT_LAPS = 45
