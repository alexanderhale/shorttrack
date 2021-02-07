import re
from os.path import exists

import numpy as np
import pandas as pd
import scrapy
from urllib.parse import urlsplit, parse_qs, urlparse

NAME = "shorttrack"

EVENT_NAME_MAPPING = {
    "500M": "500m",
    "500m(1)": "500m",
    "500m(2)": "500m",
    "1000M": "1000m",
    "1000m(1)": "1000m",
    "1000m(2)": "1000m",
    "1500M": "1500m",
    "1500m(1)": "1500m",
    "1500m(2)": "1500m",
    "1,000m": "1000m",
    "3000M": "3000m",
    "1,500m": "1500m",
    "3000MRelay": "3000mRelay",
    "5000M": "5000m",
    "5000MRelay": "5000mRelay",
    "3,000mRelay": "3000mRelay",
    "5,000mRelay": "5000mRelay",
    "2000MRelay": "2000mRelay"
}


class ShortTrackEventSpider(scrapy.Spider):

    def __init__(self):
        super().__init__(name=NAME)
        self.start_url = "https://shorttrack.sportresult.com"
        self.bad_chars = '\s+'
        self.raw_dir = 'data/archive/raw/round/'
        self.raw_split_dir = 'data/archive/raw/split/'
        self.save_html = False
        self.full_round_file_name = 'data/archive/all_rounds_merged.csv'
        self.full_split_file_name = 'data/archive/all_splits_merged.csv'
        self.rounds_splits_file_name = 'data/full/all_splits_merged.csv'
        self.laptimes_file_name = 'data/full/laptimes.csv'

        unique_column_set = ['season', 'competition', 'event', 'gender', 'round', 'instance_of_event_in_competition']
        self.already_scraped = self.init_already_scraped(unique_column_set)

    def init_already_scraped(self, unique_column_set: list) -> pd.DataFrame:
        if exists(self.full_round_file_name):
            return pd.read_csv(self.full_round_file_name)[unique_column_set].drop_duplicates()
        else:
            return pd.DataFrame(columns=unique_column_set)

    def check_already_scraped(self, season_title, competition_title, event_title, event_gender, round_title,
                              instance_of_event_in_competition) -> bool:
        """
        Return True if the queried round has already been scraped; False otherwise.
        """
        if len(self.already_scraped[[(self.already_scraped['season'] == season_title) &
                                     (self.already_scraped['competition'] == competition_title) &
                                     (self.already_scraped['event'] == event_title) &
                                     (self.already_scraped['gender'] == event_gender) &
                                     (self.already_scraped['round'] == round_title) &
                                     (self.already_scraped['instance_of_event_in_competition'] ==
                                      instance_of_event_in_competition)]]):
            return True
        return False

    def start_requests(self):
        yield scrapy.Request(url=self.start_url, callback=self.parse)

    def parse(self, response):
        """
        Gather list of season IDs and call them individually.
        """
        season_numbers = response.css('select[name="sea"] option::attr(value)').getall()
        season_titles = response.css('select[name="sea"] option::text').getall()
        for season_number, season_title in zip(season_numbers, season_titles):
            season_url = response.url + "/?sea=" + season_number
            yield scrapy.Request(url=season_url,
                                 callback=self.parse_season,
                                 meta=dict(season_id=season_number,
                                           season_title=season_title.split(" ")[0]))

    def parse_season(self, response):
        """
        Gather list of competitions in the season and call them individually.

        Example season: "2019-2020 SEASON"
        """
        competition_numbers = response.css('select[name="evt"] optgroup option::attr(value)').getall()
        competition_titles = response.css('select[name="evt"] optgroup option::text').getall()
        url_components = urlparse(response.url)
        for competition_number, competition_title in zip(competition_numbers, competition_titles):
            competition_url = f'{url_components.scheme}://{url_components.netloc}/Results.aspx?evt={competition_number}'
            response.meta.update(dict(competition_title=competition_title,
                                      competition_id=competition_number))
            yield scrapy.Request(url=competition_url,
                                 callback=self.parse_competition,
                                 meta=response.meta)

    def parse_competition(self, response):
        """
        Gather list of events in the competition and call them individually.

        Example competition: "ISU World Cup 2019/20 - Dordrecht (NED)"
        """
        event_urls = response.css('div.navilevel1 p a::attr(href)').getall()
        event_titles = response.css('div.navilevel1 p a::text').getall()

        for event_url, event_title in zip(event_urls, event_titles):
            full_event_url = response.urljoin(event_url)
            event_details = parse_qs(urlsplit(full_event_url).query)
            response.meta.update(dict(event_title=self.regex_replace(EVENT_NAME_MAPPING.get(event_title, event_title)),
                                      event_gender=event_details.get("gen", [""])[0]))
            response.meta.update(dict(instance_of_event_in_competition=self.detect_event_multiple(
                response.meta['event_title'])))
            yield scrapy.Request(url=full_event_url,
                                 callback=self.parse_event,
                                 meta=response.meta)

    def parse_event(self, response):
        """
        Gather list of rounds in the event and call them individually.

        Example event: "1500 m"
        """
        round_urls = response.css('div.navilevel3 p a::attr(href)').getall()
        round_titles = response.css('div.navilevel3 p a::text').getall()

        for round_url, round_title in zip(round_urls, round_titles):
            if not self.check_already_scraped(season_title=response.meta['season_title'],
                                              competition_title=response.meta['competition_title'],
                                              event_title=response.meta['event_title'],
                                              event_gender=response.meta['event_gender'],
                                              round_title=response.meta['round_title'],
                                              instance_of_event_in_competition=response.meta[
                                                  'instance_of_event_in_competition']):
                full_round_url = response.urljoin(round_url)
                round_details = parse_qs(urlsplit(full_round_url).query)
                response.meta.update(dict(round_title=self.regex_replace(round_title),
                                          round_id=round_details.get("ref", "")))
                yield scrapy.Request(url=full_round_url,
                                     callback=self.parse_round,
                                     meta=response.meta)

    def parse_round(self, response):
        """
        Gather athlete data and basic timing/position data for each race in the round.

        Example round: "Semifinals". Each round has multiple races (e.g. the Semifinal round most commonly has 2 races).
        """
        # create file name
        unclean_file_name = f'{response.meta["season_title"]}-{response.meta["competition_title"]}-' \
                            f'{response.meta["event_title"]}-{response.meta["event_gender"]}-' \
                            f'{response.meta["round_title"]}'
        response.meta.update(dict(round_file_name=self.regex_replace(self.regex_replace(unclean_file_name), '/', '_')))

        # save full HTML content
        if self.save_html:
            self.save_raw_html(html_content=response.body, file_name=response.meta["round_file_name"])

        # extract athlete data and basic timing/position data for each race of the round
        races = response.css('table[cellspacing="0"][align="Center"]')
        races_out = list()
        for i, race in enumerate(races):
            column_headers = race.css('tr.tablehead th::text').getall()
            athletes = race.css('tr[class*=tablecol]')
            for athlete in athletes:
                athlete_out = dict(season=response.meta["season_title"],
                                   competition=response.meta["competition_title"],
                                   event=response.meta["event_title"],
                                   gender=response.meta["event_gender"],
                                   round=response.meta["round_title"],
                                   race=i + 1,
                                   instance_of_event_in_competition=response.meta['instance_of_event_in_competition'])
                for col, data_point in zip(column_headers, athlete.css('td')):
                    if col == "Name":
                        athlete_out[col] = self.regex_replace(data_point.css('td a::text').get())
                        athlete_out["ISU ID"] = parse_qs(urlsplit(data_point.css('a::attr(href)').get()).query).get(
                            "ath", [""])[0]
                    elif col not in ["\xa0", "Relay Team"]:
                        athlete_out[col] = self.regex_replace(data_point.css('td::text').get())

                    if col == "Results":
                        athlete_out[col] = self.parse_time_string(athlete_out[col])
                races_out.append(athlete_out)

        self.save_parsed_data(df=pd.DataFrame(races_out), file_path=self.full_round_file_name)

        # call the dedicated parser to extract split data for each race of the round
        split_urls = response.css('div.tabletitle p a[href*="http://shorttrack.sportresult.com"]::attr(href)').getall()
        for split_url in split_urls:
            split_path = response.urljoin(split_url)
            response.meta.update(dict(race_number=parse_qs(urlsplit(split_path).query).get("rac", [""])[0]))
            yield scrapy.Request(url=split_path,
                                 callback=self.parse_split,
                                 meta=response.meta)

    def parse_split(self, response):
        """
        Gather split data for the race.
        """
        # create file name
        race_file_name = f'{response.meta["round_file_name"]}-race_{response.meta["race_number"]}-splits'

        # save full HTML content
        if self.save_html:
            self.save_raw_html(html_content=response.body, file_name=race_file_name, split=True)

        # extract split times and positions for each athlete on each lap
        athlete_names = response.css('tr.tablehead th[scope="col"]::text')[1:].getall()
        laps = response.css('tr[class*=tablecol]')

        split_data = dict()
        col_ids = list()
        for start_position in range(1, len(athlete_names) + 1):
            col_id = f'START_POS_{str(start_position)}'
            col_ids.append(col_id)
            split_data[f'{col_id} POSITION'] = list()
            split_data[f'{col_id} LAP TIME'] = list()
            split_data[f'{col_id} ELAPSED TIME'] = list()

        num_laps = len(laps)
        split_data["season"] = [response.meta["season_title"]] * num_laps
        split_data["competition"] = [response.meta["competition_title"]] * num_laps
        split_data["event"] = [response.meta["event_title"]] * num_laps
        split_data["gender"] = [response.meta["event_gender"]] * num_laps
        split_data["round"] = [response.meta["round_title"]] * num_laps
        split_data["race"] = [response.meta["race_number"]] * num_laps

        for lap in laps:
            for athlete_col, col_id in zip(lap.css('td')[1:], col_ids):
                athlete_position = athlete_col.css('td span::text').get()
                athlete_position_cleaned = athlete_position.strip('[]') if athlete_position is not None else np.nan
                split_data[f'{col_id} POSITION'].append(athlete_position_cleaned)

                laptime_field = athlete_col.css('td::text').getall()
                if len(laptime_field):
                    both_times = self.regex_replace(laptime_field[1]).strip(')').split('(')
                else:
                    both_times = [np.nan, np.nan]
                split_data[f'{col_id} LAP TIME'].append(self.parse_time_string(both_times[1]))
                split_data[f'{col_id} ELAPSED TIME'].append(self.parse_time_string(both_times[0]))

        self.save_parsed_data(df=pd.DataFrame(split_data), file_path=self.full_split_file_name)

    def save_raw_html(self, html_content, file_name: str, split: bool = False):
        """
        Save the raw HTML content from a scraped page.
        """
        directory = self.raw_split_dir if split else self.raw_dir
        with open(directory + file_name + '.html', 'wb') as f:
            f.write(html_content)
            self.log(message=f'Saved file {f.name}')

    def save_parsed_data(self, df: pd.DataFrame, file_path: str):
        """
        Save df to requested file_path, appending if the file already exists.
        """
        if exists(file_path):
            df.to_csv(file_path, mode='a', header=False, index=False)
            self.log(message=f'Appended to {file_path}.')
        else:
            df.to_csv(file_path, index=False)
            self.log(message=f'Created {file_path}.')

    def regex_replace(self, s: str, regex_str: str = None, replacement_chars: str = ''):
        regex_str = self.bad_chars if regex_str is None else regex_str
        return re.sub(regex_str, replacement_chars, s)

    def parse_time_string(self, s):
        """
        If fractions of a second are present in a time string, transform it into a Timedelta object.
        """
        if '.' in str(s):
            m = re.match(r'(?P<days>[-\d]+) day[s]*, (?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d[\.\d+]*)', str(s))
            if m is not None:
                return pd.Timedelta(**{key: float(val) for key, val in m.groupdict().iteritems()})
        return s

    def detect_event_multiple(self, event_name):
        """
        Check if the title of the event indicates that the event was raced multiple times in the same competition.
        """
        if '(1)' in event_name:
            return 1
        elif '(2)' in event_name:
            return 2
        return -1
