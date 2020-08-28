import re

import numpy as np
import pandas as pd
import scrapy
from urllib.parse import urlsplit, parse_qs, urlparse


class ShortTrackEventSpider(scrapy.Spider):
    name = "shorttrack"
    start_url = "https://shorttrack.sportresult.com"
    bad_chars = '\s+'
    raw_dir = 'data/scraped/raw/round/'
    raw_split_dir = 'data/scraped/raw/split/'
    parsed_dir = 'data/scraped/parsed/round/'
    parsed_split_dir = 'data/scraped/parsed/split/'

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
        """
        event_urls = response.css('div.navilevel1 p a::attr(href)').getall()
        event_titles = response.css('div.navilevel1 p a::text').getall()

        for event_url, event_title in zip(event_urls, event_titles):
            full_event_url = response.urljoin(event_url)
            event_details = parse_qs(urlsplit(full_event_url).query)
            response.meta.update(dict(event_title=event_title,
                                      event_gender=event_details.get("gen", [""])[0]))
            yield scrapy.Request(url=full_event_url,
                                 callback=self.parse_event,
                                 meta=response.meta)

    def parse_event(self, response):
        """
        Gather list of rounds in the event and call them individually.
        """
        round_urls = response.css('div.navilevel3 p a::attr(href)').getall()
        round_titles = response.css('div.navilevel3 p a::text').getall()

        for round_url, round_title in zip(round_urls, round_titles):
            full_round_url = response.urljoin(round_url)
            round_details = parse_qs(urlsplit(full_round_url).query)
            response.meta.update(dict(round_title=round_title,
                                      round_id=round_details.get("ref", "")))
            yield scrapy.Request(url=full_round_url,
                                 callback=self.parse_round,
                                 meta=response.meta)

    def parse_round(self, response):
        """
        Gather athlete data and basic timing/position data for each race in the round.
        """
        # create file name
        unclean_file_name = f'{response.meta["season_title"]}-{response.meta["competition_title"]}-' \
                            f'{response.meta["event_title"]}-{response.meta["event_gender"]}-' \
                            f'{response.meta["round_title"]}'
        response.meta.update(dict(round_file_name=re.sub("/", '_', re.sub(self.bad_chars, '', unclean_file_name))))

        # save full HTML content
        self.save_raw_html(html_content=response.body, file_name=response.meta["round_file_name"])

        # extract athlete data and basic timing/position data for each race of the round
        races = response.css('table[cellspacing="0"][align="Center"]')
        races_out = list()
        for i, race in enumerate(races):
            column_headers = race.css('tr.tablehead th::text').getall()
            athletes = race.css('tr[class*=tablecol]')
            for athlete in athletes:
                athlete_out = dict(race=i)
                for col, data_point in zip(column_headers, athlete.css('td')):
                    if col == "Name":
                        athlete_out[col] = re.sub(self.bad_chars, '', data_point.css('td a::text').get())
                        athlete_out["ISU ID"] = parse_qs(urlsplit(data_point.css('a::attr(href)').get()).query).get(
                            "ath", [""])[0]
                    elif col != "\xa0":
                        athlete_out[col] = re.sub(self.bad_chars, '', data_point.css('td::text').get())
                races_out.append(athlete_out)

        self.save_parsed_data(df=pd.DataFrame(races_out), file_name=response.meta["round_file_name"])

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
        self.save_raw_html(html_content=response.body, file_name=race_file_name, split=True)

        # extract split times and positions for each athlete on each lap
        athlete_names = response.css('tr.tablehead th[scope="col"]::text')[1:].getall()
        laps = response.css('tr[class*=tablecol]')

        split_data = dict()
        for athlete_name in athlete_names:
            split_data[f'{athlete_name} POSITION'] = list()
            split_data[f'{athlete_name} LAP TIME'] = list()
            split_data[f'{athlete_name} ELAPSED TIME'] = list()

        for lap in laps:
            for athlete_col, athlete_name in zip(lap.css('td')[1:], athlete_names):
                athlete_position = athlete_col.css('td span::text').get()
                athlete_position_cleaned = athlete_position.strip('[]') if athlete_position is not None else np.nan
                split_data[f'{athlete_name} POSITION'].append(athlete_position_cleaned)
                both_times = re.sub(self.bad_chars, '', athlete_col.css('td::text').getall()[1]).strip(')').split('(')
                split_data[f'{athlete_name} LAP TIME'].append(both_times[1])
                split_data[f'{athlete_name} ELAPSED TIME'].append(both_times[0])

        self.save_parsed_data(df=pd.DataFrame(split_data), file_name=race_file_name, split=True)

    def save_raw_html(self, html_content, file_name, split=False):
        directory = self.raw_split_dir if split else self.raw_dir
        with open(directory + file_name + '.html', 'wb') as f:
            f.write(html_content)
            self.log(message=f'Saved file {f.name}')

    def save_parsed_data(self, df: pd.DataFrame, file_name: str, split=False):
        directory = self.parsed_split_dir if split else self.parsed_dir
        full_file_name = directory + file_name + ".csv"
        df.to_csv(full_file_name, index=False)
        self.log(message=f'Saved file {full_file_name}')
