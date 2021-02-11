from logging import INFO

import numpy as np
import pandas as pd
import scrapy
from urllib.parse import urlsplit, parse_qs, urlparse

from shorttrack_scrapy.constants import UNIQUE_ROUND_COLUMNS, ROUNDS_FILE, SPLITS_FILE, MAX_ATHLETES_IN_RACE
from shorttrack_scrapy.utils import load_already_scraped, regex_replace, detect_event_multiple, clean_event_title, \
    save_raw_html, parse_time_string, save_parsed_data, treatable_event


class ShortTrackEventSpider(scrapy.Spider):
    name = "shorttrack_spider"

    def __init__(self):
        super().__init__(name=self.name)
        self.start_url = "https://shorttrack.sportresult.com"
        self.save_html = False
        self.already_scraped = load_already_scraped(UNIQUE_ROUND_COLUMNS)

    def check_already_scraped(self, season_title, competition_title, event_title, event_gender, round_title,
                              instance_of_event_in_competition) -> bool:
        """
        Return True if the queried round has already been scraped; False otherwise.
        """
        if len(self.already_scraped[(self.already_scraped['season'] == season_title) &
                                    (self.already_scraped['competition'] == competition_title) &
                                    (self.already_scraped['event'] == event_title) &
                                    (self.already_scraped['instance_of_event_in_competition'] ==
                                     instance_of_event_in_competition) &
                                    (self.already_scraped['gender'] == event_gender) &
                                    (self.already_scraped['round'] == round_title)]):
            self.log(message=f"Round already discovered: {season_title}-{competition_title}-"
                             f"{event_title}-{instance_of_event_in_competition}"
                             f"{event_gender}-{round_title}",
                     level=INFO)
            return True
        self.log(message=f"New round discovered: {season_title}-{competition_title}-"
                         f"{event_title}-{instance_of_event_in_competition}"
                         f"{event_gender}-{round_title}",
                 level=INFO)
        return False

    def start_requests(self):
        yield scrapy.Request(url=self.start_url, callback=self.parse)

    def parse(self, response):
        """
        Gather list of season IDs and call them individually.
        """
        # gather season numbers and titles from the page
        season_numbers = response.css('select[name="sea"] option::attr(value)').getall()
        season_titles = response.css('select[name="sea"] option::text').getall()

        for season_number, season_title in zip(season_numbers, season_titles):
            # assemble URL for this season
            season_url = response.url + "/?sea=" + season_number

            # scrape the season page
            yield scrapy.Request(url=season_url,
                                 callback=self.parse_season,
                                 meta=dict(season_id=season_number,
                                           season_title=season_title.split(" ")[0]))

    def parse_season(self, response):
        """
        Gather list of competitions in the season and call them individually.

        Example season: "2019-2020 SEASON"
        """
        # gather competition numbers and titles from the page
        competition_numbers = response.css('select[name="evt"] optgroup option::attr(value)').getall()
        competition_titles = response.css('select[name="evt"] optgroup option::text').getall()
        url_components = urlparse(response.url)

        for competition_number, competition_title in zip(competition_numbers, competition_titles):
            # assemble direct URL for this competition
            competition_url = f'{url_components.scheme}://{url_components.netloc}/Results.aspx?evt={competition_number}'

            # pass along metadata for use in next steps
            response.meta.update(dict(competition_title=competition_title,
                                      competition_id=competition_number))

            # scrape the competition page
            yield scrapy.Request(url=competition_url,
                                 callback=self.parse_competition,
                                 meta=response.meta)

    def parse_competition(self, response):
        """
        Gather list of events in the competition and call them individually.

        Example competition: "ISU World Cup 2019/20 - Dordrecht (NED)"
        """
        # gather events from the competition page
        event_urls = response.css('div.navilevel1 p a::attr(href)').getall()
        event_titles = response.css('div.navilevel1 p a::text').getall()

        for event_url, event_title in zip(event_urls, event_titles):
            # assemble direct URL for this event
            full_event_url = response.urljoin(event_url)

            # pass along metadata for use in next steps
            event_details = parse_qs(urlsplit(full_event_url).query)
            event_title = regex_replace(event_title)

            if treatable_event(event_title):
                response.meta.update(dict(instance_of_event_in_competition=detect_event_multiple(event_title),
                                          event_title=clean_event_title(event_title),
                                          event_gender=event_details.get("gen", [np.nan])[0]))

                # scrape the event page
                yield scrapy.Request(url=full_event_url,
                                     callback=self.parse_event,
                                     meta=response.meta)

    def parse_event(self, response):
        """
        Gather list of rounds in the event and call them individually.

        Example event: "1500 m"
        """
        # gather rounds from the event page sidebar
        round_urls = response.css('div.navilevel3 p a::attr(href)').getall()
        round_titles = response.css('div.navilevel3 p a::text').getall()

        for round_url, round_title in zip(round_urls, round_titles):
            # check if this round has already been scraped in an early scraping run
            round_title = regex_replace(round_title)
            if not self.check_already_scraped(season_title=response.meta['season_title'],
                                              competition_title=response.meta['competition_title'],
                                              event_title=response.meta['event_title'],
                                              instance_of_event_in_competition=response.meta[
                                                  'instance_of_event_in_competition'],
                                              event_gender=response.meta['event_gender'],
                                              round_title=round_title):
                # assemble direct URL for the round
                full_round_url = response.urljoin(round_url)

                # pass along metadata for use in next steps
                round_details = parse_qs(urlsplit(full_round_url).query)
                response.meta.update(dict(round_title=round_title,
                                          round_id=round_details.get("ref", np.nan)))

                # scrape the round page
                yield scrapy.Request(url=full_round_url,
                                     callback=self.parse_round,
                                     meta=response.meta)

    def parse_round(self, response):
        """
        Gather athlete data and basic timing/position data for each race in the round.

        Example round: "Semifinals". Each round has multiple races (e.g. the Semifinal round most commonly has 2 races).
        """
        # save full HTML content for this round
        if self.save_html:
            unclean_file_name = f'{response.meta["season_title"]}-{response.meta["competition_title"]}-' \
                                f'{response.meta["event_title"]}-{response.meta["event_gender"]}-' \
                                f'{response.meta["round_title"]}'
            response.meta.update(dict(round_file_name=regex_replace(regex_replace(unclean_file_name), '/', '_')))
            save_raw_html(html_content=response.body, file_name=response.meta["round_file_name"])

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
                                   instance_of_event_in_competition=response.meta['instance_of_event_in_competition'],
                                   gender=response.meta["event_gender"],
                                   round=response.meta["round_title"],
                                   race=i + 1)
                for col, data_point in zip(column_headers, athlete.css('td')):
                    if col == "Name":
                        athlete_out[col] = regex_replace(data_point.css('td a::text').get())
                        athlete_out["ISU ID"] = parse_qs(urlsplit(data_point.css('a::attr(href)').get()).query).get(
                            "ath", [np.nan])[0]
                    elif col == "Results":
                        athlete_out[col] = parse_time_string(data_point.css('td::text').get())
                    elif col == "Relay Team":
                        athlete_out["Warn."] = np.nan
                        athlete_out[col] = regex_replace(data_point.css('td::text').get())
                    elif col == "Warn.":
                        athlete_out[col] = regex_replace(data_point.css('td::text').get())
                        athlete_out["Relay Team"] = np.nan
                    elif col != "\xa0":
                        athlete_out[col] = regex_replace(data_point.css('td::text').get())

                races_out.append(athlete_out)

        save_parsed_data(df=pd.DataFrame(races_out), file_path=ROUNDS_FILE)

        # call the dedicated parser to extract split data for each race of the round
        split_urls = response.css('div.tabletitle p a[href*="http://shorttrack.sportresult.com"]::attr(href)').getall()
        for split_url in split_urls:
            split_path = response.urljoin(split_url)
            response.meta.update(dict(race_number=parse_qs(urlsplit(split_path).query).get("rac", [np.nan])[0]))
            yield scrapy.Request(url=split_path,
                                 callback=self.parse_split,
                                 meta=response.meta)

    def parse_split(self, response):
        """
        Gather split data for the race.
        """
        # save full HTML content for this race's split data
        if self.save_html:
            race_file_name = f'{response.meta["round_file_name"]}-race_{response.meta["race_number"]}-splits'
            save_raw_html(html_content=response.body, file_name=race_file_name, split=True)

        # extract split times and positions for each athlete on each lap
        athlete_names = response.css('tr.tablehead th[scope="col"]::text')[1:].getall()
        laps = response.css('tr[class*=tablecol]')

        num_laps = len(laps)
        split_data = {
            "season": [response.meta["season_title"]] * num_laps,
            "competition": [response.meta["competition_title"]] * num_laps,
            "event": [response.meta["event_title"]] * num_laps,
            "instance_of_event_in_competition": [response.meta["instance_of_event_in_competition"]] * num_laps,
            "gender": [response.meta["event_gender"]] * num_laps,
            "round": [response.meta["round_title"]] * num_laps,
            "race": [response.meta["race_number"]] * num_laps
        }
        col_ids = list()
        for start_position in range(1, MAX_ATHLETES_IN_RACE + 1):
            col_id = f'START_POS_{str(start_position)}'
            if start_position <= len(athlete_names):
                col_ids.append(col_id)
            split_data[f'{col_id} POSITION'] = [np.nan] * num_laps
            split_data[f'{col_id} LAP TIME'] = [np.nan] * num_laps
            split_data[f'{col_id} ELAPSED TIME'] = [np.nan] * num_laps

        for lap_index, lap in enumerate(laps):
            for athlete_col, col_id in zip(lap.css('td')[1:], col_ids):
                athlete_position = athlete_col.css('td span::text').get()
                athlete_position_cleaned = athlete_position.strip('[]') if athlete_position is not None else np.nan
                split_data[f'{col_id} POSITION'][lap_index] = athlete_position_cleaned

                laptime_field = athlete_col.css('td::text').getall()
                if len(laptime_field):
                    both_times = regex_replace(laptime_field[1]).strip(')').split('(')
                else:
                    both_times = [np.nan, np.nan]
                split_data[f'{col_id} LAP TIME'][lap_index] = parse_time_string(both_times[1])
                split_data[f'{col_id} ELAPSED TIME'][lap_index] = parse_time_string(both_times[0])

        save_parsed_data(df=pd.DataFrame(split_data), file_path=SPLITS_FILE)
