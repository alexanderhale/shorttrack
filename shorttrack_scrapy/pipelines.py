# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from os import replace
from os.path import exists

import numpy as np
import pandas as pd
from tqdm import tqdm

from shorttrack_scrapy.constants import ROUNDS_SPLITS_FILE, ROUNDS_FILE, SPLITS_FILE, LAPTIMES_FILE, \
    PREVIOUS_LAPTIMES_FILE, UNIQUE_RACE_COLUMNS, HALF_LAP_EVENTS, LONGEST_EVENT_LAPS, LIGHT_ATHLETE_NAMES, \
    ROUNDS_SPLITS_LIGHT_FILE, LAPTIMES_LIGHT_FILE, COMPRESSED_LAPTIMES_FILE, PREVIOUS_COMPRESSED_LAPTIMES_FILE
from shorttrack_scrapy.utils import save_parsed_data


class ShorttrackScrapyPipeline(object):
    def process_item(self, item, spider):
        return item

    def close_spider(self, spider):
        rounds_splits_df = self.combine_rounds_splits()
        self.generate_laptimes(rounds_splits_df)
        self.generate_light(rounds_splits_df)

    def combine_rounds_splits(self):
        """
        Combine the round and split data into one DataFrame. Also use the laptime data to extract the positions
        gained/lost each lap.
        """
        # load in the scraped data
        all_rounds = pd.read_csv(ROUNDS_FILE)
        all_splits = pd.read_csv(SPLITS_FILE)

        # separate each race into its own group
        individual_races = all_rounds.groupby(UNIQUE_RACE_COLUMNS)
        rounds_splits_df = all_rounds.copy()

        for race_details, athlete_race_data in tqdm(individual_races):
            athlete_indices = athlete_race_data.index

            # find the laptime data corresponding to this race
            laps = all_splits[(all_splits['season'] == race_details[0]) &
                              (all_splits['competition'] == race_details[1]) &
                              (all_splits['event'] == race_details[2]) &
                              (all_splits['instance_of_event_in_competition'] == race_details[3]) &
                              (all_splits['gender'] == race_details[4]) &
                              (all_splits['round'] == race_details[5]) &
                              (all_splits['race'] == race_details[6])]

            # indicate how many laps' worth of split data were found for this race
            rounds_splits_df.loc[athlete_indices, 'laps_of_split_data'] = laps.shape[0]

            # append lap data columns to the race data
            if laps.shape[0] > 1:
                for athlete_index in athlete_indices:
                    athlete_start_position = f'START_POS_{rounds_splits_df.loc[athlete_index, "Start Pos."]}'

                    if f'{athlete_start_position} POSITION' in laps.columns:
                        for lap_number, (lap_index, lap_data) in enumerate(laps.iterrows()):
                            rounds_splits_df.loc[athlete_index, f'lap_{lap_number + 1}_position'] = lap_data[
                                f'{athlete_start_position} POSITION'] if lap_data[
                                f'{athlete_start_position} POSITION'] else np.nan
                            rounds_splits_df.loc[athlete_index, f'lap_{lap_number + 1}_laptime'] = lap_data[
                                f'{athlete_start_position} LAP TIME']
                            rounds_splits_df.loc[athlete_index, f'lap_{lap_number + 1}_elapsedtime'] = lap_data[
                                f'{athlete_start_position} ELAPSED TIME']

        # replace zeros with NaNs
        pos_cols = [f'lap_{x}_position' for x in range(1, 46)]
        laptime_cols = [f'lap_{x}_laptime' for x in range(1, 46)]
        rounds_splits_df[pos_cols] = rounds_splits_df[pos_cols].replace(0.0, np.nan)
        rounds_splits_df[laptime_cols] = rounds_splits_df[laptime_cols].replace(0.0, np.nan)

        # save to CSV for loading in dashboard
        rounds_splits_df.to_csv(ROUNDS_SPLITS_FILE, index=False)
        return rounds_splits_df

    def generate_laptimes(self, rounds_splits_df: pd.DataFrame):
        """
        Extract positions gained/lost from laptime data.
        """
        # make a backup of existing laptime data
        if exists(LAPTIMES_FILE):
            replace(LAPTIMES_FILE, PREVIOUS_LAPTIMES_FILE)

        race_details_cols = list(rounds_splits_df.columns[:17])
        lap_details_cols = race_details_cols.copy()
        lap_details_cols.extend(['lap', 'laptime', 'lap_start_position', 'lap_end_position', 'position_change'])

        for index, athlete_race in tqdm(rounds_splits_df.iterrows()):
            laptimes = pd.DataFrame(columns=lap_details_cols)

            # detect if the event starts with a half-lap
            start_lap = 2 if athlete_race['event'] in HALF_LAP_EVENTS else 1

            for i in range(start_lap, LONGEST_EVENT_LAPS + 1):
                try:
                    laptime = float(athlete_race[f'lap_{i}_laptime'])
                except Exception:
                    laptime = np.nan

                # TODO use standard deviation to filter out erroneous laptimes instead of the 7.8 threshold
                if not np.isnan(laptime) and laptime > 7.8:
                    # if we have laptime data for lap i, append it to the list
                    lap_details = athlete_race[race_details_cols]
                    lap_details['lap'] = i
                    lap_details['laptime'] = laptime
                    lap_details['lap_start_position'] = float(athlete_race[f'lap_{i - 1}_position']) if i > 1 else float(athlete_race['Start Pos.'])
                    lap_details['lap_end_position'] = float(athlete_race[f'lap_{i}_position'])
                    lap_details['position_change'] = (-1) * (lap_details['lap_end_position'] - lap_details['lap_start_position'])

                    laptimes = laptimes.append(lap_details)

            save_parsed_data(df=laptimes, file_path=LAPTIMES_FILE)

    def generate_light(self, rounds_splits_df: pd.DataFrame):
        """
        Generate the "light" version of the dataset for use on the demo server. Also create a compressed Pickle file
        of the full laptimes dataset.
        """
        light_rounds_splits_df = rounds_splits_df[rounds_splits_df["Name"].isin(LIGHT_ATHLETE_NAMES)]
        light_rounds_splits_df.to_csv(ROUNDS_SPLITS_LIGHT_FILE, index=False)

        laptimes_df = pd.read_csv(LAPTIMES_FILE)
        light_laptimes_df = laptimes_df[laptimes_df["Name"].isin(LIGHT_ATHLETE_NAMES)]
        light_laptimes_df.to_csv(LAPTIMES_LIGHT_FILE)

        if exists(COMPRESSED_LAPTIMES_FILE):
            replace(COMPRESSED_LAPTIMES_FILE, PREVIOUS_COMPRESSED_LAPTIMES_FILE)
        laptimes_df.to_pickle(COMPRESSED_LAPTIMES_FILE, compression='zip')
