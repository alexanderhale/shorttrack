# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import numpy as np
import pandas as pd
from tqdm import tqdm

from shorttrack_scrapy.spiders.short_track_spider import ShortTrackEventSpider


class ShorttrackScrapyPipeline(object):
    def process_item(self, item, spider: ShortTrackEventSpider):
        return item

    def close_spider(self, spider: ShortTrackEventSpider):
        rounds_splits_df = self.combine_rounds_splits(spider)
        self.generate_laptimes(spider, rounds_splits_df)

    def combine_rounds_splits(self, spider: ShortTrackEventSpider):
        """
        Combine the round and split data into one DataFrame.
        """
        all_rounds = pd.read_csv(spider.full_round_file_name)
        all_splits = pd.read_csv(spider.full_split_file_name)
        individual_races = all_rounds.groupby(
            ['season', 'competition', 'event', 'gender', 'round', 'race', 'instance_of_event_in_competition'])
        rounds_splits_df = all_rounds.copy()

        for race_details, athlete_race_data in tqdm(individual_races):
            athlete_indices = athlete_race_data.index

            laps = all_splits[(all_splits['season'] == race_details[0]) &
                              (all_splits['competition'] == race_details[1]) &
                              (all_splits['event'] == race_details[2]) &
                              (all_splits['gender'] == race_details[3]) &
                              (all_splits['round'] == race_details[4]) &
                              (all_splits['race'] == race_details[5]) &
                              (all_splits['instance_of_event_in_competition'] == race_details[6])]

            rounds_splits_df.loc[athlete_indices, 'laps_of_split_data'] = laps.shape[0]

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
        rounds_splits_df.to_csv(spider.rounds_splits_file_name, index=False)
        return rounds_splits_df

    def generate_laptimes(self, spider: ShortTrackEventSpider, rounds_splits_df: pd.DataFrame):
        """
        Extract positions gained/lost from laptime data.
        """
        individual_events = rounds_splits_df[rounds_splits_df['event'].isin({'500m', '1000m', '1500m'})]

        race_details_cols = list(individual_events.columns[:16]) + ['instance_of_event_in_competition']
        lap_details_cols = race_details_cols.copy()
        lap_details_cols.extend(['lap', 'laptime', 'lap_start_position', 'lap_end_position', 'position_change'])

        for index, athlete_race in tqdm(individual_events.iterrows()):
            laptimes = pd.DataFrame(columns=lap_details_cols)
            start_lap = 2 if athlete_race['event'] in ['500m', '1500m'] else 1
            for i in range(start_lap, 46):
                try:
                    laptime = float(athlete_race[f'lap_{i}_laptime'])
                except Exception:
                    laptime = np.nan

                # TODO use standard deviation to filter out erroneous laptimes instead of the 7.8 threshold
                if not np.isnan(laptime) and laptime > 7.8:
                    lap_details = athlete_race[race_details_cols]
                    lap_details['lap'] = i
                    lap_details['laptime'] = laptime
                    lap_details['lap_start_position'] = float(athlete_race[f'lap_{i - 1}_position']) if i > 1 else float(athlete_race['Start Pos.'])
                    lap_details['lap_end_position'] = float(athlete_race[f'lap_{i}_position'])
                    lap_details['position_change'] = (-1) * (lap_details['lap_end_position'] - lap_details['lap_start_position'])

                    laptimes = laptimes.append(lap_details)

            laptimes['lap'] = laptimes['lap'].astype('int')

            spider.save_parsed_data(df=laptimes, file_path=spider.laptimes_file_name)
