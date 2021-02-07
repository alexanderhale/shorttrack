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
        self.combine_rounds_splits(spider)

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

            rounds_splits_df.to_csv(spider.rounds_splits_file_name, index=False)
