import re
from os import listdir

import pandas as pd
import scrapy
from urllib.parse import urlsplit, parse_qs

class ShortTrackEventSpider(scrapy.Spider):
    name = "shorttrackevent"
    start_urls = {
                    # 2018_2019
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000058&gen=w&ref=19185&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000059&gen=w&ref=19332&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000060&gen=w&ref=19411&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000061&gen=w&ref=19636&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000062&gen=w&ref=19735&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000064&gen=w&ref=19495&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000066&gen=w&ref=19830&view=rou',

                    # 2019_2020
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000067&gen=w&ref=63952&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000068&gen=w&ref=64046&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000068&gen=w&ref=64046&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000069&gen=w&ref=63952&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000070&gen=w&ref=64046&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000071&gen=w&ref=64046&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000072&gen=w&ref=63952&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000073&gen=w&ref=64046&view=rou',
                    'https://shorttrack.sportresult.com/Results.aspx?evt=11213100000074&gen=w&ref=64046&view=rou'
                 }
    bad_chars = '\s+'
    full_dir = 'data/scraped/raw/event/'
    parsed_dir = 'data/scraped/parsed/event/'

    def start_requests(self):
        
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # extract event details for categorization purposes
        event_name = response.css('span.eventhead::text').get()
        gender = response.url.split("&")[-3].split("=")[-1]
        distance = response.css('div.navilevel1.naviexpanded1 p a::text').get()
        stage = response.css('div.navilevel3.naviselected3 p a::text').get()

        # save full HTML content
        filename = f'{event_name}-{distance}-{gender}-{stage}'
        filename_details = re.sub("/", '_', re.sub(self.bad_chars, '', filename))
        full_filename = self.full_dir + filename_details + '.html'
        with open(full_filename, 'wb') as f:
            f.write(response.body)
        
        # extract and save timing data from this page
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
                        athlete_out["ISU ID"] = parse_qs(urlsplit(data_point.css('a::attr(href)').get()).query)["ath"][0]
                    elif col != "\xa0":
                        athlete_out[col] = re.sub(self.bad_chars, '', data_point.css('td::text').get())
                races_out.append(athlete_out)

        pd.DataFrame(races_out).to_csv(self.parsed_dir + filename_details + ".csv", index=False)

        # create a request to ShortTrackSplitSpider to extract the splits for the races on this page
        split_urls = response.css('div.tabletitle p a::attr(href)').getall()
        # TODO

        # gather URLs which have been scraped already
        already_scraped = listdir(self.full_dir)

        # find other results-containing URLs and explore them
        other_pages_to_crawl = response.css('div.navilevel1 p a::attr(href)').getall() + response.css('div.navilevel3 p a::attr(href)').getall()
        for other_page in other_pages_to_crawl:
            other_page_path = response.urljoin(other_page)
            if "Results.aspx" in other_page_path and other_page_path not in already_scraped:
                self.log(f'Adding {other_page_path} to crawl list.')
                yield scrapy.Request(url=other_page_path, callback=self.parse)

        self.log(f'Saved file {full_filename}')


class ShortTrackSplitSpider(scrapy.Spider):
    name = "shorttracksplit"
    bad_chars = '\s+'
    full_dir = 'data/scraped/raw/split/'
    parsed_dir = 'data/scraped/parsed/split/'

    def start_requests(self):
        pass

    def parse(self, response):
        # TODO
        pass
