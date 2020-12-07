# Short Track Data Analysis
This repository extracts historical short track speed skating data and analyzes it to reveal trends.

## Data
All data is scraped from the International Skating Union (ISU) results website: https://shorttrack.sportresult.com/. The most interesting dataset is [rounds_splits.csv](./data/scraped/cleaned/rounds_splits.csv), which contains a row for every athlete in every ISU race between 1994 and 2020. Information about the athlete (or relay team) is available, including name, nationality, gender, starting position, finishing position, and finishing time. Where available, the athlete's lap split times and position at the end of each lap is also listed.

#### Data Terms of Use
The ISU's [terms of use](https://www.isu.org/quick-links-sep/legal-information) forbid the "permanent copying or storage" of their data. Whether storage on GitHub constitutes "permanence" is unclear - I will happily take down the data portion of this repository upon request.

## Trends
No trends have been extracted from the data yet. Here are some ideas:
* Is a particular athlete more likely to start at the back or front of a pack for a longer-distance race?
* What is the most frequent lap that the winner of a race makes their pass to the front?
* Is there a pattern of positions within the pack that the winner often follows?

