import pandas as pd
import numpy as np

import seaborn as sns
import matplotlib.pyplot as plt
import panel as pn
import panel.widgets as pnw

# constants
ALL_EVENTS_NAME = 'All'
EVENT_500M = '500m'
EVENT_1000M = '1000m'
EVENT_1500M = '1500m'
DEFAULT_START_POSITION = 1
DEFAULT_POSITION_CHANGE = 1

# data load
full_rounds = pd.read_csv('../data/scraped/cleaned/rounds_splits.csv')
laptimes = pd.read_csv('../data/scraped/cleaned/individual_athlete_lap_data.csv')
pos_cols = [f'lap_{x}_position' for x in range(1, 46)]
laptime_cols = [f'lap_{x}_laptime' for x in range(1, 46)]
full_rounds[pos_cols] = full_rounds[pos_cols].replace(0.0, np.nan)
full_rounds[laptime_cols] = full_rounds[laptime_cols].replace(0.0, np.nan)
individual_events = full_rounds[full_rounds['event'].isin({'500m', '1000m', '1500m'})]


# helper functions
def select_event_subset(df, e):
    """
    Return only the rows of df which belong to the requested event.
    """
    return df[df['event'] == e]


def get_ax():
    fig = plt.Figure()
    ax = fig.add_subplot(111)
    return fig, ax


# declare UI template
ui_template = pn.template.MaterialTemplate(title='Short Track Athlete Profile')
pn.config.sizing_mode = 'stretch_width'

# declare variable widgets
athlete_name = pnw.Select(name='Athlete', options=list(individual_events['Name'].unique()))
event_distance = pnw.RadioButtonGroup(name='Event', value=ALL_EVENTS_NAME)
start_position = pnw.RadioButtonGroup(name='Start Position')
position_gain_loss = pnw.RadioButtonGroup(name='Position Gain/Loss')
athlete_races = pnw.DataFrame()
athlete_races_single_event = pnw.DataFrame()
athlete_laptimes = pnw.DataFrame()
athlete_laptimes_single_event = pnw.DataFrame()


def athlete_name_changed(event):
    """
    Triggering event is athlete_name.value
    """
    athlete_races.value = individual_events[individual_events['Name'] == event.new]
    athlete_laptimes.value = laptimes[laptimes['Name'] == event.new]

    event_distance.options = list(athlete_races.value['event'].unique()) + [ALL_EVENTS_NAME]
    if event_distance.value not in event_distance.options:
        # if the new event distance options don't contain the current event_distance value, default to ALL_EVENTS_NAME
        event_distance.value = ALL_EVENTS_NAME
    else:
        # if there was no change, trigger the widget refresh manually
        event_distance.param.trigger('value')


def event_distance_changed(event):
    """
    Triggering event is event_distance.value
    """
    if event.new == ALL_EVENTS_NAME:
        athlete_races_single_event.value = athlete_races.value
        athlete_laptimes_single_event.value = athlete_laptimes.value
    else:
        athlete_races_single_event.value = athlete_races.value[athlete_races.value['event'] == event.new]
        athlete_laptimes_single_event.value = athlete_laptimes.value[athlete_laptimes.value['event'] == event.new]

    start_position.options = list(athlete_races.value['Start Pos.'].unique())
    position_gain_loss.options = list(athlete_laptimes.value['position_change'].unique())

    if start_position.value not in start_position.options:
        start_position.value = DEFAULT_START_POSITION
    if position_gain_loss.value not in position_gain_loss.options:
        position_gain_loss.value = DEFAULT_POSITION_CHANGE


# declare reloading between widgets
athlete_name.param.watch(athlete_name_changed, 'value')
event_distance.param.watch(event_distance_changed, 'value')

# trigger initial widget dependencies
athlete_name.param.trigger('value')


@pn.depends(athlete_races_single_event)
def first_lap_positions(athlete_races_single_event__):
    """
    The position in the pack that the athlete likes to start this event distance.
    """
    fig, ax = get_ax()
    sns.histplot(data=athlete_races_single_event__,
                 x="lap_1_position",
                 stat="probability",
                 ax=ax,
                 discrete=True).set_title(f'Early Selection of Position in Pack - {event_distance.value}')
    return fig


@pn.depends(athlete_races)
def half_lap_500m_mean(athlete_races__):
    """
    The athlete's average 500m half-lap start time.
    """
    athlete_races_500m = select_event_subset(athlete_races__, EVENT_500M)
    mean_start_time = round(athlete_races_500m['lap_1_laptime'].astype('float').mean(), 3)
    return pn.indicators.Number(name='Mean 500m Half-Lap Start Time',
                                value=mean_start_time,
                                format='{value}s')


@pn.depends(athlete_races)
def half_lap_500m_hist(athlete_races__):
    """
    Histogram of the athlete's 500m half-lap start time (thresholding at 9s to remove outliers).
    """
    athlete_races_500m = select_event_subset(athlete_races__, EVENT_500M)
    athlete_races_500m = athlete_races_500m.astype({'lap_1_laptime': float})

    thresholded_start_times = athlete_races_500m[athlete_races_500m['lap_1_laptime'] < 9]
    fig, ax = get_ax()
    sns.histplot(data=thresholded_start_times,
                 ax=ax,
                 x="lap_1_laptime").set_title('500m Half-Lap Start Times')

    return fig


@pn.depends(start_position, athlete_races)
def start_performance_500m(start_position__, athlete_races__):
    """
    A histogram of the position the athlete is in after the first half-lap of the 500m, for the selected start position.
    """
    athlete_races_500m = athlete_races__[athlete_races__['event'] == EVENT_500M]
    start_performances = athlete_races_500m[athlete_races_500m['Start Pos.'] == int(start_position__)]
    start_performances = start_performances.astype({'lap_1_position': float})

    fig, ax = get_ax()
    sns.histplot(data=start_performances,
                 x="lap_1_position",
                 ax=ax).set_title(f'500m Start Result from Lane {start_position__}')
    return fig


@pn.depends(athlete_laptimes)
def fastest_leading_laptimes(athlete_laptimes__):
    """
    The average of the 25 fastest laptimes achieved by the athlete when leading the race.
    """
    return pn.indicators.Number(name='Fastest Leading Laptimes',
                                value=round(athlete_laptimes__[athlete_laptimes__['lap_end_position'] == 1][
                                                'laptime'].nsmallest(25).mean(), 3),
                                format='{value}s')


@pn.depends(athlete_laptimes)
def fastest_following_laptimes(athlete_laptimes__):
    """
    The average of the 25 fastest laptimes achieved by the athlete when not leading the race.
    """
    return pn.indicators.Number(name='Fastest Leading Laptimes',
                                value=round(athlete_laptimes__[athlete_laptimes__['lap_end_position'] != 1][
                                                'laptime'].nsmallest(25).mean(), 3),
                                format='{value}s')


@pn.depends(athlete_laptimes_single_event, position_gain_loss)
def likely_lap_to_pass(athlete_laptimes_single_event__, position_gain_loss__):
    """
    A histogram of how often an athlete makes a pass (or gets passed) on a particular lap, for the selected number of
    positions gained/lost in the selected event distance.
    """
    selected_passes = athlete_laptimes_single_event__[
        athlete_laptimes_single_event__['position_change'] == position_gain_loss__]

    fig, ax = get_ax()
    sns.histplot(data=selected_passes,
                 x="lap",
                 ax=ax).set_title(f'Passes on each Lap of {position_gain_loss__} Positions')
    return fig


@pn.depends(athlete_races_single_event)
def x_plus_y_position_selection(athlete_races_single_event__):
    """
    A histogram displaying which advancing position an athlete selects, when there are multiple available.
    """
    advancing_races = athlete_races_single_event__[athlete_races_single_event__['Qual.'].isin(['Q', 'q', 'QA', 'qA'])]
    advancing_races = advancing_races.astype({'Place': int})

    fig, ax = get_ax()
    sns.histplot(data=advancing_races,
                 x="Place",
                 ax=ax).set_title('X + Y Position Selection')
    return fig


@pn.depends(athlete_laptimes)
def pacing_1500m_leading(athlete_laptimes__):
    """
    The average pace that the athlete likes to skate when leading the first 4 laps of the 1500m event.

    The amount that the athlete likes to pick up the pace when making a pass in the first 4 laps of the 1500m distance
    """
    laps = select_event_subset(athlete_laptimes__, EVENT_1500M)
    leading_pace = laps[(laps['lap'] > 1) &
                        (laps['lap'] < 5) &
                        (laps['lap_start_position'] == 1) &
                        (laps['position_change'] == 0)]['laptime'].mean()

    return pn.indicators.Number(name='1500m Leading Pace',
                                value=round(leading_pace, 2),
                                format='{value}s')


@pn.depends(athlete_laptimes)
def pacing_1500m_instigation(athlete_laptimes__):
    """
    The average pace that the athlete likes to skate when leading the first 4 laps of the 1500m event.

    The amount that the athlete likes to pick up the pace when making a pass in the first 4 laps of the 1500m distance
    """
    laps = select_event_subset(athlete_laptimes__, EVENT_1500M)
    early_passes_to_front = laps[(laps['lap'] > 1) &
                                 (laps['lap'] < 5) &
                                 (laps['lap_start_position'] > 1) &
                                 (laps['lap_end_position'] == 1)]

    speed_up_sum = 0
    denominator = len(early_passes_to_front)

    for idx, early_pass in early_passes_to_front.iterrows():
        previous_laptime = laps[(laps['season'] == early_pass['season']) &
                                (laps['competition'] == early_pass['competition']) &
                                (laps['event'] == early_pass['event']) &
                                (laps['gender'] == early_pass['gender']) &
                                (laps['round'] == early_pass['round']) &
                                (laps['race'] == early_pass['race']) &
                                (laps['instance_of_event_in_competition'] ==
                                 early_pass['instance_of_event_in_competition']) &
                                (laps['lap'] == early_pass['lap'] - 1)]
        if len(previous_laptime):
            speed_up_sum += previous_laptime.iloc[0]['laptime'] - early_pass['laptime']
        else:
            denominator -= 1

    return pn.indicators.Number(name='1500m Pace Instigation',
                                value=round((speed_up_sum / denominator), 3),
                                format='{value}s')


# set up sidebar display
ui_template.sidebar.append(athlete_name)
ui_template.sidebar.append(event_distance)
ui_template.sidebar.append(start_position)
ui_template.sidebar.append(position_gain_loss)

# set up main display
ui_template.main.append(
    pn.Column(pn.Row(first_lap_positions, half_lap_500m_mean, half_lap_500m_hist),
              pn.Row(start_performance_500m, fastest_leading_laptimes, fastest_following_laptimes),
              pn.Row(likely_lap_to_pass, x_plus_y_position_selection),
              pn.Row(pacing_1500m_leading, pacing_1500m_instigation))
)

# launch display
ui_template.show()
