import pandas as pd
import numpy as np
from tqdm import tqdm


def find_walkings(df: pd.DataFrame):
    df.drop(columns=['time_offset'], inplace=True)
    df.drop_duplicates(inplace=True)
    # изменяем тип столбцов с датами на datetime
    df['time_start_local'] = pd.to_datetime(df['time_start_local'])
    df['time_end_local'] = pd.to_datetime(df['time_end_local'])
    # рассчитываем продолжительность записи
    df['duration'] = df['time_end_local'] - df['time_start_local']
    df['date'] = df['time_start_local'].dt.date

    # delete back forward date rows
    df = df[df['duration'] > pd.Timedelta(0)]

    # Отсортируем данные по времени начала записи
    df.sort_values(by=['time_start_local'], inplace=True)

    # merge consecutive records
    merged = 1
    while merged != 0:
        df = df.merge(df, how = 'left', left_on=['time_end_local'],
                       right_on=['time_start_local'])
        df.rename(columns={'time_start_local_x':'time_start_local',
                            'time_end_local_x':'time_end_local',
                           'date_x':'date',
                            'steps_x': 'steps',
                            'duration_x':'duration'}, inplace=True)
        df['steps'][df['time_start_local_y'].notna()]=df['steps'][df['time_start_local_y'].notna()] \
            + df['steps_y'][df['time_start_local_y'].notna()]

        df['duration'][df['time_start_local_y'].notna()]=df['duration'][df['time_start_local_y'].notna()] \
            + df['duration_y'][df['time_start_local_y'].notna()]

        df['time_end_local'][df['time_start_local_y'].notna()]=\
            df['time_end_local_y'][df['time_start_local_y'].notna()]
        merged = len(df['time_end_local_y'][df['time_start_local_y'].notna()])

        df.drop(columns=['time_start_local_y', 'time_end_local_y','steps_y', 'date_y', 'duration_y'], inplace = True)

    # delete nested entries
    for i, d in tqdm(df.iterrows()):
        parrent_int = df[(df['time_start_local'] <= d.time_start_local) & (df['time_end_local'] >= d.time_end_local)]
        if len(parrent_int) > 1:
            df.drop([i], inplace=True)
    del parrent_int

    # delete duplicates with 1 second bias
    for i, d in df.iterrows():
        dupl = df[((df['time_start_local'] == d.time_start_local + pd.Timedelta(1, unit='second')) | \
                   (df['time_start_local'] == d.time_start_local - pd.Timedelta(1, unit='second'))) & \
                  ((df['time_end_local'] == d.time_end_local - pd.Timedelta(1, unit='second')) | \
                   (df['time_end_local'] == d.time_end_local - pd.Timedelta(1, unit='second'))) & \
                  (df['steps'] == d.steps)]
        if len(dupl) > 0:
            df.drop([i], inplace=True)

    # merge rows with small delays between
    changes = 1
    while changes != 0:
        df.reset_index(inplace=True)
        df.drop(columns=['index'], inplace=True)
        for i, d in df.iterrows():
            if i < len(df) - 1 and \
                    df['time_start_local'].iloc[i + 1] <= df['time_end_local'].iloc[i] + pd.Timedelta(30,
                                                                                                      unit='second'):
                df['time_end_local'].iloc[i] = df['time_end_local'].iloc[i + 1]
                st = df['steps'].iloc[i] + df['steps'].iloc[i + 1]
                df['steps'].iloc[i] = st
                break
        changes = len(df) - len(df.drop_duplicates(subset=['time_end_local'], keep='first'))
        df.drop_duplicates(subset=['time_end_local'], keep='first', inplace=True)

    # обновим колонку продолжительности после пересчетов и объединений
    df['duration'] = df['time_end_local'] - df['time_start_local']
    # посчитаем темп передвижения в минуту
    df['speed'] = df['duration'] / np.timedelta64(1, 's')
    df['speed'] = 60 * df['steps'] / df['speed']

    df = df[['date', 'time_start_local', 'time_end_local', 'steps']]\
        [(df['duration']>=pd.Timedelta(5, unit='min'))&(df['speed']>59)&(df['speed']<121)]
    df.rename(columns={'time_start_local': 'start', 'time_end_local': 'end'}, inplace=True)
    df['start'] = df['start'].dt.strftime("%Y-%m-%d %H:%M:%S")
    df['end'] = df['end'].dt.strftime("%Y-%m-%d %H:%M:%S")

    walkings = {}
    cols = ['start', 'end', 'steps']
    for i in df['date'].unique():
        walkings[i.strftime("%Y-%m-%d")] = df[cols][df['date'] == i].to_dict('records')
    return walkings