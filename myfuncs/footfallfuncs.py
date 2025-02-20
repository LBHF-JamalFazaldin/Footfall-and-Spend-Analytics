import pandas as pd
import numpy as np
from scipy.stats import zscore

# Function to apply datetime features i.e dt.year, dayofweek, am/pm values.
def apply_features(df, date='count_date', **kwargs):
    print('\nApplying features...')
    try:
        df[date] = pd.to_datetime(df[date])
        df['year'] = df[date].dt.year
        df['day_name'] = df[date].dt.dayofweek

        day_dict = {
            '0':['Monday','Weekday'],
            '1':['Tuesday','Weekday'],
            '2':['Wednesday','Weekday'],
            '3':['Thursday','Weekday'],
            '4':['Friday','Weekday'],
            '5':['Saturday','Weekend'],
            '6':['Sunday','Weekend']
        }

        df['day_name'] = df['day_name'].astype(str)
        df['week_name'] = df['day_name'].map(lambda x: day_dict[x][1])
        df['day_name'] = df['day_name'].map(lambda x: day_dict[x][0])
        
        time = kwargs.get('time', False)
        if time:
            try:
                time_dict = {
                    '00-03':'6pm-6am',
                    '03-06':'6pm-6am',
                    '06-09':'6am-6pm',
                    '09-12':'6am-6pm',
                    '12-15':'6am-6pm',
                    '15-18':'6am-6pm',
                    '18-21':'6pm-6am',
                    '21-24':'6pm-6am'
                }
                df['day_night'] = df[time].map(time_dict)
            except KeyError as e:
                print(f'Invalid time column: {e}\n')
        
        print('Features applied.\n')
        return df
    except Exception as e:
        print(f'Error applying features: {e}\n')
        return pd.DataFrame()

# Function to pivot daynight values for a given dataset of aggregated footfall count to related am/pm fields for mapping purposes.
def transform_to_daynight(df, **kwargs):
    print('\nTransforming to daynight...')
    try:
        primary_key = kwargs.get('primary_key',False)
        index = ['count_date','year','day_name','week_name']
        if primary_key:
            index = index + [primary_key]
        transform = df.pivot_table(
            index =index,
            columns='day_night',
            values='corrected_value_total'
        ).reset_index()
        return transform
    except Exception as e:
        print(f'Error transforming to daynight: {e}\n')
        return pd.DataFrame()

# Function to detect anomalies within the footfall counts
def detect_anomalies(df, **kwargs):
    print('\nDetecting anomalies...')
    try:
        used_keys = {
            'footfall_type','day_night',
            'agg','std','primary_key'
        }
        redundant_kwargs = set(kwargs.keys()) - used_keys
        if redundant_kwargs:
            print(f'Redundant kwargs: {redundant_kwargs}\n')
            return pd.DataFrame()
        kwargs = {key: kwargs.get(key, f'default_value_{key}') for key in used_keys}
        
        footfall_type = kwargs.get('footfall_type')
        agg = kwargs.get('agg')
        categories = [
            'count_date',f'{footfall_type}_{agg}',
            'zscore','year','is_anomaly?',
            'day_name','week_name','day_night'
        ]
        primary_key = kwargs.get('primary_key')
        if primary_key:
            if type(primary_key) is not list:
                keywords = [primary_key]
            else:
                keywords = primary_key
            for keyword in keywords:
                categories = categories + [f'{keyword}']

        std = kwargs.get('std', 3)
        anomalies = df.copy()
        anomalies['zscore'] = anomalies.groupby(keywords)[f'{footfall_type}_{agg}'].transform(zscore)
        anomalies['is_anomaly?'] = (anomalies['zscore'] < -std) | (anomalies['zscore'] > std)
        num_anomalies = anomalies['is_anomaly?'].sum()
        print(f'{num_anomalies} anomalies have been detected.')

        anomalies = anomalies[categories]
        anomalies['moving_average'] = anomalies.groupby(keywords)[f'{footfall_type}_{agg}'].transform(lambda x: x.rolling(window=7).mean())
        anomalies['corrected_value'] = np.where(
            anomalies['is_anomaly?'],
            anomalies['moving_average'],anomalies[f'{footfall_type}_{agg}']
        )
        anomalies['corrected_ma_monthly'] = anomalies.groupby(keywords)['corrected_value'].transform(lambda x: x.rolling(window=30).mean())
        anomalies['corrected_ma_weekly'] = anomalies.groupby(keywords)['corrected_value'].transform(lambda x: x.rolling(window=7).mean())

        print('Anomalies have been flagged and corrected.\n')
        return anomalies
    except Exception as e:
        print(f'Error detecting anomalies: {e}\n')
        return pd.DataFrame()

# Function to aggregate footfall counts based off a specified grouping/field and anomaly detection
def agg_footfall_data(df, **kwargs):
    print('\nAggregating footfall data...')
    try:
        used_keys = {
            'primary_key','day_night',
            'agg', 'footfall_type','time_indicator'
        }
        redundant_kwargs = set(kwargs.keys()) - used_keys
        if redundant_kwargs:
            print(f'Redundant kwargs: {redundant_kwargs}')
            return pd.DataFrame()
        unused_keys = set(used_keys) - set(kwargs.keys())
        if unused_keys:
            print(f'Missing kwargs: {unused_keys}\nThese args will be set to default values')
        
        time_indicator = kwargs.get('time_indicator','time_indicator')
        df = apply_features(df, time=time_indicator)

        merge_list = [
            'day_name','week_name','day_night','count_date'
        ]
        new_categories = [
            'count_date','day_name','week_name','day_night',
            'corrected_ma_monthly','corrected_ma_weekly',
            'corrected_value'
        ]
        
        primary_key = kwargs.get('primary_key')
        if primary_key:
            if not isinstance(primary_key, list):
                keywords = [primary_key]
            for keyword in keywords:
                merge_list = [f'{keyword}'] + merge_list
                new_categories = new_categories + [f'{keyword}']
        
        agg = kwargs.get('agg','sum')
        agg_data = df.groupby(merge_list + ['year']).agg(
            residents_sum = ('resident',f'{agg}'),
            workers_sum = ('worker',f'{agg}'),
            visitors_sum = ('visitor',f'{agg}')
        )
        agg_data = agg_data.reset_index()
        agg_data = agg_data.sort_values(
            ['count_date'],
            ascending=False
        )

        default_values = ['residents','workers','visitors']
        footfall_type = kwargs.get('footfall_type', default_values)
        anomalies = {}
        i = 0
        for footfall in footfall_type:
            if footfall not in default_values:
                raise KeyError(f'Invalid footfall type: [{footfall}]')
        for footfall in footfall_type:
            i = i + 1
            anomalies[f'{footfall}_z'] = detect_anomalies(agg_data,footfall_type=footfall,std=2.6,primary_key=primary_key,agg=agg)
            if i > len(footfall_type)-1:
                new_categories = new_categories + ['year']
            anomalies[f'{footfall}_z'] = anomalies[f'{footfall}_z'][new_categories]

        footfall_data = pd.merge(
            anomalies['residents_z'], anomalies['workers_z'],
            how='left', on=merge_list,
            suffixes=['_residents','_workers']
        ).merge(
            anomalies['visitors_z'],
            how='left', on=merge_list,
        ).rename(columns={
                'corrected_value':'corrected_value_visitors',
                'corrected_ma_monthly':'corrected_ma_monthly_visitors',
                'corrected_ma_weekly':'corrected_ma_weekly_visitors'
            }
        )
        for footfall in footfall_type:
            if footfall not in default_values:
                raise KeyError(f'Invalid footfall type: [{footfall}]')
        for footfall in footfall_type:
            footfall_data['corrected_value_total'] = 0
            footfall_data['corrected_value_total'] = footfall_data['corrected_value_total'] + footfall_data[f'corrected_value_{footfall}']
        footfall_data['corrected_value_total'].fillna(0, inplace=True)
        footfall_data['corrected_ma_monthly_total'] = footfall_data.groupby(keywords)['corrected_value_total'].transform(lambda x: x.rolling(window=30).mean())
        footfall_data['corrected_ma_weekly_total'] = footfall_data.groupby(keywords)['corrected_value_total'].transform(lambda x: x.rolling(window=7).mean())

        print('Footfall Data Aggregated.\n')
        return footfall_data
    except Exception as e:
        print(f'Error aggregating footfall data: {e}\n')
        return pd.DataFrame()

# Function that encapsulates a variety of methods to output a desired aggregation of footfall counts for mapping purposes.
def typical_footfall(footfall_data, start, end, **kwargs):
    print('Calculating typical daily footfall...\nFor Weedays and Weekends and Weekly averages...\n')
    columns_to_drop = [
        'OID_','Col_ID','Row_ID','Hex_ID',
        'Centroid_X','Centroid_Y','area',
        'Shape_Length','Shape_Area'
    ]
    for column in columns_to_drop:
        if column in footfall_data.columns:
            footfall_data = footfall_data.drop(columns=column)

    footfall_data['count_date'] = pd.to_datetime(footfall_data['count_date'])

    footfall_data = footfall_data[
        (footfall_data['count_date'] <= pd.to_datetime(end)) &
        (footfall_data['count_date'] >= pd.to_datetime(start))]
    
    primary_key = kwargs.get('primary_key', 'hex_id')
    time_indicator = kwargs.get('time_indicator','time_indicator')
    columns_to_fill = [
        'resident','worker','visitor'
    ]
    footfall_data.loc[:, columns_to_fill] = footfall_data[columns_to_fill].applymap(lambda x: np.nan if x < 0 else x)
    footfall_data[columns_to_fill] = footfall_data[columns_to_fill].fillna(0)
    footfall_data = footfall_data.sort_values(by=['count_date',f'{time_indicator}',f'{primary_key}'])
    

    footfall_data = agg_footfall_data(
        footfall_data,
        primary_key=primary_key,
        agg=kwargs.get('agg','sum'),
        footfall_type=kwargs.get('footfall_type',['residents','workers','visitors'])
    )
        
    if kwargs.get('day_night',False):
        footfall_data = transform_to_daynight(footfall_data, primary_key=primary_key)
        averages = footfall_data.copy()
        averages = averages.groupby(['year','week_name',f'{primary_key}']).agg(
            daytime_mean = ('6am-6pm','mean'),
            nighttime_mean = ('6pm-6am','mean')
        ).reset_index()
        weekday = averages[averages['week_name'] == 'Weekday']
        weekend = averages[averages['week_name'] == 'Weekend']
        typical = footfall_data.groupby(['year',f'{primary_key}']).agg(
            daytime_mean = ('6am-6pm','mean'),
            nighttime_mean = ('6pm-6am','mean')
        ).reset_index()
    else:
        averages = footfall_data.copy()
        averages = averages.groupby(['year','week_name',f'{primary_key}']).agg(
            averages = ('corrected_value_total','mean'),
        ).reset_index()
        weekday = averages[averages['week_name'] == 'Weekday']
        weekend = averages[averages['week_name'] == 'Weekend']
        typical = footfall_data.groupby(['year',f'{primary_key}']).agg(
            averages = ('corrected_value_total','mean'),
        ).reset_index()

    typical_footfall = {
        0 : typical,
        1 : weekday,
        2 : weekend
    }

    return typical_footfall