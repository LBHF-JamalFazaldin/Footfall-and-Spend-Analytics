import pandas as pd
import numpy as np
from scipy.stats import zscore

def apply_features(df, date='count_date', **kwargs):
    """
    Adds datetime-based features to a DataFrame, such as year, day of week, month, and optionally day/night classification.

    Args:
        df (pd.DataFrame): Input DataFrame containing a date column.
        date (str, optional): Name of the date column. Defaults to 'count_date'.
        time (str, optional): Name of the time column for day/night mapping.

    Returns:
        pd.DataFrame: DataFrame with new feature columns added.d
    """
    print('\nApplying features...')
    try:
        df[date] = pd.to_datetime(df[date])
        df['year'] = df[date].dt.year
        df['day_name'] = df[date].dt.dayofweek
        df['month'] = df[date].dt.month

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

def transform_to_daynight(df, **kwargs):
    """
    Pivots the DataFrame to separate day/night values for aggregated footfall counts.

    Args:
        df (pd.DataFrame): Input DataFrame with day_night and corrected_value_total columns.
        primary_key (str, optional): Additional column to include in the index.

    Returns:
        pd.DataFrame: Pivoted DataFrame with day/night columns.
    """
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

def detect_anomalies(df, **kwargs):
    """
    Detects anomalies in footfall counts using z-score and corrects them using a moving average.

    Args:
        df (pd.DataFrame): Input DataFrame with aggregated footfall counts.
        footfall_type (str): Type of footfall ('residents', 'workers', 'visitors').
        agg (str): Aggregation method used.
        std (float, optional): Z-score threshold for anomaly detection. Defaults to 3.
        day_night (str, optional): Column name for day/night classification.
        primary_key (str, optional): Additional grouping column.

    Returns:
        pd.DataFrame: DataFrame with anomaly flags and corrected values.
    """
    print('\nDetecting anomalies...')
    try:
        used_keys = {
            'footfall_type','day_night',
            'agg','std','primary_key',
        }
        redundant_kwargs = set(kwargs.keys()) - used_keys
        if redundant_kwargs:
            print(f'Redundant kwargs: {redundant_kwargs}\n')
            return pd.DataFrame()
        
        std = kwargs.get('std', 3)
        footfall_type = kwargs.get('footfall_type')
        agg = kwargs.get('agg')
        
        merge_list = ['year']
        merge_list = [kwargs.get('day_night')] + merge_list if kwargs.get('day_night', False) else merge_list
        merge_list = [kwargs.get('primary_key')] + merge_list if kwargs.get('primary_key', False) else merge_list
        
        categories = merge_list + [
            'count_date',f'{footfall_type}_{agg}','zscore',
            'is_anomaly?','day_name','week_name','month'
        ]
        
        anomalies = df.copy()
        anomalies['zscore'] = anomalies.groupby(merge_list)[f'{footfall_type}_{agg}'].transform(zscore)
        anomalies['is_anomaly?'] = (anomalies['zscore'].abs() > std)
        num_anomalies = anomalies['is_anomaly?'].sum()
        print(f'{num_anomalies} anomalies have been detected.')

        anomalies = anomalies[categories]
        anomalies['moving_average'] = anomalies.groupby(merge_list)[f'{footfall_type}_{agg}'].transform(lambda x: x.rolling(window=7, min_periods=1).mean().round())
        anomalies['corrected_value'] = np.where(
            anomalies['is_anomaly?'],
            anomalies['moving_average'],anomalies[f'{footfall_type}_{agg}']
        )
        anomalies.to_csv(f"C:\\Users\\jf79\\OneDrive - Office Shared Service\\Documents\\H&F Analysis\\Python CSV Repositry\\{footfall_type}.csv")
        print('Anomalies have been flagged and corrected.\n')
        return anomalies
    except Exception as e:
        print(f'Error detecting anomalies: {e}\n')
        return pd.DataFrame()

def agg_footfall_data(df, **kwargs):
    """
    Aggregates footfall counts by specified grouping fields and applies anomaly detection.

    Args:
        df (pd.DataFrame): Input DataFrame with raw footfall counts.
        primary_key (str, optional): Column to group by.
        day_night (str, optional): Column for day/night classification.
        std (float, optional): Z-score threshold for anomaly detection.
        agg (str, optional): Aggregation method ('sum', 'mean', etc.).
        footfall_type (list, optional): List of footfall types to aggregate.
        time_indicator (str, optional): Name of the time indicator column.

    Returns:
        pd.DataFrame: Aggregated and corrected footfall data.
    """
    print('\nAggregating footfall data...')
    try:
        used_keys = {
            'primary_key','day_night','std',
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
            'count_date','day_name','week_name'
        ]
        merge_list = [kwargs.get('day_night')] + merge_list if kwargs.get('day_night',False) else merge_list
        merge_list = [kwargs.get('primary_key')] + merge_list if kwargs.get('primary_key',False) else merge_list
        new_categories = merge_list + ['corrected_value']
        
        agg = kwargs.get('agg','sum')
        agg_data = df.groupby(merge_list + ['year','month']).agg(
            residents_sum = ('resident',f'{agg}'),
            workers_sum = ('worker',f'{agg}'),
            visitors_sum = ('visitor',f'{agg}')
        )
        agg_data = agg_data.reset_index()
        agg_data = agg_data.sort_values(
            ['count_date'],
            ascending=True
        )

        default_values = ['residents','workers','visitors']
        footfall_types = kwargs.get('footfall_type', default_values)
        std = kwargs.get('std',3)
        corrected_dataframes = {}
        i = 0
        for footfall_type in footfall_types:
            if footfall_type not in default_values:
                raise KeyError(f'Invalid footfall type: [{footfall_type}]')
        for footfall_type in footfall_types:
            i = i + 1
            corrected_dataframes[f'{footfall_type}_z'] = detect_anomalies(
                agg_data,footfall_type=footfall_type,
                std=std,agg=agg,
                day_night = kwargs.get('day_night', False),
                primary_key=kwargs.get('primary_key', False)
            )
            if i > len(footfall_types)-1:
                new_categories = new_categories + ['year','month']
            corrected_dataframes[f'{footfall_type}_z'] = corrected_dataframes[f'{footfall_type}_z'][new_categories]
            
        footfall_data = pd.merge(
            corrected_dataframes['residents_z'], corrected_dataframes['workers_z'],
            how='left', on=merge_list,
            suffixes=['_residents','_workers']
        ).merge(
            corrected_dataframes['visitors_z'],
            how='left', on=merge_list,
        ).rename(columns={
                'corrected_value':'corrected_value_visitors'
            }
        )
        
        merge_list = ['year','month']
        merge_list = [kwargs.get('day_night')] + merge_list if kwargs.get('day_night',False) else merge_list
        merge_list = [kwargs.get('primary_key')] + merge_list if kwargs.get('primary_key',False) else merge_list
        footfall_data['corrected_value_total'] = 0
        for footfall_type in footfall_types:
            footfall_data['corrected_value_total'] = footfall_data['corrected_value_total'] + footfall_data[f'corrected_value_{footfall_type}']
        footfall_data['corrected_value_total'].fillna(0, inplace=True)
        footfall_data.to_csv(r"C:\Users\jf79\OneDrive - Office Shared Service\Documents\H&F Analysis\Python CSV Repositry\footfall_data.csv")
        
        print('Footfall Data Aggregated.\n')
        return footfall_data
    except Exception as e:
        print(f'Error aggregating footfall data: {e}\n')
        return pd.DataFrame()

def typical_footfall(footfall_data, start, end, **kwargs):
    """
    Calculates typical daily, weekday, and weekend footfall averages for mapping purposes.

    Args:
        footfall_data (pd.DataFrame): DataFrame containing footfall counts.
        start (str or datetime): Start date for filtering.
        end (str or datetime): End date for filtering.
        primary_key (str, optional): Column to group by. Defaults to 'hex_id'.
        time_indicator (str, optional): Name of the time indicator column.
        day_night (str, optional): Column for day/night classification.
        agg (str, optional): Aggregation method.
        footfall_type (list, optional): List of footfall types to aggregate.

    Returns:
        dict: Dictionary containing DataFrames for typical, weekday, and weekend footfall.
    """
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
        day_night=kwargs.get('day_night',False),
        primary_key=primary_key,
        agg=kwargs.get('agg', 'sum'),
        footfall_type=kwargs.get('footfall_type',['residents','workers','visitors'])
    )
        
    if kwargs.get('day_night', False):
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