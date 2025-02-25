from matplotlib.pyplot import matplotlib as plt

def plot_footfall(df, df2=None, year=False, category=False, dual_axis=False):
    tf = df.copy()
    if df2 is not None:
        tf2 = df2.copy()
    title = f'Comparison of Normalised Footfall (Monthly MA)'
    if year:
        tf = tf[tf['year'] == year]
        if df2 is not None:
            tf2 = tf2[tf2['year'] == year]
        title = f'{title} ({year})'
    
    if category == 'normalized':
        tf['corrected_normalized'] = tf['corrected_ma_monthly_total']/tf['corrected_ma_monthly_total'].max()
        if df2 is not None:
            tf2['corrected_normalized'] = tf2['corrected_ma_monthly_total']/tf2['corrected_ma_monthly_total'].max()

    if dual_axis:
        fig, ax1 = plt.subplots(figsize=(10, 7))
        ax1.plot(
            tf['count_date'],
            tf[f'corrected_{category}'],
            label='Footfall (London)',
            color='red'
        )
        ax1.set_xlabel('Date', fontsize=14)
        ax1.set_ylabel('Footfall Count (London)', color='blue', fontsize=14)
        ax1.tick_params(axis='y', color='blue', labelsize=12)
        ax1.legend(loc='upper left')

        if df2 is not None:
            ax2 = ax1.twinx()  # Create a secondary y-axis
            ax2.plot(
                tf2['count_date'],
                tf2[f'corrected_{category}'],
                label='Footfall (H&F)',
                color='blue'
            )
            ax2.set_ylabel('Footfall Count (H&F)', color='red')
            ax2.tick_params(axis='y', labelcolor='red')
            ax2.legend(loc='upper right')

        plt.title(f'{title}')
        plt.show()

    if category and dual_axis == False:
        plt.figure(figsize=(12,7))

        plt.plot(
            tf['count_date'],
            tf[f'corrected_{category}'],
            label = 'Normalised Footfall (London)',
            color = 'red'
        )

        if df2 is not None:
            plt.plot(
            tf2['count_date'],
            tf2[f'corrected_{category}'],
            label = 'Normalised Footfall (H&F)',
            color = 'blue'
            )
        
        seasons = {
            'Winter':('01-01','03-20','lightblue'),
            'Spring':('03-21','06-20','lightgreen'),
            'Summer':('06-21','09-22','gold'),
            'Autumn':('09-23','12-21','orange'),
            'Christmas':('12-22','12-31','red'),
        }
        if year:
            plot_years = year
            for season, (start_md, end_md, colour) in seasons.items():
                start = f'{year}-{start_md}'
                end = f'{year}-{end_md}'
                plt.axvspan(pd.to_datetime(start), pd.to_datetime(end), color=colour, alpha=0.25, label=season)
        else:
            plot_years = range(2022,2025)         
            for year in plot_years:
                for season, (start_md, end_md, colour) in seasons.items():
                    start = f'{year}-{start_md}'
                    end = f'{year}-{end_md}'
                    plt.axvspan(pd.to_datetime(start), pd.to_datetime(end), color=colour, alpha=0.25, label=season)
        
        handles, labels = plt.gca().get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Normalised Footfall', fontsize=12)
        plt.title(f'{title}', fontsize=16)
        plt.legend(
            by_label.values(),
            by_label.keys(),
            fontsize=9
        )
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.xlim(
            right = tf['count_date'].max(),
            left = tf['count_date'].min()
        )
        plt.show()

    else:
        print('## Please specify a category ##')

def plot_daily_footfall(df, df2=None, year=False, day_night=False):
    title = f'Comparison of Daytime and Nightime Footfall'
    merge_list = ['day_name']
    tf = df.copy()
    if df2 is not None:
        tf2 = df2.copy()
    if year:
        tf = tf[tf['year'] == year]
        if df2 is not None:
            tf2 = tf2[tf2['year'] == year]
        title = f'{title} ({year})'
    
    if day_night:
        tf = ff.transform_to_daynight(tf)
        if df2 is not None:
            tf2 = ff.transform_to_daynight(tf2)

    # Aggregate data for both datasets and merge them
    aggregated_data = tf.groupby(merge_list).agg(
        Nighttime_mean = ('6pm-6am','mean'),
        Daytime_mean = ('6am-6pm','mean'),
    ).reset_index()
    
    if df2 is not None:
        aggregated_data = pd.merge(
            aggregated_data,
            tf2.groupby(merge_list).agg(
            Nighttime_mean = ('6pm-6am','mean'),
            Daytime_mean = ('6am-6pm','mean'),
            ).reset_index(),
            how='left', on=merge_list,
            suffixes=['_lon', '_lbhf']
        )

    dictionary = {
        'Monday':'0',
        'Tuesday':'1',
        'Wednesday':'2',
        'Thursday':'3',
        'Friday':'4',
        'Saturday':'5',
        'Sunday':'6'
    }
    aggregated_data['day_order'] = aggregated_data['day_name'].map(dictionary)
    aggregated_data = aggregated_data.set_index('day_order').sort_index()
    
    # Extract categories and values for plotting
    categories = aggregated_data['day_name'].unique()

    if df2 is not None:
        night1 = aggregated_data['Nighttime_mean_lon']
        day1 = aggregated_data['Daytime_mean_lon']
        night2 = aggregated_data['Nighttime_mean_lbhf']
        day2 = aggregated_data['Daytime_mean_lbhf']
    else:
        night1 = aggregated_data['Nighttime_mean']
        day1 = aggregated_data['Daytime_mean']

    # Create the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    bar_width = 0.35
    index = np.arange(len(categories))

    if df2 is not None:
        # Plot daytime and nighttime footfall data
        ax.bar(index, day1, bar_width, label='Daytime Footfall (LDN)', color='skyblue')
        ax.bar([i + bar_width for i in index], night1, bar_width, label='Nighttime Footfall (LDN)', color='blue')
        ax.bar(index, day2, bar_width, label='Daytime Footfall (LBHF)', color='skyblue')
        ax.bar([i + bar_width for i in index], night2, bar_width, label='Nighttime Footfall (LBHF)', color='blue')
    else:
        # Plot daytime and nighttime footfall data
        ax.bar(index, day1, bar_width, label='Daytime Footfall (LBHF)', color='skyblue')
        ax.bar([i + bar_width for i in index], night1, bar_width, label='Nighttime Footfall (LBHF)', color='blue')

    # Set axis labels and title
    ax.set_xlabel("Days of the Week", fontsize=14)
    ax.set_ylabel("Total Footfall", fontsize=14)
    ax.set_title("Comparison of Daytime and Nighttime Footfall (2024)", fontsize=16)
    ax.set_xticks([i + bar_width / 2 for i in index])
    ax.set_xticklabels(aggregated_data['day_name'])
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{int(x/1_000)}K'))
    ax.tick_params(axis='both', which='major', labelsize=12)
    if df2 is not None:
        ax.set_ylim([min(night2)-(min(night2)*0.1), max(day1) * 1.1])
    else:
        ax.set_ylim([min(night1)-(min(night1)*0.1), max(day1) * 1.1])

    ax.legend(fontsize=14)
    plt.tight_layout()
    plt.show()
    
def calulcaute_QoQ_values(df):
    df = df.set_index('count_date')
    df = df[['corrected_value_total']]
    quarterly_df = df.resample('Q').mean()
    # Calculate Quarter-over-Quarter (QoQ) percentage change
    quarterly_df['QoQ_change'] = quarterly_df['corrected_value_total'].pct_change() * 100
    # Calculate Year-over-Year (YoY) percentage change, looking back 4 quarters
    quarterly_df['YoY_change'] = quarterly_df['corrected_value_total'].pct_change(periods=4) * 100
    quarterly_df = quarterly_df.round(2)
    display(quarterly_df)
    return quarterly_df