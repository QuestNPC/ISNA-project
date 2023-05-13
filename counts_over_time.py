import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd


wd = os.getcwd()

def stuff(tag, weeks, path):
    path = os.path.join(wd, path)
    file = tag + '.fea'
    path = path + '\\'+ file
    df = pd.read_feather(path)
    df = df.sort_values('Date Created')
    print('Counting...')

    grouped = df.groupby(["Week","Year"], as_index=False)
    totals = grouped['Count'].sum()

    # Create a DataFrame with all weeks and years
    all_weeks = pd.DataFrame(weeks, columns=["Week", "Year"])
    all_weeks['datetime'] = pd.to_datetime(all_weeks['Year'].astype(str) + all_weeks['Week'].astype(str) + '1', format='%Y%W%w')

    # Merge all_weeks with totals to ensure all weeks and years are included
    totals_df = pd.merge(all_weeks, totals, how='left', on=["Week", "Year"])
    totals_df['Count'] = totals_df['Count'].fillna(0)
    totals_df = totals_df.set_index(['Week', 'Year'])

    
    avg = grouped['Count'].mean()
    std = grouped['Count'].std()
    skew = grouped['Count'].skew()
    kurt = grouped['Count'].apply(pd.DataFrame.kurt)

    avg.columns = ["Week","Year",'Average']
    std.columns = ["Week","Year",'Standard Deviation']
    skew.columns = ["Week","Year",'Skewness']
    kurt.columns = ["Week","Year",'Kurtosis']
    print('Making df...')

    # Sort the unique pairs

    df_results = pd.merge(all_weeks, totals,how='left', on=['Year', 'Week'])
    df_results = pd.merge(df_results, avg,how='left', on=['Year', 'Week'])
    df_results = pd.merge(df_results, std,how='left', on=['Year', 'Week'])
    df_results = pd.merge(df_results, skew,how='left', on=['Year', 'Week'])
    df_results = pd.merge(df_results, kurt,how='left', on=['Year', 'Week'])
    print('Making a plot...')
    
    df_results = df_results.sort_values(['datetime'])


    x = np.arange(len(weeks))
    plt.figure(figsize=(20, 8))
    plt.grid(axis='y', alpha=0.7)
    plt.bar(x, df_results['Count'])
    plt.xticks(x, weeks, rotation=90)
    plt.title('Weekly hashtag usage of ' + tag)
    plt.xlabel('Week, Year')
    plt.title('Weekly totals for ' + tag)
    ax = plt.gca()
    
    [l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % 8 != 0]
    
    plt.show()

    plt.figure(figsize=(20, 8))
    plt.grid(axis='y', alpha=0.7)
    plt.scatter(x, df_results['Average'], label='Average per hour for ' + tag, marker='.',color='red')
    plt.errorbar(x, df_results['Average'], yerr=df_results['Standard Deviation'],fmt="o")
    plt.xlabel('Week, Year')
    plt.xticks(x, weeks, rotation=90)
    plt.title('Hourly averages with std for ' + tag)
    ax = plt.gca()
    [l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % 8 != 0]
    
    plt.show()

    df_results.to_excel('Calculations\\' + tag +'_calc.xlsx', index=False)
    


if  __name__ == '__main__':
    weeks = [(x , 2020) for x in range(4,54)] + [(y, 2021) for y in range(1,53)] + [(z, 2022) for z in range(1,53)]
    hashtags = ['#covid19','#coronavirus','#covid', '#corona', '#lockdown', '#bts', '#pfizer', '#vaccine']
    output = ''
    for hashtag in hashtags:
        
        stuff(hashtag, weeks, 'weekly')