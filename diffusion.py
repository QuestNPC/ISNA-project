import numpy as np
import os
import pandas as pd


wd = os.getcwd()

def calc_diff(tag, periods, path):
    path = os.path.join(wd, path)
    file = tag + '.fea'
    path = path + '\\'+ file
    df = pd.read_feather(path)
    
    diffusions = []
    for weeks in periods:
        sum1 = 0
        for week in weeks:
            week_num, year = week
            df_week=df.loc[(df['Week']== week_num)&(df['Year'] == year)]
            weekly_sum = df_week['Retweets'].sum()
            sum1 = sum1 + weekly_sum
        
        diffusions.append(sum1 / len(weeks))
    return diffusions        


    

    


if  __name__ == '__main__':
    hashtags = ['#covid19'] #tag of intrest
    #input periods of interests, each as a list of tuples, tuples being week your pairs like (week, year)
    periods = [[[(x,2020) for x in range(7,21)],[(x,2020) for x in range(24,43)],[(x,2022) for x in range(1,26)]]]
    output = ''
    df = pd.DataFrame()
    for i, hashtag in enumerate(hashtags):
        values = calc_diff(hashtag, periods[i], 'weekly')
        df[hashtag] = values
    print('Done')
    print(df)
    df.to_excel('Calculations\\diffusion.xlsx', index=False)