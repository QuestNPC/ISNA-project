import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd


wd = os.getcwd()

def calc_week_sent_stats(tag, weeks, path, df_re):
    path = os.path.join(wd, path)
    file = tag + '.fea'
    path = path + '\\'+ file
    df = pd.read_feather(path)
    grouped = df.groupby(["Week","Year"], as_index=False)
    neutral = grouped['Logits_Neutral'].sum()
    negative = grouped['Logits_Negative'].sum()
    positive = grouped['Logits_Positive'].sum()
    group_sum = grouped.sum()
    print(group_sum)
    
    all_weeks = pd.DataFrame(weeks, columns=["Week", "Year"])
    all_weeks['datetime'] = pd.to_datetime(all_weeks['Year'].astype(str) + all_weeks['Week'].astype(str) + '1', format='%Y%W%w')

    max_labels = []
    max_week=[]
    max_year=[]
    for n, p, neu in zip(negative['Logits_Negative'], positive['Logits_Positive'], neutral['Logits_Neutral']):
        max_sum = max(n, p, neu)
        if max_sum == n:
            max_labels.append('negative')
            max_week.append(negative.loc[negative['Logits_Negative']==n]['Week'].values[0])
            max_year.append(negative.loc[negative['Logits_Negative']==n]['Year'].values[0])
        elif max_sum == p:
            max_labels.append('positive')
            max_week.append(positive.loc[positive['Logits_Positive']==p]['Week'].values[0])
            max_year.append(positive.loc[positive['Logits_Positive']==p]['Year'].values[0])
        else:
            max_labels.append('neutral')
            max_week.append(neutral.loc[neutral['Logits_Neutral']==neu]['Week'].values[0])
            max_year.append(neutral.loc[neutral['Logits_Neutral']==neu]['Year'].values[0])
    
    df_result = pd.DataFrame()
    df_result[tag] = max_labels
    df_result['Week'] = max_week
    df_result['Year'] = max_year

    negative = pd.merge(all_weeks, negative,how='left', on=['Week','Year'])
    positive = pd.merge(all_weeks, positive,how='left', on=['Week','Year'])
    negative = negative.sort_values(['datetime'])
    positive = positive.sort_values(['datetime'])
    df_re = pd.merge(df_re,df_result,how='left', on=['Week','Year'])
    plt.figure(figsize=(20, 8))

    x = np.arange(len(weeks))
    plt.plot(x, positive['Logits_Positive'],label='Positive',color='red')
    plt.plot(x, -negative['Logits_Negative'],label='Negative', color='blue')
    plt.xticks(x, weeks, rotation=90)
    plt.grid(axis='y', alpha=0.7)
    plt.title('Weekly positive and negative sentiments for ' + tag)
    ax = plt.gca()
    [l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % 8 != 0]
    plt.show()
    return df_re
    
    


if  __name__ == '__main__':
    weeks = [(x , 2020) for x in range(4,54)] + [(y, 2021) for y in range(1,53)] + [(z, 2022) for z in range(1,53)]
    hashtags = ['#covid19','#coronavirus','#covid', '#corona', '#lockdown', '#bts', '#pfizer', '#vaccine']
    #hashtags = ['#bts']
    output = ''

    sentiments = pd.DataFrame(weeks, columns=["Week", "Year"])
    sentiments['datetime'] = pd.to_datetime(sentiments['Year'].astype(str) + sentiments['Week'].astype(str) + '1', format='%Y%W%w')

    for hashtag in hashtags:
        sentiments = calc_week_sent_stats(hashtag, weeks, 'weekly', sentiments)
    
    sentiments.to_excel('Calculations\\sentiments.xlsx')