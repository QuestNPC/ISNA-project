import pandas as pd
import os
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import numpy as np
from scipy.stats import t
import pickle

wd = os.getcwd()

def weekly_retweets(df):
    
    grouped = df.groupby(['week', 'year'])
    weekly_sums = grouped['Retweets'].sum()
    
    return weekly_sums, df['Retweets'].sum()


def weekly_likes(df):
    grouped = df.groupby(['week', 'year'])
    weekly_sums = grouped['Likes'].sum()
    return weekly_sums, df['Likes'].sum()

def sentiment(tagshit):
    #weekly sentiment for tag + plot

    return

if  __name__ == '__main__':
    with open('df_dict.pickle', 'rb') as file:
        data = pickle.load(file)
    likes = {}
    retweets = {}
    likes_sum = {}
    retweets_sum = {}
    for key, value in data.items():
        likes[key], likes_sum[key] = weekly_likes(data[key])
        retweets[key], retweets_sum[key] = weekly_retweets(data[key])
    print(sorted(retweets_sum.items(), key=lambda x:x[1], reverse=True))
    print(sorted(likes_sum.items(), key=lambda x:x[1], reverse=True))