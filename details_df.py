import pandas as pd
import os
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import numpy as np
from scipy.stats import t
import json
import pickle
import multiprocessing as mp

wd = os.getcwd()

def process_file(filename, id_dict):
    data = pd.read_csv(filename)
    hastag_dataframes = {}
    for key, value in id_dict.items():
        df = data[data['Tweet_ID'].isin(value)]
        if not df.empty:
            subset = df[['Tweet_ID', 'Likes', 'Retweets', 'week', 'year']]
            try:
                hastag_dataframes[key] = pd.concat([hastag_dataframes[key], subset], ignore_index=True)
            except KeyError:
                hastag_dataframes[key] = subset
    return hastag_dataframes

def make_hastag_df(id_dict):
    print("Making dataframes")
    hastag_dataframes = {}

    hashtag_path = 'COVID19_Tweets_Dataset/Summary_Details'
    path = os.path.join(wd, hashtag_path)

    filenames = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.csv'):
                filenames.append(os.path.join(root, file))

    with mp.Pool(processes=4) as pool:
        results = [pool.apply_async(process_file, args=(filename, id_dict)) for filename in filenames]
        for result in results:
            for key, value in result.get().items():
                try:
                    hastag_dataframes[key] = pd.concat([hastag_dataframes[key], value], ignore_index=True)
                except KeyError:
                    hastag_dataframes[key] = value

    with open("df_dict.pickle", 'wb') as f:
        pickle.dump(hastag_dataframes, f)

    return



if  __name__ == '__main__':
    #skip going over all tags, focus on top 5 most used
    with open(r"id_dict.json", "r") as read_file:
        tag_ids = json.load(read_file)

    #get top 10
    with open(r"tag_counts.json", "r") as read_file:
        data = json.load(read_file)
    data = data[0:10]
    
    d = {}
    for key, value in data:
        d[key] = tag_ids[0][key]
    make_hastag_df(d)
    with open('df_dict.pickle', 'rb') as fout:
        hastag_dataframes = pickle.load(fout)
