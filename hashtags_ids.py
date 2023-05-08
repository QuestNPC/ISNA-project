import pandas as pd
import os
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import numpy as np
from scipy.stats import t
import json
import multiprocessing as mp
import pickle
from numba import jit, cuda

'''
This would be the first file to run, it creates dictionary of hastags with corresponding tweet_ids, as well as counts how namy times they appear and arrange them from high-low, counting is also done to ner.
Dictionaries are saved in .json files.
'''
wd = os.getcwd()

def process_files(files, output_dir):
    hashtag_counts = pd.DataFrame()
    hashtag_ids = pd.DataFrame()

    for file in files:
        if file.endswith(".fea"):
            data = pd.read_feather(file, encoding='latin-1')
            data['Hashtag'] = data['Hashtag'].apply(lambda x: x.lower())

            df = data.groupby(["Hashtag"])

            sum_df = df.count()
            index_df = df['Tweet_ID'].apply(list)
            df1 = pd.concat([hashtag_counts, sum_df])
            hashtag_counts = df1.groupby(["Hastag"]).sum()
            df1 = pd.concat([index_df,hashtag_ids])
            df = df1.groupby(["Hashtag"])
            hashtag_ids = df['Tweet_ID'].apply(list)
    # save intermediate results to file
    filename = os.path.join(output_dir, os.path.basename(files[0]).replace(".fea", ".pkl"))
    with open(filename, "wb") as f:
        pickle.dump((hashtag_counts, hashtag_ids), f)



def hashtag_count(output_dir):
    '''
    Goes over the csv files in the folder and creates dictionary containing hashtags and how many times they were found,
    also creates dictionary that contains hashtags as keys and a list of ids of the tweets where the tag appeared.
    '''
    print('Counting hashtags')
    hashtag_path = 'COVID19_Tweets_Dataset\Summary_Hashtag'
    path = os.path.join(wd, hashtag_path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".fea"):
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                all_files.append(relative_path)

    num_processes = mp.cpu_count()
    chunk_size = int(len(all_files) / num_processes)

    pool = mp.Pool(processes=num_processes)
    
    for i in range(num_processes):
        start = i * chunk_size
        end = start + chunk_size
        if i == num_processes - 1:
            end = len(all_files)
        pool.apply_async(process_files, args=(all_files[start:end], output_dir))

    pool.close()
    pool.join()
    # read intermediate results back one by one and merge them
    hashtag_counts = {}
    hashtag_ids = {}
    for file in os.listdir(output_dir):
        if file.endswith(".pkl"):
            filename = os.path.join(output_dir, file)
            with open(filename, "rb") as f:
                counts, ids = pickle.load(f)
            for tag, count in counts.items():
                try:
                    hashtag_counts[tag] += count
                except KeyError:
                    hashtag_counts[tag] = count
            for tag, ids_list in ids.items():
                try:
                    hashtag_ids[tag].extend(ids_list)
                except KeyError:
                    hashtag_ids[tag] = ids[tag]
    sorted_hashtag_counts = sorted(hashtag_counts.items(), key=lambda x:x[1], reverse=True)
    for file in os.listdir(output_dir):
        filename = os.path.join(output_dir, file)
        os.remove(filename)                 #clear the trash
    return sorted_hashtag_counts, hashtag_ids


def process_ner(files, output_dir):
    ner_counts = pd.DataFrame()
    for file in files:
        if file.endswith(".fea"):
            df = pd.read_feather(file)
            grouped = df.groupby("NER_Text", as_index=False).count()
            df1 = pd.concat([ner_counts, grouped], ignore_index=True)
            ner_counts = df1.groupby("NER_Text", as_index=False).sum()  
    filename = os.path.join(output_dir, os.path.basename(file))
    ner_counts.to_feather(filename)

def ner_count(output_dir):
    '''
    Goes over the csv files in the folder and creates dictionary containing hashtags and how many times they were found,
    also creates dictionary that contains hashtags as keys and a list of ids of the tweets where the tag appeared.
    '''
    print('Counting ner')
    hashtag_path = 'COVID19_Tweets_Dataset\Summary_NER'
    path = os.path.join(wd, hashtag_path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".fea"):
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                all_files.append(relative_path)

    num_processes = 6
    chunk_size = 168 #hours in a week

    pool = mp.Pool(processes=num_processes)
    
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i:i+chunk_size]
        pool.apply_async(process_ner, args=(chunk, output_dir))
    
    pool.close()
    pool.join()

    ner_counts = pd.DataFrame()
    for file in os.listdir(output_dir):
        if file.endswith(".fea"):
            filename = os.path.join(output_dir, file)
            df = pd.read_feather(filename)
            df = df.drop(columns=["Start_Pos", "End_Pos"])
            ner_counts = pd.concat([ner_counts, df], ignore_index=True)
            ner_counts = ner_counts.groupby(["NER_Text"],as_index=False).sum()

    for file in os.listdir(output_dir):
        filename = os.path.join(output_dir, file)
        os.remove(filename)                 #clear the trash
    sorted_ner= ner_counts.sort_values(by=["Tweet_ID"], ascending=False).reset_index()
    return sorted_ner





if  __name__ == '__main__':
    path = "NER.fea"
    sorted_ner = ner_count("temp")
    sorted_ner.to_feather(path)