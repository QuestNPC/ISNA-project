import pandas as pd
import os
import multiprocessing as mp
from math import fsum

wd = os.getcwd()

#for some reason the least summing results in 0 values

def process_ner(files, output_dir):
    #Some entries are combination of several words, but if memory serves right, single tweet could have several entries if several different entities were detected
    ner_counts = pd.DataFrame()
    for file in files:
        if file.endswith(".fea"):
            df = pd.read_feather(file)
            df["NER_Text"] = df["NER_Text"].str.lower()
            grouped = df.groupby("NER_Text", as_index=False).size()
            df1 = pd.concat([ner_counts, grouped], ignore_index=True)
            ner_counts = df1.groupby("NER_Text", as_index=False).sum()
    filename = os.path.join(output_dir, os.path.basename(file))
    ner_counts.to_feather(filename)

def ner_count(output_dir):

    print('Step 1...')
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
    chunk_size = 72

    pool = mp.Pool(processes=num_processes)
    
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i:i+chunk_size]
        pool.apply_async(process_ner, args=(chunk, output_dir))
    
    pool.close()
    pool.join()

    
    hashtag_path = 'temp'
    path = os.path.join(wd, hashtag_path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".fea"):
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                all_files.append(relative_path)

    num_processes = 6
    chunk_size = int(len(all_files)/8) #hours in a week

    pool = mp.Pool(processes=num_processes)
    print('Step 2 ....')
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i:i+chunk_size]
        pool.apply_async(combiner, args=(chunk, 'temp2'))
    
    pool.close()
    pool.join()
    

    hashtag_path = 'temp2'
    path = os.path.join(wd, hashtag_path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".fea"):
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                all_files.append(relative_path)
    print('Step 3 ....')
    combiner(all_files, "combo_result")

    ner_counts = pd.DataFrame()
    hashtag_path = 'combo_result'
    path = os.path.join(wd, hashtag_path)
    for root,dirs,files in os.walk(path):
        for file in files:
            relative_path = os.path.relpath(os.path.join(file, root))
            relative_path = os.path.join(relative_path, file)
            ner_counts = pd.read_feather(relative_path)
    print('Done.')
    sorted_ner= ner_counts.sort_values(by=["size"], ascending=False).reset_index(drop=True)
    return sorted_ner

def combiner(files, output):
    ner_counts = pd.DataFrame()
    filename = ""
    for file in files:
        df = pd.read_feather(file)
        df1 = pd.concat([ner_counts, df], ignore_index=True)
        ner_counts = df1.groupby("NER_Text",as_index=False).sum()   
        filename = os.path.join(output, os.path.basename(file))

    ner_counts.to_feather(filename)
    for file in files:
        os.remove(file)                 #clear the trash
    
if  __name__ == '__main__':

    path = "ner_count_all.fea"
    sorted_ner = ner_count("temp")
    print(sorted_ner)
    sorted_ner.to_feather(path)