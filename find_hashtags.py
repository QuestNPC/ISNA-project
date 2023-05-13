import pandas as pd
import os
import multiprocessing as mp
import re
import numpy as np

wd = os.getcwd()

#.feather stopped working when storing the frame, .csv is alternative

def combine_df(file, df1, tag):
    df = pd.read_feather(file)
    count = 0
    df_filter = df.loc[df['Hashtag'] == tag][['Logits_Neutral','Logits_Positive','Logits_Negative','Retweets',"Week","Year", "Date Created", 'Likes']]
 
    if not df_filter.empty:
        count = df['Hashtag'].value_counts()[tag]
        df_filter.loc[df_filter.index[0], 'Count'] = count
    if df_filter.empty:
        #no hits, return 1 row of date, week, year, tag, and 
        df_filter = pd.DataFrame(columns=['Logits_Neutral', 'Logits_Positive', 'Logits_Negative', 'Retweets','Week', 'Year', 'Date Created', 'Likes', 'Count']) #column order is not of significance for us
        df_filter.loc[df_filter.index[0]] = [np.nan] * 9 + [0]
        
        match = re.search(r'\d{4}_\d{2}_\d{2}_\d{2}', file)
        date_str = match.group(0)
        date = pd.to_datetime(date_str, format="%Y_%m_%d_%H")
        
        df_filter['Week'] = date.week
        df_filter['Year'] = date.year
        df_filter['Date Created'] = date 
    df = pd.concat([df1, df_filter], ignore_index=True)
    return df

def processor(files, output, tag, i):
    df = pd.DataFrame()
    for file in files:
        df = combine_df(file, df, tag)
    filename = tag + '_' + str(i) + '.fea'
    path = os.path.join(wd,output)
    path = os.path.join(path, filename)
    try:
        df.to_feather(path)
    except Exception as e:
        print(e)
def make_likes_df(output_dir, hashtag):
    print("Step 1 for: ", hashtag)
    
    details_path = 'combo'
    path = os.path.join(wd, details_path)
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
        pool.apply_async(processor, args=(chunk, 'temp', hashtag, int(i/168)))
    
    pool.close()
    pool.join()
    
    print('Step 2 for: ' + hashtag)
    all_files = []
    path = 'temp'
    path = os.path.join(wd, path)
    for root,dirs,files in os.walk(path):
        for file in files:
            relative_path = os.path.relpath(os.path.join(file, root))
            relative_path = os.path.join(relative_path, file)
            all_files.append(relative_path)
    combiner(all_files, output_dir, hashtag)
    print('Done for: ' + hashtag)

def combiner(files, output, tag):
    counts = pd.DataFrame()
    filename = output + '\\' + tag + ".fea"
    for file in files:
        df = pd.read_feather(file)
        counts = pd.concat([counts, df], ignore_index=True)
        os.remove(file) #get rid of stuff to make sure no double reading happens
    
    print('Why are we looping?')
    counts.to_feather(filename)
    #for file in files:
    #    os.remove(file)

if  __name__ == '__main__':
    output_dir = "weekly"
    #give list of top hashtags identified
    hashtags = ['#coronavirus','#covid19','#covid', '#corona', '#lockdown', '#bts', '#pfizer', '#vaccine']
    for hashtag in hashtags:
        make_likes_df(output_dir, hashtag)