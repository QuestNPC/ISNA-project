import pandas as pd
import os
import multiprocessing as mp
import numpy as np
import re

wd = os.getcwd()



def combine_df(files):
    file1 = files[0]
    file2 = files[1]
    file3 = files[2]
    try:
        df_detail = pd.read_feather(file1)
    except FileNotFoundError:
        df_detail = pd.DataFrame(np.nan, index=[], columns=["Tweet_ID","Likes","Retweets"]) #remake a missing frame
    
    try:
        df_to_merge = pd.read_feather(file2) #if hashtag file is missing whole thing is skipped, but that doesn't matter as we need hashtag data for the analysis to be made
    except:
        return
    match = re.search(r'\d{4}_\d{2}_\d{2}_\d{2}', file1) #take time data from file name, this is done now again due to detection of possible issues gaps in files

    if match:
        date_str = match.group(0)
        date = pd.to_datetime(date_str, format="%Y_%m_%d_%H")
    
    combo = pd.merge(df_detail, df_to_merge ,on="Tweet_ID", how='left')

    #make missing time data, could have skipped date shit earlier if i had forseeen this potential issue
    

    try:
        df_sent = pd.read_feather(file3)
    except FileNotFoundError:
        df_sent = pd.DataFrame(np.nan, index=[], columns=['Tweet_ID','Logits_Neutral','Logits_Positive','Logits_Negative'])
    df_to_merge = df_sent[['Tweet_ID', 'Logits_Neutral', 'Logits_Positive', 'Logits_Negative']]
    combo = pd.merge(combo, df_to_merge,on="Tweet_ID", how='outer')
    combo['Date Created'] = date
    combo['Week'] = combo['Date Created'].dt.isocalendar().week
    combo['Year'] = combo['Date Created'].dt.isocalendar().year
    return combo

def processor(files, output):
    for file in files:
        if file[0].endswith(".fea"):
            df = combine_df(file)
            filename = os.path.basename(file[0])
            path = os.path.join(wd,output)
            path = os.path.join(path, filename)
            df.reset_index(drop=True)
            try:
                df.to_feather(path)
            except AttributeError:
                print('NoneType', path)
            except ValueError:
                print('Empty', path)
            if not os.path.exists(path):
                print(path)

def make_combined_df(output_dir):
    print("Making dataframes")

    details_path = 'COVID19_Tweets_Dataset\Summary_Hashtag'
    path = os.path.join(wd, details_path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".fea"):
                #changes was made to go by hashtag folder instead of details folder, as missing details file is not as big of a issue compared to hashtag file
                relative_path2 = os.path.relpath(os.path.join(file, root))
                relative_path2 = os.path.join(relative_path2, file)
                relative_path = os.path.relpath(os.path.join(file, root.replace('Summary_Hashtag','Summary_Details')))
                relative_path = os.path.join(relative_path, file.replace('Summary_Hashtag','Summary_Details'))
                relative_path3 = os.path.relpath(os.path.join(file, root.replace('Summary_Hashtag','Summary_Sentiment')))
                relative_path3 = os.path.join(relative_path3, file.replace('Summary_Hashtag','Summary_Sentiment'))
                paths = [relative_path, relative_path2, relative_path3]
                all_files.append(paths)
    
    num_processes = 8
    chunk_size = 168 #hours in a week

    pool = mp.Pool(processes=num_processes)
    
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i:i+chunk_size]
        pool.apply_async(processor, args=(chunk, output_dir))
    
    pool.close()
    pool.join()
    return



if  __name__ == '__main__':
    output_dir = "combo"
    make_combined_df(output_dir)