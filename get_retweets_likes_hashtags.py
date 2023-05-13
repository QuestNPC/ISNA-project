import pandas as pd
import os
import multiprocessing as mp

wd = os.getcwd()



def combine_df(file, df1):
    df = pd.read_feather(file)
    df = df.groupby(['Hashtag'], as_index=False)[['Likes','Retweets']].sum()
    df = pd.concat([df, df1], ignore_index=True)
    df = df.groupby(['Hashtag'], as_index=False)[['Likes','Retweets']].sum()
    return df

def processor(files, output):
    df = pd.DataFrame()
    for file in files:
        df = combine_df(file, df)
    filename = os.path.basename(file)
    path = os.path.join(wd,output)
    path = os.path.join(path, filename)
    df.to_feather(path)

def make_likes_df():
    print("Making dataframes")

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
        pool.apply_async(processor, args=(chunk, 'temp'))
    
    
    pool.close()
    pool.join()

    df = pd.DataFrame()
    all_files = []
    path = 'temp'
    path = os.path.join(wd, path)
    for root,dirs,files in os.walk(path):
        for file in files:
            relative_path = os.path.relpath(os.path.join(file, root))
            relative_path = os.path.join(relative_path, file)
            all_files.append(relative_path)
   
    df = combiner(all_files)
    return df

def combiner(files):
    counts = pd.DataFrame()
    for file in files:
        df = pd.read_feather(file)
        df1 = pd.concat([counts, df], ignore_index=True)
        counts = df1.groupby(['Hashtag'], as_index=False)[['Likes','Retweets']].sum()

    for file in files:
        os.remove(file)
    return counts

if  __name__ == '__main__':
    output = "Counts\\retweets_likes.fea"
    df = make_likes_df()
    
    df[['Likes','Retweets']] = df[['Likes','Retweets']].astype('int64')
    df.to_feather(output)
    
    print(df.sort_values(by=["Likes"], ascending=False))
    print(df.sort_values(by=["Retweets"], ascending=False))