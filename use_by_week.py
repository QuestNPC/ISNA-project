import pandas as pd
import os
import multiprocessing as mp

wd = os.getcwd()



def combine_df(file, df1, tag):
    df = pd.read_feather(file)
    count = 0
    if not df.empty:
        count = df['Hashtag'].value_counts()[tag]
    df['Hashtag'] = df['Hashtag'].str.lower()
    df = df.loc[df['Hashtag'] == tag][['Logits_Neutral','Logits_Positive','Logits_Negative','Retweets','Week_Year']]
    if not count == 0:  #avoiding issues with dataframes with no hits on hashtag
        df['Count'] = pd.Series(count, index=df.index[[0]])
    df = pd.concat([df, df1], ignore_index=True)
    return df

def processor(files, output, tag, i):
    df = pd.DataFrame()
    for file in files:
        df = combine_df(file, df, tag)
    filename = tag + '_' + str(i) + '.fea'
    path = os.path.join(wd,output)
    path = os.path.join(path, filename)
    df.to_feather(path)

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
    combiner(all_files, output_dir)
    print('Done for: ' + hashtag)

def combiner(files, output):
    counts = pd.DataFrame()
    filename = ""
    for file in files:
        df = pd.read_feather(file)
        if not df.empty:                            #if tag hasn't been used during the peroid of the dataframe, it is empty and can be skipped
            counts = pd.concat([counts, df], ignore_index=True)
            filename = os.path.join(output, os.path.basename(file))

    print(counts)
    counts.to_feather(filename)
    for file in files:
        os.remove(file)

if  __name__ == '__main__':
    output_dir = "weekly"
    #give list of top hashtags identified
    hashtags = ['#covid19','#coronavirus','#covid', '#corona', '#lockdown', '#bts', '#pfizer', '#vaccine']
    for hashtag in hashtags:
        make_likes_df(output_dir, hashtag)