import pandas as pd
import os
import multiprocessing as mp

wd = os.getcwd()



def combine_df(files):
    file1 = files[0]
    file2 = files[1]
    file3 = files[2]
    df_detail = pd.read_feather(file1)
    df_detail = df_detail.drop(['Week', "Year"], axis=1).assign(Week_Year=df_detail[['Week','Year']].astype(int).apply(tuple, axis=1))
    df_to_merge = pd.read_feather(file2)
    combo = pd.merge(df_detail, df_to_merge,on="Tweet_ID", how='left')
    df_sent = pd.read_feather(file3)
    df_to_merge = df_sent[['Tweet_ID', 'Logits_Neutral', 'Logits_Positive', 'Logits_Negative']]
    combo = pd.merge(combo, df_to_merge,on="Tweet_ID", how='left')
    return combo

def processor(files, output):
    for file in files:
        if file[0].endswith(".fea"):
            df = combine_df(file)
            filename = os.path.basename(file[0])
            path = os.path.join(wd,output)
            path = os.path.join(path, filename)
            df.to_feather(path)

def make_hastag_df(output_dir):
    print("Making dataframes")

    details_path = 'COVID19_Tweets_Dataset\Summary_Details'
    path = os.path.join(wd, details_path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".fea"):
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                relative_path2 = os.path.relpath(os.path.join(file, root.replace('Summary_Details','Summary_Hashtag')))
                relative_path2 = os.path.join(relative_path2, file.replace('Summary_Details','Summary_Hashtag'))
                relative_path3 = os.path.relpath(os.path.join(file, root.replace('Summary_Details','Summary_Sentiment')))
                relative_path3 = os.path.join(relative_path3, file.replace('Summary_Details','Summary_Sentiment'))
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
    make_hastag_df(output_dir)