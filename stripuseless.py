import pandas as pd
import os
import multiprocessing as mp

wd = os.getcwd()

def thinner():
    path = 'COVID19_Tweets_Dataset\Summary_Details'
    path = os.path.join(wd, path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):
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
        pool.apply_async(remove_excess_df, args=(all_files[start:end],))

    pool.close()
    pool.join()
    return

def remove_excess_df(files):

    #path shit here
    for file in files:
        df = pd.read_csv(file, encoding='latin-1')
        df['Date Created'] = pd.to_datetime(df['Date Created'])
        df['Week'] = df['Date Created'].dt.isocalendar().week
        df['Year'] = df['Date Created'].dt.isocalendar().year
        df = df[["Tweet_ID","Likes","Retweets","Week","Year"]]
        os.remove(file)  
        file = file.replace(".csv", ".fea")
        df.to_feather(file)

def feartherer(path):

    path = os.path.join(wd, path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):
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
        pool.apply_async(changer, args=(all_files[start:end],))
    
    pool.close()
    pool.join()

def changer(files):
    #path shit here
    for path in files:
        df = pd.read_csv(path, encoding='latin-1')
        os.remove(path)  
        path = path.replace(".csv", ".fea")
        df.to_feather(path)

def fixer(path):

    path = os.path.join(wd, path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):
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
        pool.apply_async(fix, args=(all_files[start:end],))

    pool.close()
    pool.join()
    return

def fix(files):
    for path in files:
        df = pd.read_csv(path, encoding='latin-1')
        if "Hastag" in list(df.columns.values):
            df.rename(columns = {'Hastag':'Hashtag'}, inplace = True)
        os.remove(path)  
        path = path.replace(".csv", ".fea")
        df.to_feather(path)
        df = pd.read_feather(path)
        print(df)

if __name__== '__main__':
    thinner()
    #path = 'COVID19_Tweets_Dataset\Summary_Hashtag'
    #fixer(path)
    #path = 'COVID19_Tweets_Dataset\Summary_NER'
    #feartherer(path)
    #path = 'COVID19_Tweets_Dataset\Summary_Sentiment'
    #feartherer(path)