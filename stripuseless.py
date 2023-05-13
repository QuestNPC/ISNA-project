import pandas as pd
import os
import multiprocessing as mp

wd = os.getcwd()

'''
Shit's done (for now)
'''

def thinner(path):
    path = os.path.join(wd, path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                all_files.append(relative_path)

    num_processes = 8
    chunk_size = 168

    pool = mp.Pool(processes=num_processes)
    
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i:i+chunk_size]
        pool.apply_async(remove_excess_df, args=(chunk,))
    
    pool.close()
    pool.join()
    return

def remove_excess_df(files):

    #path shit here
    for file in files:
        try:
            df = pd.read_csv(file, encoding='cp65001', low_memory=False) #some files require this encoding to be read
        except UnicodeDecodeError:
            df = pd.read_csv(file, encoding='latin-1', low_memory=False) #And some use this
        df = df[["Tweet_ID","Likes","Retweets"]]                                                 
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

    
    num_processes = 8
    chunk_size = 168

    pool = mp.Pool(processes=num_processes)
    
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i:i+chunk_size]
        pool.apply_async(changer, args=(chunk,))
    
    pool.close()
    pool.join()

def changer(files):
    #path shit here
    for path in files:
        try:
            df = pd.read_csv(path, encoding='cp65001', low_memory=False) #some files require this encoding to be read
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding='latin-1', low_memory=False) #And some use this
        os.remove(path)  
        path = path.replace(".csv", ".fea")
        df.to_feather(path)

def feartherer_ner(path):

    path = os.path.join(wd, path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                all_files.append(relative_path)

    num_processes = 8
    chunk_size = 168

    pool = mp.Pool(processes=num_processes)
    
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i:i+chunk_size]
        pool.apply_async(changer_ner, args=(chunk,))
    
    pool.close()
    pool.join()

def changer_ner(files):
    #path shit here
    for path in files:
        try:
            df = pd.read_csv(path, encoding='cp65001', low_memory=False) #some files require this encoding to be read
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding='latin-1', low_memory=False) #And some use this
        df["NER_Text"] = df["NER_Text"].str.lower()
        df = df[['Tweet_ID',"NER_Text"]]
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


    num_processes = 8
    chunk_size = 168

    pool = mp.Pool(processes=num_processes)
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i:i+chunk_size]
        pool.apply_async(fix, args=(chunk,))

    pool.close()
    pool.join()
    return

def fix(files):
    for path in files:
        try:
            df = pd.read_csv(path, encoding='cp65001', low_memory=False) #some files require this encoding to be read
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding='latin-1', low_memory=False) #And some use this
        if "Hastag" in list(df.columns.values):
            df.rename(columns = {'Hastag':'Hashtag'}, inplace = True)
        df['Hashtag'] = df['Hashtag'].str.lower()
        df = df.drop_duplicates(['Tweet_ID', 'Hashtag']).reset_index(drop=True) #lets keep the duplicates out
        os.remove(path)  
        path = path.replace(".csv", ".fea")
        try:
            df.to_feather(path)
        except Exception as e:
            print(e)

if __name__== '__main__':
    path = 'COVID19_Tweets_Dataset\Summary_Details'
    print('Details...')
    #thinner(path)
    path = 'COVID19_Tweets_Dataset\Summary_Hashtag'
    print('Tags...')
    fixer(path)
    path = 'COVID19_Tweets_Dataset\Summary_NER'
    print('NER...')
    #feartherer_ner(path)
    path = 'COVID19_Tweets_Dataset\Summary_Sentiment'
    print('Sentiment...')
    #feartherer(path)
