import pandas as pd
import os
import multiprocessing as mp

wd = os.getcwd()

def process_tag(files, output_dir):
    ner_counts = pd.DataFrame()
    for file in files:
        if file.endswith(".fea"):
            df = pd.read_feather(file)
            df['Hashtag'] = df['Hashtag'].str.lower()
            grouped = df.groupby("Hashtag", as_index=False).count()
            df1 = pd.concat([ner_counts, grouped], ignore_index=True)
            ner_counts = df1.groupby("Hashtag", as_index=False).sum()  
    filename = os.path.join(output_dir, os.path.basename(file))
    ner_counts.to_feather(filename)

def tag_count(output_dir):

    print('Counting tags')
    hashtag_path = 'COVID19_Tweets_Dataset\Summary_Hashtag'
    path = os.path.join(wd, hashtag_path)
    all_files = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".fea"):
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                all_files.append(relative_path)

    num_processes = 10
    chunk_size = 168 #hours in a week

    pool = mp.Pool(processes=num_processes)
    
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i:i+chunk_size]
        pool.apply_async(process_tag, args=(chunk, output_dir))
    
    pool.close()
    pool.join()

    print("Combining...")
    ner_counts = pd.DataFrame()
    for file in os.listdir(output_dir):
        if file.endswith(".fea"):
            filename = os.path.join(output_dir, file)
            df = pd.read_feather(filename)
            ner_counts = pd.concat([ner_counts, df], ignore_index=True)
            ner_counts = ner_counts.groupby(["Hashtag"],as_index=False).sum()
    for file in os.listdir(output_dir):
        filename = os.path.join(output_dir, file)
        os.remove(filename)                 #clear the trash
    sorted_ner= ner_counts.sort_values(by=["Tweet_ID"], ascending=False).reset_index(drop=True)
    print(sorted_ner)
    return sorted_ner


def tag_combine_years():


    tag_counts = pd.DataFrame()
    for file in os.listdir('TAG'):
        if file.endswith(".fea"):
            filename = os.path.join('TAG', file)
            df = pd.read_feather(filename)
            df2 = pd.concat([tag_counts, df], ignore_index=True)
            tag_counts = df2.groupby(["Hashtag"],as_index=False).sum()
    
    sorted_tag= tag_counts.sort_values(by=["Tweet_ID"], ascending=False).reset_index(drop=True)
    return sorted_tag


if  __name__ == '__main__':
    
    path = "tags_count_all.fea"
    sorted_tag = tag_count("temp")
    #sorted_tag = tag_combine_years()
    print(sorted_tag[0:5])
    sorted_tag.to_feather(path)