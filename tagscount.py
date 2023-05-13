import pandas as pd
import os
import multiprocessing as mp

wd = os.getcwd()

def process_tag(files, output_dir):
    tag_counts = pd.DataFrame()
    for file in files:
        if file.endswith(".fea"):
            df = pd.read_feather(file)
            df['Hashtag'] = df['Hashtag'].str.lower()
            grouped = df.groupby("Hashtag", as_index=False).size()
            df1 = pd.concat([tag_counts, grouped], ignore_index=True)
            tag_counts = df1.groupby("Hashtag", as_index=False).sum()  
    filename = os.path.join(output_dir, os.path.basename(file))
    tag_counts.to_feather(filename)

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
    tag_counts = pd.DataFrame()
    for file in os.listdir(output_dir):
        if file.endswith(".fea"):
            filename = os.path.join(output_dir, file)
            df = pd.read_feather(filename)
            tag_counts = pd.concat([tag_counts, df], ignore_index=True)
            tag_counts = tag_counts.groupby(["Hashtag"],as_index=False).sum()
    for file in os.listdir(output_dir):
        filename = os.path.join(output_dir, file)
        os.remove(filename)                 #clear the trash
    sorted_tags= tag_counts.sort_values(by=["size"], ascending=False).reset_index(drop=True)
    print(sorted_tags)
    return sorted_tags





if  __name__ == '__main__':
    
    path = "Counts\\tags_count_all.fea"
    sorted_tag = tag_count("temp")
    print(sorted_tag[0:5])
    sorted_tag.to_feather(path)