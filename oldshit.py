#Store old but "functioning" implementations just in case

'''
def like_counts(id_dict):
    #path shit here
    hashtag_path = 'COVID19_Tweets_Dataset\Summary_Details'
    path = os.path.join(wd, hashtag_path)
    hashtag_likes = {}
    hashtag_retweets = {}
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):      #only read .csv files
                
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                data = pd.read_csv(relative_path)
                
                for key, value in id_dict.items():
                    df = data.query(value)
                    for i, row in df:
                        likes = row['Likes']
                        retweets = row['Retweets'] 
                        try:
                                hashtag_likes[key] = hashtag_likes[key] + likes
                        except KeyError:
                            hashtag_likes[key] = likes
                        try:
                            hashtag_retweets[key] = hashtag_retweets[key] + retweets
                        except KeyError:
                            hashtag_retweets[key] = retweets
                    
                
                for index, row in data.iterrows():
                    id = row["Tweet_ID"]
                    likes = row['Likes']
                    retweets = row['Retweets']

                    #try implement
                    for key, value in id_dict.items():
                        if id in value:
                            try:
                                hashtag_likes[key] = hashtag_likes[key] + likes
                            except KeyError:
                                hashtag_likes[key] = likes
                            try:
                                hashtag_retweets[key] = hashtag_retweets[key] + retweets
                            except KeyError:
                                hashtag_retweets[key] = retweets

    sorted_like_counts = sorted(hashtag_likes.items(), key=lambda x:x[1], reverse=True)
    sorted_retweet_counts = sorted(hashtag_retweets.items(), key=lambda x:x[1], reverse=True)

    return sorted_like_counts[0:5], sorted_retweet_counts[0:5]    
'''
'''
def weekly_likes(tag, df):
    #weekly progress plots for thing a
    #group test
    #path shit here
    hashtag_path = 'COVID19_Tweets_Dataset\Summary_Details'
    path = os.path.join(wd, hashtag_path)
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                data = pd.read_csv(relative_path)
                grouped = df.groupby(['week', 'year'])
'''

'''
def make_hastag_df(id_dict):
    print("Making dataframes")
    flocation ="dir_parts/"
    hastag_dataframes = {}
    #path shit here
    hashtag_path = 'COVID19_Tweets_Dataset\Summary_Details'
    path = os.path.join(wd, hashtag_path)
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):
                
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                data = pd.read_csv(relative_path)
                for key, value in id_dict.items():
                    df = data[data['Tweet_ID'].isin(value)]
                    if (df.size > 0): # no hits skip
                        subset = df[["Tweet_ID","Likes","Retweets","week","year"]]
                        try:
                            hastag_dataframes[key] = pd.concat([hastag_dataframes[key],subset], ignore_index=True)
                        except KeyError:
                            hastag_dataframes[key] = subset
                
    save_path ="df_dict.pickle"
    with open(save_path, 'wb') as f:
        pickle.dump(hastag_dataframes, f)
    return hastag_dataframes

'''

''' FROM IDS'''
'''
def hashtag_count():
    Goes over the csv files in the folder and creates dictionary containing hashtags and how many times they were found,
    also creates dictionary that contains hashtags as keys and a list of ids of the tweets where the tag appeared.

    print('Counting hastags')
    hashtag_path = 'COVID19_Tweets_Dataset\Summary_Hashtag'
    path = os.path.join(wd, hashtag_path)
    hashtag_counts = {}
    hashtag_ids = {}

    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):      #only read .csv files
                
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                data = pd.read_csv(relative_path, encoding='latin-1')

                for index, row in data.iterrows():
                    id = row["Tweet_ID"]
                    try:
                        tag = row['Hashtag']
                    except KeyError:
                        tag = row['Hastag'] #they have typo
                    
                    try:
                        hashtag_counts[tag.lower()] = hashtag_counts[tag.lower()] + 1 #hashtags are not case sensitive
                    except KeyError:
                        hashtag_counts[tag.lower()] = 1
                    except AttributeError:
                        pass #not interested in Nan

                    #picking up ids when we go through the tweets first time so there is no need to return to these files
                    try:
                        hashtag_ids[tag.lower()].append(id) 
                    except KeyError:
                        hashtag_ids[tag.lower()] = [id]
                    except AttributeError:
                        pass #not interested in Nan

    sorted_hashtag_counts = sorted(hashtag_counts.items(), key=lambda x:x[1], reverse=True)
    return sorted_hashtag_counts, hashtag_ids

def ner_count():
    ner_path = 'COVID19_Tweets_Dataset\Summary_NER'
    path = os.path.join(wd, ner_path)
    ner_counts = {}

    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):      #only read .csv files
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                data = pd.read_csv(relative_path, encoding='latin-1')

                for ner in data['NER_Text']:
                    try:
                        ner_counts[ner.lower()] = ner_counts[ner.lower()] + 1 #avoiding case sensitivities
                    except KeyError:
                        ner_counts[ner.lower()] = 1
                    except AttributeError:
                       pass  #not interested in Nan

    sorted_ner_counts = sorted(ner_counts.items(), key=lambda x:x[1], reverse=True)
    return sorted_ner_counts

'''

'''
def process_files(files):
    hashtag_counts = {}
    hashtag_ids = {}

    for file in files:
        if file.endswith(".csv"):
            data = pd.read_csv(file, encoding='latin-1')
            print('Processing:', file)
            for index, row in data.iterrows():
                id = row["Tweet_ID"]
                try:
                    tag = row['Hashtag']
                except KeyError:
                    tag = row['Hastag'] #they have typo

                try:
                    hashtag_counts[tag.lower()] = hashtag_counts[tag.lower()] + 1 #hashtags are not case sensitive
                except KeyError:
                    hashtag_counts[tag.lower()] = 1
                except AttributeError:
                    pass #not interested in Nan

                #picking up ids when we go through the tweets first time so there is no need to return to these files
                try:
                    hashtag_ids[tag.lower()].append(id) 
                except KeyError:
                    hashtag_ids[tag.lower()] = [id]
                except AttributeError:
                    pass #not interested in Nan

    return hashtag_counts, hashtag_ids

def hashtag_count():
    
    #Goes over the csv files in the folder and creates dictionary containing hashtags and how many times they were found,
    #also creates dictionary that contains hashtags as keys and a list of ids of the tweets where the tag appeared.
    
    print('Counting hastags')
    hashtag_path = 'COVID19_Tweets_Dataset\Summary_Hashtag'
    path = os.path.join(wd, hashtag_path)
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

    results = []
    for i in range(num_processes):
        start = i * chunk_size
        end = start + chunk_size
        if i == num_processes - 1:
            end = len(all_files)
        results.append(pool.apply_async(process_files, args=(all_files[start:end],)))

    hashtag_counts = {}
    hashtag_ids = {}
    i=0
    for r in results: 
        i+=1
        print('Processing results: ',i)
        counts, ids = r.get()
        for tag, count in counts.items():
            try:
                hashtag_counts[tag] += count
            except KeyError:
                hashtag_counts[tag] = count
        for tag, ids_list in ids.items():
            try:
                hashtag_ids[tag].extend(ids_list)
            except KeyError:
                hashtag_ids[tag] = ids_list

    sorted_hashtag_counts = sorted(hashtag_counts.items(), key=lambda x:x[1], reverse=True)
    return sorted_hashtag_counts, hashtag_ids
'''