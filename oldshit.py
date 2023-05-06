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