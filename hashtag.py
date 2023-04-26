import pandas as pd
import networkx
import os
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

wd = os.getcwd()
top_5_tags = []
top_5_ner = []

def hashtag_count():
    '''
    Goes over the csv files in the folder and creates dictionary containing hashtags and how many times they were found,
    also creates dictionary that contains hashtags as keys and a list of ids of the tweets where the tag appeared.
    '''
    
    hashtag_path = 'COVID19_Tweets_Dataset\Summary_Hashtag'
    path = os.path.join(wd, hashtag_path)
    hashtag_counts = {}
    hashtag_ids = {}

    for root,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".csv"):      #only read .csv files
                
                relative_path = os.path.relpath(os.path.join(file, root))
                relative_path = os.path.join(relative_path, file)
                data = pd.read_csv(relative_path)

                for index, row in data.iterrows():
                    id = row["Tweet_ID"]
                    tag = row['Hastag']
                    
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
    top_5_tags = sorted_hashtag_counts[0:5]
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
                data = pd.read_csv(relative_path)

                for ner in data['NER_Text']:
                    try:
                        ner_counts[ner.lower()] = ner_counts[ner.lower()] + 1 #avoiding case sensitivities
                    except KeyError:
                        ner_counts[ner.lower()] = 1
                    except AttributeError:
                       pass  #not interested in Nan

    sorted_ner_counts = sorted(ner_counts.items(), key=lambda x:x[1], reverse=True)
    top_5_ner = sorted_ner_counts[0:5]

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

                for index, row in data.iterrows():
                    id = row["Tweet_ID"]
                    likes = row['Likes']
                    retweets = row['Retweets']

                    #try implement
                    keys = [key for key, value in id_dict.items() if id in value]

                    '''
                    for tag in id_dict:
                        for tag_id in id_dict[tag]:
                            if tag_id == id:
                                try:
                                    hashtag_likes[tag] = hashtag_likes[tag] + likes
                                except KeyError:
                                    hashtag_likes[tag] = likes
                                try:
                                    hashtag_retweets[tag] = hashtag_retweets[tag] + retweets
                                except KeyError:
                                    hashtag_retweets[tag] = retweets
                                break #we can ignore rest of the ids of the tag as a match has been found, test tags need ot be checked as one tweet can have several tags
                    '''
                    
                    for key in keys:
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

if  __name__ == '__main__':
    sorted_tags, ids = hashtag_count()
    #ner_count()
    likes, retweets = like_counts(ids)
    print(likes)
    print(retweets)