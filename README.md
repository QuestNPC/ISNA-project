This is repo for course work for course Introduction to network analysis.

The project analyses covid19 tweet dataset found at https://github.com/lopezbec/COVID19_Tweets_Dataset

The reposity itself will not contain the data.

The repository needs empty folders called 'temp' 'Counts' 'combo' 'weekly' to work

The dataset should be place on a folder called 'COVID19_Tweets_Dataset' and should initially contain only the .csv data files in their original folder structure/division.

run the scrips in order:
stripuseless.py first
tagscount.py to get number how many times each hashtag appears
nercount to get number how many times each named entity appears
powerlaw.py to fit power law on previous results
dataframe_combiner.py to combine data from several files
get_retweets_likes_hashtags.py to get top ahshtags by likes and retweets
find_hashtags.py to fetch and isolate data of set of hashtags into their own files for each hashtag
over_time.py files and sentiment.py can be run in any order to get the respective statistics calculated to .xlsx files and make plot out of them
in diffusion.py you can set a hashtag with list of 3 (week,year) time period lists to calculate the diffusions for the periods, you have to have made seaparate file for the hashtag presiously (it is assument for other steps as well after find_hashtags.py)
