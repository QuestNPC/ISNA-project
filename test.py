import pandas as pd

df = pd.read_feather('test.fea')
df1 = pd.read_feather('test2.fea')
df = df.groupby(['Hashtag'])[['Likes','Retweets']].sum()
df1 = df1.groupby(['Hashtag'])[['Likes','Retweets']].sum()
print(df)
print(df1)
df3 = pd.concat([df1, df])
print(df3)
print(df3.groupby(['Hashtag'])[['Likes','Retweets']].sum())