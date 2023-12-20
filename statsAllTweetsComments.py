import pandas as pd
from datetime import datetime

tweetsDf = pd.read_csv("tweetsWithCC.csv")
redditDf = pd.read_csv("redditWithCC.csv")

# determine stats regardless of thread ie. all tweets in a particular day
# trying to determine if online deliberation changes as activity increases

## Twitter

tweetsDf["createdDate"] = pd.to_datetime(tweetsDf["createdDate"])
tweetsAnalysisDf = tweetsDf.drop(['conversationId', 'tweetText'], axis=1)

tweetsAnalysisDay = tweetsAnalysisDf.groupby(by=pd.Grouper(freq='D', key='createdDate')).agg(postCount=('id','count'),\
    meanMeanTox=('toxicityScore','mean'), meanMeanCC=('CC','mean') )

tweetsAnalysisDay.to_csv('meanAllTwitterDay.csv')

tweetsAnalysisDayCora = tweetsAnalysisDay.corr(method='pearson')
print(tweetsAnalysisDayCora.head())

tweetsAnalysisDayCora.to_csv('meanAllTwitterDayCora.csv')

## Reddit

redditDf.created_utc = pd.to_numeric(redditDf.created_utc,errors="coerce")
redditDf["createdDatetime"] = redditDf["created_utc"].apply(lambda t: datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S"))

#----

redditDf["createdDatetime"] = pd.to_datetime(redditDf["createdDatetime"])

redditAnalysisDay = redditDf.groupby(by=pd.Grouper(freq='D', key='createdDatetime')).agg(postCount=('id','count'),\
    meanMeanTox=('toxicityScore','mean'), meanMeanCC=('CC','mean') )

redditAnalysisDay.to_csv('meanAllRedditDay.csv')

redditAnalysisDayCora = redditAnalysisDay.corr(method='pearson')
print(redditAnalysisDayCora.head())

redditAnalysisDayCora.to_csv('meanAllRedditDayCora.csv')
