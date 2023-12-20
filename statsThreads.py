import pandas as pd
from scipy.stats import pearsonr

tweetsDf = pd.read_csv("TwitterThreadStats.csv")
redditDf = pd.read_csv("RedditThreadStats.csv")

# correlations for twitter and Reddit threads

# correlations and p-values for twitter threads
tweetsNoIdDf = tweetsDf.drop('conversationId', axis=1)
tweetsCora, tweetsPValues = tweetsNoIdDf.corr(method=lambda x, y: pearsonr(x, y)[0]), tweetsNoIdDf.corr(method=lambda x, y: pearsonr(x, y)[1])

tweetsCora.to_csv('threadTweetsCora.csv')
tweetsPValues.to_csv('threadTweetsPValues.csv', float_format='%.6f')  # Set the desired precision

# correlations and p-values for Reddit threads
redditCora, redditPValues = redditDf.corr(method=lambda x, y: pearsonr(x, y)[0]), redditDf.corr(method=lambda x, y: pearsonr(x, y)[1])

redditCora.to_csv('threadRedditCora.csv')
redditPValues.to_csv('threadRedditPValues.csv', float_format='%.6f')  # Set the desired precision

## Reddit

redditDf["createdDatetime"] = pd.to_datetime(redditDf["createdDatetime"])

# find out the averages of all the threads on each day
redditAnalysisDay = redditDf.groupby(by=pd.Grouper(freq='D', key='createdDatetime')).agg(postCountSum=('postCount','sum'),\
    meanMeanTox=('meanTox','mean'), meanMeanCC=('meanCC','mean') )
redditAnalysisDay.to_csv('meanThreadRedditDay.csv')

redditAnalysisDayCora = redditAnalysisDay.corr(method='pearson')
print(redditAnalysisDayCora.head())
redditAnalysisDayCora.to_csv('meanThreadRedditDayCora.csv')

# means for all threads during entire time period

redditMeansAllDf = redditDf.mean(numeric_only=True)
redditMeansAllDf.to_csv('meanThreadRedditAll.csv')

redditStdDevAllDf = redditDf.std(numeric_only=True)
redditStdDevAllDf.to_csv('stdDevThreadRedditAll.csv')

## Twitter

# date whole thread analysis
tweetsDf["createdDate"] = pd.to_datetime(tweetsDf["createdDate"])
tweetsAnalysisDf = tweetsDf.drop(['conversationId', 'tweetText'], axis=1)

# find out the averages of all the threads on each day
tweetsAnalysisDay = tweetsAnalysisDf.groupby(by=pd.Grouper(freq='D', key='createdDate')).agg(postCountSum=('postCount','sum'),\
    meanMeanTox=('meanTox','mean'), meanMeanCC=('meanCC','mean') )

print(tweetsAnalysisDay.head())
tweetsAnalysisDay.to_csv('meanThreadTwitterDay.csv')

tweetsAnalysisDayCora = tweetsAnalysisDay.corr(method='pearson')
print(tweetsAnalysisDayCora.head())

tweetsAnalysisDayCora.to_csv('meanThreadTwitterDayCora.csv')

twitterMeansAllDf = tweetsAnalysisDf.mean(numeric_only=True)
twitterMeansAllDf.to_csv('meanThreadTwitterAll.csv')

twitterStdDevAllDf = tweetsAnalysisDf.std(numeric_only=True)
twitterStdDevAllDf.to_csv('stdDevThreadTwitterAll.csv')