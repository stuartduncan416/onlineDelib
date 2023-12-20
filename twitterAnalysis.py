import pandas as pd
import re
from scipy.stats import zscore
from scipy.stats import skew

def countUrls(text):
    pattern2 = re.compile(r'https?://[^\s<>"]+|www\.[^\s<>"]')
    urls = pattern2.findall(text)
    return(len(urls))

def linkPerPost(row):
    return(row["sumLink"] / row["postCount"])

def replyPerUser(row):
    return(row["postCount"] / row["uniqueUsers"])

def gini(values):
    sorted_list = sorted(values)
    height, area = 0, 0
    for value in sorted_list:
        height += value
        area += height - value / 2.
    fair_area = height * len(values) / 2.
    return (fair_area - area) / fair_area

def processTweets(dataframe):

    # drop duplicate tweets (find out why duplicate tweets, most likely because duplicates in starter CSV)
    dataframe.drop_duplicates(subset=['id'],inplace=True)

    # add count of urls in each tweet to dataframe
    dataframe["linkCount"] = dataframe["tweetText"].apply(countUrls)

    # add cognitive complexity of each tweet to the dataframe and output results to CSV
    LIWCVars = ["Sixltr", "cause","insight","tentat","conj","discrep","certain","differ","negate"]

    for variable in LIWCVars :

        if (dataframe[variable].sum() != 0):
            dataframe["Z" + variable] = zscore(dataframe[variable])
        else:
            dataframe["Z" + variable] = 0

    dataframe["CC"] = dataframe["ZSixltr"] + dataframe["Zcause"] + dataframe["Zinsight"] \
        + dataframe["Ztentat"] + dataframe["Zconj"] - dataframe["Zcertain"] - \
        dataframe["Zdiffer"] - dataframe["Znegate"]

    dataframe.to_csv('tweetsWithCC.csv')

    # caculate the thread statistics
    tweetThreadAnalysis = dataframe.groupby(["conversationId"]).agg(postCount=('id','count'),\
        sumWC=('WC','sum'), meanWC=('WC','mean'), meanWCStd=('WC','std'),\
        meanTox=('toxicityScore','mean'), meanToxStd=('toxicityScore','std'), meanCC=('CC','mean'), meanCCStd=('CC','std'), \
        meanAnalytic=('Analytic','mean'), meanAnalyticStd=('Analytic','std'), sumLink = ('linkCount','sum'), \
        uniqueUsers=('userid','nunique'))

    # add links per post and replies per user to dataFrame
    tweetThreadAnalysis['linkPerPost'] = tweetThreadAnalysis.apply(linkPerPost, axis=1)
    # tweetThreadAnalysis['postPerUser'] = tweetThreadAnalysis.apply(postPerUser, axis=1)
    tweetThreadAnalysis['replyPerUser'] = tweetThreadAnalysis.apply(replyPerUser, axis=1)

    tweetThreadAnalysis.to_csv("stats1.csv")

    # calcualte posts per user for each thread
    tweetUserAnalysis = dataframe.groupby(["conversationId","userid"]).agg(postCount=('id','count'))

    # caclulate the skew, standard deviation, and gini coefficient for the count of users posts for each thread
    tweetUserAnalysisByThread = tweetUserAnalysis.groupby(["conversationId"]).agg(postCountSkew=('postCount','skew'),\
        postCountStd=('postCount','std'), postCountGini=('postCount',gini))

    tweetUserAnalysisByThread.to_csv("stats2.csv")

    # merge the two thread datafranes together
    mergedThreadStats = pd.merge(tweetThreadAnalysis, tweetUserAnalysisByThread, how='left', on = 'conversationId')
    mergedThreadStats.to_csv("stats3.csv")

    # add the thread dates (date when first post was posted) from starter.csv (list of all parent tweets)
    parents = pd.read_csv("anonJustStartersTweet.csv")

    # drop duplicates from parents
    parents.drop_duplicates(subset=['id'],inplace=True)
    mergedThreadStatsWithDates = pd.merge(mergedThreadStats, parents[['conversationId','createdDate','tweetText','retweet','like','quotes']], how='left', on = 'conversationId')

    mergedThreadStatsWithDates.to_csv("twitterThreadStats.csv", index=False)


def main():
    tweetsDf = pd.read_csv("anonTweetData.csv")
    tweetsDf.toxicityScore =pd.to_numeric(tweetsDf.toxicityScore, errors ='coerce').fillna(0).astype('float64')
    processTweets(tweetsDf)

if __name__ == "__main__":
    main()
