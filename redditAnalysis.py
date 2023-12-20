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
    dataframe["linkCount"] = dataframe["body"].apply(countUrls)

    # add cognitive complexity of each tweet to the datafram and output results to CSV
    LIWCVars = ["Sixltr", "cause","insight","tentat","conj","discrep","certain","differ","negate"]

    for variable in LIWCVars :

        if (dataframe[variable].sum() != 0):
            dataframe["Z" + variable] = zscore(dataframe[variable])
        else:
            dataframe["Z" + variable] = 0

    dataframe["CC"] = dataframe["ZSixltr"] + dataframe["Zcause"] + dataframe["Zinsight"] \
        + dataframe["Ztentat"] + dataframe["Zconj"] - dataframe["Zcertain"] - \
        dataframe["Zdiffer"] - dataframe["Znegate"]

    dataframe.to_csv('redditWithCC.csv')

    # caculate the thread statistics
    tweetThreadAnalysis = dataframe.groupby(["link_id"]).agg(postCount=('id','count'),\
        sumWC=('WC','sum'), meanWC=('WC','mean'), meanWCStd=('WC','std'),\
        meanTox=('toxicityScore','mean'), meanToxStd=('toxicityScore','std'), meanCC=('CC','mean'), meanCCStd=('CC','std'), \
        meanAnalytic=('Analytic','mean'), meanAnalyticStd=('Analytic','std'), sumLink = ('linkCount','sum'), \
        uniqueUsers=('author_fullname','nunique'))

    # add links per post and replies per user to dataFrame
    tweetThreadAnalysis['linkPerPost'] = tweetThreadAnalysis.apply(linkPerPost, axis=1)
    tweetThreadAnalysis['replyPerUser'] = tweetThreadAnalysis.apply(replyPerUser, axis=1)

    tweetThreadAnalysis.to_csv("stats1.csv")

    # calcualte posts per user for each thread
    tweetUserAnalysis = dataframe.groupby(["link_id","author_fullname"]).agg(postCount=('id','count'))

    # caclulate the skew, standard deviation, and gini coefficient for the count of users posts for each thread
    tweetUserAnalysisByThread = tweetUserAnalysis.groupby(["link_id"]).agg(postCountSkew=('postCount','skew'),\
        postCountStd=('postCount','std'), postCountGini=('postCount',gini))

    tweetUserAnalysisByThread.to_csv("stats2.csv")

    # merge the two thread datafranes together
    mergedThreadStats = pd.merge(tweetThreadAnalysis, tweetUserAnalysisByThread, how='left', on = 'link_id')
    mergedThreadStats.to_csv("stats3.csv")


    # add the thread dates (date when first post was posted) from starter.csv (list of all parent tweets)
    parents = pd.read_csv("anonRedditJustStarters.csv")
    # drop duplicates from parents
    parents.drop_duplicates(subset=['link_id'],inplace=True)
    mergedThreadStatsWithDates = pd.merge(mergedThreadStats, parents[['link_id','createdDatetime','title','full_link','author_fullname']], how='left', on = 'link_id')
    mergedThreadStatsWithDates.to_csv("RedditThreadStats.csv", index=False)


def main():
    tweetsDf = pd.read_csv("anonRedditData.csv")
    tweetsDf.toxicityScore =pd.to_numeric(tweetsDf.toxicityScore, errors ='coerce').fillna(0).astype('float64')
    print(tweetsDf.shape)
    tweetsDf = tweetsDf[tweetsDf.author != "[deleted]"]
    print(tweetsDf.shape)

    processTweets(tweetsDf)

if __name__ == "__main__":
    main()
