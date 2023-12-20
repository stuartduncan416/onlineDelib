from googleapiclient import discovery
from googleapiclient.errors import HttpError
import json
import pandas as pd
from datetime import datetime
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

start_time = time.time()
# PUT YOUR GOOGLE PERSPECTIVE API KEY HERE
API_KEY = 'YOUR API KEY HERE'

now = datetime.now()

currentIdFileName = "RedditDoneIds_{}{}{}_{}-{}-{}.csv".format(now.year,now.month,now.day, now.hour, now.minute, now.second)

resultList = []
redoList = []
doneIds = []

def analyze(id, text):

    try:

        client = discovery.build(
          "commentanalyzer",
          "v1alpha1",
          developerKey=API_KEY,
          discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
          static_discovery=False,
        )

        analyze_request = {
          'comment': { 'text': text },
          'requestedAttributes': {'TOXICITY': {}}
        }

        response = client.comments().analyze(body=analyze_request).execute()
        toxicity = response['attributeScores']['TOXICITY']['spanScores'][0]['score']['value']
        resultTuple = (id, toxicity)
        resultList.append(resultTuple)
        doneIds.append(id)
        print("The current length of results {} - Score {}".format(len(resultList), toxicity))

    except HttpError as err:

        print("-------------------------")
        print("This is the error: {}".format(err))

        if err.resp.status == 429:
            print("Hit quota sleeping for two minutes then trying again ...")
            redoItem = [id, text]
            redoList.append(redoItem)
            time.sleep(60)
        else:
            errorString = "ERROR - {}".format(err.resp.status)
            resultTuple = (id, errorString)
            doneIds.append(id)
            resultList.append(resultTuple)

def runner(tweets):

    threads = []

    sleepCount = 0
    overallCount = 1

    with ThreadPoolExecutor(max_workers=30) as executor:

        for tweet in tweets:

            print(overallCount)

            threads.append(executor.submit(analyze, tweet[0], tweet[1]))

            if(overallCount % 200) == 0:

                with open(currentIdFileName, "w") as idFile:
                    for doneId in doneIds:
                        idFile.write("%s\n" % doneId)
                    idFile.close()

                time.sleep(10)
                print("Sleeping for 10 seconds")
                sleepCount = 0

            overallCount = overallCount + 1

            if(overallCount % 1000) == 0:

                print("Writing batch {} to csv .....".format(overallCount))

                try:
                    proccessedTweets = pd.DataFrame(resultList, columns = ['id','toxicityScore'])
                except:
                    print("An error occurred")

                joinedTweets = pd.merge(tweetsDf, proccessedTweets, how="left", on=["id"])
                joinedTweetsFilename = "joinedReddit_{}{}{}.csv".format(now.year,now.month,now.day)
                joinedTweets.to_csv(joinedTweetsFilename, index=False)

def main():

    # if no done id file leave this empty

    doneIdsFilename = ""

    # read in all tweet data
    global tweetsDf
    tweetsDf = pd.read_csv("redditAllComments.csv")

    print("Details of tweetDf {}".format(tweetsDf.shape))

    subList = tweetsDf[['id','body']]
    tweetList = subList.values.tolist()

    if(doneIdsFilename !=""):
        # read in the reddit id that have already been proccssed
        proccessedTweets = pd.read_csv(doneIdsFilename, header=None)
        proccessedTweets.rename(columns={ proccessedTweets.columns[0]: "id" }, inplace=True)
        print(proccessedTweets.head())

        merged = tweetsDf['id'].isin(proccessedTweets['id']) & ~tweetsDf['id'].duplicated()

        tweetsDf = tweetsDf[~merged]
        subList = tweetsDf[['id','tweetText']]
        tweetRemainingList = subList.values.tolist()

        runner(tweetRemainingList)
    else:
        runner(tweetList)

    proccessedTweets = pd.DataFrame(resultList, columns = ['id','toxicityScore'])
    joinedTweets = pd.merge(tweetsDf, proccessedTweets, how="left", on=["id"])

    print("The length if the final result list {}".format(len(resultList)))
    print("--- Runtime : %s seconds ---" % (time.time() - start_time))
    tweetsDf.to_csv('final_subsetReddit.csv', index=False)
    joinedTweets.to_csv('final_joinedReddit.csv', index=False)

if __name__ == "__main__":
    main()
