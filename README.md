# Towards a Computational Mixed Methods Framework to Measure Online Deliberative Discourse

This repository contains more background information on the programmatic approach behind our computational method for measuring social media-based deliberative discourse. In our soon to be published-paper we test this method using a case study of Twitter and Reddit commentary surrounding the Canadian convoy protests of COVID-19 vaccine mandates and restrictions. 

For more details on this project please reach out for a pre-print version of this forthcoming paper. This research, and the open-source ancillary material that supports it, works to encourage other researchers to test and refine our method using other sources of online deliberative discourse. 

A prevailing challenge surrounding reproducing our approach is that both Twitter and Reddit have put forth restrictions to research access to platform data since this project was started. 

Twitter data for this project was acquired using the now closed Twitter Academic Research API. 

Reddit data for this project was acquired using the Pushshift Reddit API, which no longer has the same access to a complete Reddit archive, due to changes to Reddit API access. 

Despite these data access issues, we hope that by describing the process of how we calculated our measures of deliberative discourse and sharing the Python code employed throughout this project, will serve as resources that allows other researchers to adapt, critique and refine our methods using other sources of deliberative discourse. 

This project uses the Python programming language, The Google Perspective API to measure comment toxicity, and the LIWC2015 software to calculate linguistic features of social media commentary. 

In broad strokes these are the steps we used to complete this work:

- Search social media platforms for relevant content and output results to a CSV file
  - In the case of Twitter this was done using Twarc to access the Twitter Academic API
  - In the case of Reddit the platform was done using the PMAW Python library
- Run LIWC2015 on social media platform data and output results to a CSV file
- Run the social media content through the Google Perspective API, getting a toxicity measure for each social media comment (pespectiveMultiThread.py)
- Using Custom Python scripts sort these Twitter and Reddit messages into threads and calculate the outlined measures at a thread level (twitterAnalysis.py and redditAnalysis.py)
- Using Custom Python scripts calculate aggregate statistics for all threads across Twitter and Reddit (statsThread.py)
- Using Custom Python scripts calculate statistics based on date social media post was created regardless of thread (statsAllTweetComments.py)

This repository contains four anonymized sample csv data files which should provide a sense of the formatting of the original Twitter and Reddit data files. While these files consist of a sample of 100 records, original analysis was done using the provided scripts on 2.8 million tweets and over 500,000 Reddit comments. 

