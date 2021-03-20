

import pandas as pd
import numpy as np
import datetime as dt
from dateutil import tz
import re
from pmaw import PushshiftAPI


api = PushshiftAPI()


#before = int(dt.datetime(2021,2,1,0,0).timestamp())
#after = int(dt.datetime(2020,12,1,0,0).timestamp())
#before = int(dt.datetime(2019,7,1,0,0).timestamp())
#after = int(dt.datetime(2019,1,1,0,0).timestamp())
#before = int(dt.datetime(2020,1,1,0,0).timestamp())
#after = int(dt.datetime(2018,12,31,0,0).timestamp())
before = int(dt.datetime(2021,1,28,0,0).timestamp())
after = int(dt.datetime(2021,1,21,0,0).timestamp())
#after = int(dt.datetime(2021,2,28,0,0).timestamp())

subreddit="wallstreetbets"
subs = ["wallstreetbets", "stocks", "investing", "StockMarket", "options"]
#subs = ["wallstreetbets", "stocks", "investing", "smallstreetbets"]
#limit=100000
limit=None
#from_zone = tz.gettz('UTC')
#to_zone = tz.gettz('America/New_York')


#Obtain comments to subreddit:

comments = api.search_comments(subreddit=subreddit, limit=limit, before=before, after=after)
print(f'Retrieved {len(comments)} comments from Pushshift')

#comment_list = [comment for comment in comments]
comments_df = pd.DataFrame(comments)
#comments_core_df = comments_df[['author', 'author_created_utc', 'body', 'created_utc', 'retrieved_on', 'updated_utc']]
#comments_core_df = comments_df[['author', 'author_created_utc', 'body', 'created_utc', 'retrieved_on']]
comments_core_df = comments_df[['author', 'body', 'created_utc', 'retrieved_on']]
comments_core_df['utc'] = pd.to_datetime(comments_core_df['created_utc'], unit='s')
#comments_core_df['date'] = comments_core_df['utc'].dt.tz_localize('utc').dt.tz_convert('US/Central')
comments_core_df['date'] = comments_core_df['utc'].dt.tz_localize('utc').dt.tz_convert('America/New_York')
comments_core_df['day'] = comments_core_df['date'].dt.strftime('%Y-%m-%d')


#Obtain submissions to subreddit:

posts = api.search_submissions(subreddit=subreddit, limit=limit, after=after)
print(f'Retrieved {len(posts)} posts from Pushshift')

#post_list = [post for post in posts]
post_df = pd.DataFrame(posts)
post_core_df = post_df[['created_utc', 'retrieved_on', 'selftext', 'title']]
post_core_df['utc'] = pd.to_datetime(post_core_df['created_utc'], unit='s')
#post_core_df['date'] = post_core_df['utc'].dt.tz_localize('utc').dt.tz_convert('US/Central')
post_core_df['date'] = post_core_df['utc'].dt.tz_localize('utc').dt.tz_convert('America/New_York')
post_core_df['day'] = post_core_df['date'].dt.strftime('%Y-%m-%d')
post_core_df['selftext'] = post_core_df['selftext'].apply(str)


#Obtain a list of all tickers:

def get_stock_list():
    ticker_dict = {}
    filelist = ["input/list1.csv", "input/list2.csv", "input/list3.csv"]
    for file in filelist:
        tl = pd.read_csv(file, skiprows=0, skip_blank_lines=True)
        tl = tl[tl.columns[0]].tolist()
        for ticker in tl:
            ticker_dict[ticker] = 1
    return ticker_dict

#Obtain a list of all put/call comments/posts:

#\W*(((?i))call)\W*
#\W*((?i)put(?-i))\W*

def put_list(source, column):
    putlist = []
    for i in range(0, len(source[column])):
        match = re.findall(r'\W*((?i)put)\W*', source[column][i])
        if len(match) > 0:
            putlist.append(1)
        else:
            putlist.append(0)
    return putlist

def call_list(source, column):
    calllist = []
    for i in range(0, len(source[column])):
        match = re.findall(r'\W*((?i)call)\W*', source[column][i])
        if len(match) > 0:
            calllist.append(1)
        else:
            calllist.append(0)
    return calllist

#Version 1:

def get_tickers():

    blacklist = [
        "YOLO", "TOS", "CEO", "CFO", "CTO", "DD", "BTFD", "WSB", "OK", "RH",
        "KYS", "FD", "TYS", "US", "USA", "IT", "ATH", "RIP", "GDP",
        "OTM", "ATM", "ITM", "IMO", "LOL", "DOJ", "BE", "PR", "PC", "ICE",
        "TYS", "ISIS", "PRAY", "PT", "FBI", "SEC", "GOD", "NOT", "POS", "COD",
        "AYYMD", "FOMO", "TL;DR", "EDIT", "STILL", "LGMA", "WTF", "RAW", "PM",
        "LMAO", "LMFAO", "ROFL", "EZ", "RED", "BEZOS", "TICK", "IS", "DOW",
        "AM", "PM", "LPT", "GOAT", "FL", "CA", "IL", "PDFUA", "MACD", "HQ",
        "OP", "DJIA", "PS", "AH", "TL", "DR", "JAN", "FEB", "JUL", "AUG",
        "SEP", "SEPT", "OCT", "NOV", "DEC", "FDA", "IV", "ER", "IPO", "RISE",
        "IPA", "URL", "MILF", "BUT", "SSN", "FIFA", "USD", "CPU", "AT",
        "GG", "ELON", "MOON", "HAND", "GTFO"
    ]

    regexPattern = r'\b([A-Z]+)\b'
    tickerDict = get_stock_list()
    days = list(comments_core_df['day'].unique())

    mentions = pd.DataFrame(columns = ['ticker', 'mentions', 'date'])

    for day in days:

        dailyTickers = {}

        comdf = comments_core_df[comments_core_df['day'] == day]

        #comments_list = comments_core_df['body'].list()
        for comment in list(comdf['body']):
            for phrase in re.findall(regexPattern, comment):
                if phrase not in blacklist:
                    if phrase in tickerDict:
                        if phrase not in dailyTickers:
                            dailyTickers[phrase] = 1
                        else:
                            dailyTickers[phrase] += 1

        dailymentions = pd.DataFrame(pd.Series(dailyTickers)).reset_index()
        dailymentions.columns = ['ticker', 'mentions']
        dailymentions['date'] = day

        mentions = mentions.append(dailymentions)

    return mentions

def get_comm_tickers(source):

    blacklist = [
        "YOLO", "TOS", "CEO", "CFO", "CTO", "DD", "BTFD", "WSB", "OK", "RH",
        "KYS", "FD", "TYS", "US", "USA", "IT", "ATH", "RIP", "GDP",
        "OTM", "ATM", "ITM", "IMO", "LOL", "DOJ", "BE", "PR", "PC", "ICE",
        "TYS", "ISIS", "PRAY", "PT", "FBI", "SEC", "GOD", "NOT", "POS", "COD",
        "AYYMD", "FOMO", "TL;DR", "EDIT", "STILL", "LGMA", "WTF", "RAW", "PM",
        "LMAO", "LMFAO", "ROFL", "EZ", "RED", "BEZOS", "TICK", "IS", "DOW",
        "AM", "PM", "LPT", "GOAT", "FL", "CA", "IL", "PDFUA", "MACD", "HQ",
        "OP", "DJIA", "PS", "AH", "TL", "DR", "JAN", "FEB", "JUL", "AUG",
        "SEP", "SEPT", "OCT", "NOV", "DEC", "FDA", "IV", "ER", "IPO", "RISE",
        "IPA", "URL", "MILF", "BUT", "SSN", "FIFA", "USD", "CPU", "AT",
        "GG", "ELON", "MOON", "HAND", "GTFO"
    ]

    regexPattern = r'\b([A-Z]+)\b'
    tickerDict = get_stock_list()
    days = list(source['day'].unique())

    mentions = pd.DataFrame(columns = ['ticker', 'mentions', 'date'])

    for day in days:

        dailyTickers = {}

        comdf = source[source['day'] == day]

        #comments_list = source['body'].list()
        for comment in list(comdf['body']):
            for phrase in re.findall(regexPattern, comment):
                if phrase not in blacklist:
                    if phrase in tickerDict:
                        if phrase not in dailyTickers:
                            dailyTickers[phrase] = 1
                        else:
                            dailyTickers[phrase] += 1

        dailymentions = pd.DataFrame(pd.Series(dailyTickers)).reset_index()
        dailymentions.columns = ['ticker', 'mentions']
        dailymentions['date'] = day

        mentions = mentions.append(dailymentions)

    return mentions

def get_post_tickers(source):

    source = post_core_df

    blacklist = [
        "YOLO", "TOS", "CEO", "CFO", "CTO", "DD", "BTFD", "WSB", "OK", "RH",
        "KYS", "FD", "TYS", "US", "USA", "IT", "ATH", "RIP", "GDP",
        "OTM", "ATM", "ITM", "IMO", "LOL", "DOJ", "BE", "PR", "PC", "ICE",
        "TYS", "ISIS", "PRAY", "PT", "FBI", "SEC", "GOD", "NOT", "POS", "COD",
        "AYYMD", "FOMO", "TL;DR", "EDIT", "STILL", "LGMA", "WTF", "RAW", "PM",
        "LMAO", "LMFAO", "ROFL", "EZ", "RED", "BEZOS", "TICK", "IS", "DOW",
        "AM", "PM", "LPT", "GOAT", "FL", "CA", "IL", "PDFUA", "MACD", "HQ",
        "OP", "DJIA", "PS", "AH", "TL", "DR", "JAN", "FEB", "JUL", "AUG",
        "SEP", "SEPT", "OCT", "NOV", "DEC", "FDA", "IV", "ER", "IPO", "RISE",
        "IPA", "URL", "MILF", "BUT", "SSN", "FIFA", "USD", "CPU", "AT",
        "GG", "ELON", "MOON", "HAND", "GTFO"
    ]

    regexPattern = r'\b([A-Z]+)\b'
    tickerDict = get_stock_list()
    days = list(source['day'].unique())

    mentions = pd.DataFrame(columns = ['ticker', 'mentions', 'date'])

    for day in days:

        dailyTickers = {}

        postdf = source[source['day'] == day]

        #comments_list = comments_core_df['body'].list()
        for post in list(postdf['title']):
            for phrase in re.findall(regexPattern, post):
                if phrase not in blacklist:
                    if phrase in tickerDict:
                        #print(phrase)
                        if phrase not in dailyTickers:
                            dailyTickers[phrase] = 1
                        else:
                            dailyTickers[phrase] += 1

        for post in list(postdf['selftext']):
            if post != '':
                for phrase in re.findall(regexPattern, post):
                    if phrase not in blacklist:
                        if phrase in tickerDict:
                            #print(phrase)
                            if phrase not in dailyTickers:
                                dailyTickers[phrase] = 1
                            else:
                                dailyTickers[phrase] += 1

        dailymentions = pd.DataFrame(pd.Series(dailyTickers)).reset_index()
        dailymentions.columns = ['ticker', 'mentions']
        dailymentions['date'] = day

        mentions = mentions.append(dailymentions)

    return mentions

mentions_cm_df = get_comm_tickers(comments_core_df)
mentions_ps_df = get_post_tickers(post_core_df)

mentions_cm_df.rename(columns={'mentions':'mentions_comments'}, inplace=True)
mentions_ps_df.rename(columns={'mentions':'mentions_posts'}, inplace=True)

mentions_cm_ps = pd.merge(mentions_cm_df, mentions_ps_df, on=['ticker', 'date'], how='outer')
mentions_cm_ps['mentions_comments'].replace(np.nan, 0, inplace=True)
mentions_cm_ps['mentions_posts'].replace(np.nan, 0, inplace=True)
mentions_cm_ps['mentions'] = mentions_cm_ps['mentions_comments'] + mentions_cm_ps['mentions_posts']

#Daily data:

mentions = ['mentions', 'mentions_comments', 'mentions_posts']

mentions_cm_ps.sort_values(['ticker', 'date'], inplace=True)
mentions_cm_ps['date'] = pd.to_datetime(mentions_cm_ps['date'])

mentions_cm_ps.set_index('date', inplace=True)
mentions_daily = mentions_cm_ps[['ticker', 'mentions_comments', 'mentions_posts', 'mentions']].groupby('ticker')\
    .resample('D').mean().reset_index()

for word in mentions:
    mentions_daily[word].replace(np.nan, 0, inplace=True)
    mentions_daily[word+'_last'] = mentions_daily.groupby('ticker')[word].shift(1)
    mentions_daily[word+'_7days'] = 0
    for m in range(1, 7):
        mentions_daily.sort_values(['ticker', 'date'], inplace=True)
        mentions_daily['mt_lag'] = mentions_daily.groupby('ticker')[word].shift(m)
        mentions_daily[word+'_7days'] = mentions_daily[word+'_7days'] + mentions_daily['mt_lag']
    mentions_daily[word+'_7days_av'] = mentions_daily[word+'_7days']/7

mentions_daily = mentions_daily[['ticker', 'date', 'mentions_comments', 'mentions_posts',  'mentions',
                                'mentions_last', 'mentions_7days', 'mentions_7days_av', 'mentions_comments_last',
                                'mentions_comments_7days', 'mentions_comments_7days_av', 'mentions_posts_last',
                                'mentions_posts_7days', 'mentions_posts_7days_av']]

mentions_daily.to_csv('mentions_daily.csv', index=False)

#Weekly data:

#mentions_daily['date'] = pd.to_datetime(mentions_daily['date'])
mentions_daily['week_id'] = mentions_daily['date'].dt.strftime('%Y-%W')
mentions_daily['week_day'] = mentions_daily['date'].dt.strftime('%w')

mt_weekly = mentions_daily.groupby(['ticker', 'week_id'])[mentions].sum().reset_index()
for word in mentions:
    mt_weekly[word].replace(np.nan, 0, inplace=True)
    mt_weekly[word+'_last'] = mt_weekly.groupby('ticker')[word].shift(1)

mt_weekly_1 = mentions_daily[mentions_daily['week_day'] == '1']
mt_weekly_1 = mt_weekly_1[['ticker', 'week_id', 'date', 'mentions_7days_av',
                        'mentions_comments_7days_av', 'mentions_posts_7days_av']]

mt_weekly = pd.merge(mt_weekly, mt_weekly_1, on=['ticker', 'week_id'], how='outer')

mt_weekly.to_csv('mentions_weekly.csv', index=False)

#Monthly data:

#mentions_daily['date'] = pd.to_datetime(mentions_daily['date'])
mentions_daily['month_id'] = mentions_daily['date'].dt.strftime('%Y-%m')

mt_monthly = mentions_daily.groupby(['ticker', 'month_id'])[mentions].sum().reset_index()
mt_monthly_1 = mentions_daily.groupby(['ticker', 'month_id'])[mentions].mean().reset_index()

for word in mentions:
    mt_monthly[word].replace(np.nan, 0, inplace=True)
    mt_monthly[word+'_last'] = mt_monthly.groupby('ticker')[word].shift(1)

    mt_monthly_1[word].replace(np.nan, 0, inplace=True)
    mt_monthly_1[word+'_last'] = mt_monthly_1.groupby('ticker')[word].shift(1)

mt_monthly = pd.merge(mt_monthly, mt_monthly_1, on=['ticker', 'month_id'], how='outer')

mt_monthly.to_csv('mentions_monthly.csv', index=False)

#Puts vs. Calls:

put_comments = put_list(comments_core_df, 'body')
call_comments = call_list(comments_core_df, 'body')
put_posts = put_list(post_core_df, 'title')
call_posts = call_list(post_core_df, 'title')

comments_core_df['put'] = pd.Series(put_comments)
comments_core_df['call'] = pd.Series(call_comments)
post_core_df['put'] = pd.Series(put_posts)
post_core_df['call'] = pd.Series(call_posts)

comments_puts = comments_core_df[comments_core_df['put']==1]
comments_calls = comments_core_df[comments_core_df['call']==1]
post_puts = post_core_df[post_core_df['put']==1]
post_calls = post_core_df[post_core_df['call']==1]

def get_daily(comments, posts):
    mentions_cm_df = get_comm_tickers(comments)
    mentions_ps_df = get_post_tickers(posts)

    mentions_cm_df.rename(columns={'mentions': 'mentions_comments'}, inplace=True)
    mentions_ps_df.rename(columns={'mentions': 'mentions_posts'}, inplace=True)

    mentions_cm_ps = pd.merge(mentions_cm_df, mentions_ps_df, on=['ticker', 'date'], how='outer')
    mentions_cm_ps['mentions_comments'].replace(np.nan, 0, inplace=True)
    mentions_cm_ps['mentions_posts'].replace(np.nan, 0, inplace=True)
    mentions_cm_ps['mentions'] = mentions_cm_ps['mentions_comments'] + mentions_cm_ps['mentions_posts']

    mentions = ['mentions', 'mentions_comments', 'mentions_posts']

    mentions_cm_ps.sort_values(['ticker', 'date'], inplace=True)
    mentions_cm_ps['date'] = pd.to_datetime(mentions_cm_ps['date'])

    mentions_cm_ps.set_index('date', inplace=True)
    mentions_daily = mentions_cm_ps[['ticker', 'mentions_comments', 'mentions_posts', 'mentions']].groupby('ticker') \
        .resample('D').mean().reset_index()

    return mentions_daily


#Version2 via $:

def extract_ticker(body, start_index):
   """
   Given a starting index and text, this will extract the ticker, return None if it is incorrectly formatted.
   """
   count  = 0
   ticker = ""

   for char in body[start_index:]:
      # if it should return
      if not char.isalpha():
         # if there aren't any letters following the $
         if (count == 0):
            return None

         return ticker.upper()
      else:
         ticker += char
         count += 1

   return ticker.upper()

def parse_tickers():

    blacklist = [
        "YOLO", "TOS", "CEO", "CFO", "CTO", "DD", "BTFD", "WSB", "OK", "RH",
        "KYS", "FD", "TYS", "US", "USA", "IT", "ATH", "RIP", "GDP",
        "OTM", "ATM", "ITM", "IMO", "LOL", "DOJ", "BE", "PR", "PC", "ICE",
        "TYS", "ISIS", "PRAY", "PT", "FBI", "SEC", "GOD", "NOT", "POS", "COD",
        "AYYMD", "FOMO", "TL;DR", "EDIT", "STILL", "LGMA", "WTF", "RAW", "PM",
        "LMAO", "LMFAO", "ROFL", "EZ", "RED", "BEZOS", "TICK", "IS", "DOW",
        "AM", "PM", "LPT", "GOAT", "FL", "CA", "IL", "PDFUA", "MACD", "HQ",
        "OP", "DJIA", "PS", "AH", "TL", "DR", "JAN", "FEB", "JUL", "AUG",
        "SEP", "SEPT", "OCT", "NOV", "DEC", "FDA", "IV", "ER", "IPO", "RISE",
        "IPA", "URL", "MILF", "BUT", "SSN", "FIFA", "USD", "CPU", "AT",
        "GG", "ELON", "MOON", "HAND", "GTFO"
    ]

    ticker_list = get_stock_list()
    tickerlist = list(ticker_list.keys())
    #ticker_dict = ticker_list
    ticker_dict = {}

    for body in list(comments_core_df['body']):
        if '$' in body:
            index = body.find('$') + 1
            word = extract_ticker(body, index)

            if word and word not in blacklist:
                if word in ticker_dict:
                    ticker_dict[word] += 1
                    #ticker_dict[word].count += 1
                    #ticker_dict[word].bodies.append(body)
                else:
                    #ticker_dict[word] = Ticker(word)
                    #ticker_dict[word].count = 1
                    #ticker_dict[word].bodies.append(body)
                    ticker_dict[word] = 1

        # checks for non-$ formatted comments, splits every body into list of words
        word_list = re.sub("[^\w]", " ", body).split()
        for count, word in enumerate(word_list):
            # initial screening of words
            if word.isupper() and len(word) != 1 and (word.upper() not in blacklist) and len(word) <= 5 \
                    and (word.upper() in tickerlist) and word.isalpha():
                # add/adjust value of dictionary
                if word in ticker_dict:
                    ticker_dict[word] += 1
                    # ticker_dict[word].count += 1
                    # ticker_dict[word].bodies.append(body)
                else:
                    # ticker_dict[word] = Ticker(word)
                    # ticker_dict[word].count = 1
                    # ticker_dict[word].bodies.append(body)
                    ticker_dict[word] = 1

    return ticker_dict





