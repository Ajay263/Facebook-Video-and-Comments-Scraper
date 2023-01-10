from __future__ import unicode_literals
import warnings
warnings.filterwarnings('ignore')
import youtube_dl
from time import sleep
from random import randint
from facebook_scraper import *
import pandas as pd
from datetime import datetime
import logging
enable_logging(logging.DEBUG)

fbpage = 'nashtvz'
number_of_pages = 2
posts_file = 'C:/Users/Ajay/Documents/Census/' + fbpage + '_posts.csv'
comments_file = 'C:/Users/Ajay/Documents/Census/' + fbpage + '_comments.csv'
replies_file = 'C:/Users/Ajay/Documents/Census/' + fbpage + '_replies.csv'
cookies_file = 'C:/Users/Ajay/Documents/Census/facebook.com_cookies.txt'

# define pandas dataframe
df_video = pd.DataFrame(columns=['post_id', 'post_text', 'shared_text', 'time', 'video', 'video_duration_seconds',
                                 'video_height', 'video_id', 'video_quality', 'video_size_MB', 'video_watches',
                                 'video_width', 'likes', 'comments',
                                 'shares', 'post_url', 'link', 'user_id', 'username', 'user_url',
                                 'is_live', 'factcheck', 'shared_post_id', 'shared_time',
                                 'shared_user_id', 'shared_username', 'shared_post_url', 'available',
                                 'comments_full', 'reactors', 'w3_fb_url', 'reactions', 'reaction_count',
                                 'image_id', 'image_ids', 'fetched_time'])

for post in get_posts(
        fbpage,
        pages=number_of_pages,
        extra_info=True,
        options={"comments": False, "post_per_page": 300}):

    dataframe = post
    df = pd.DataFrame.from_dict(dataframe, orient='index')
    df = df.transpose()
    df_video = df_video.append(df)
df_video.to_csv(r'Scrapped_FB.csv', index=False)

# ------------------- current sle'/opt/fbscraping/data/eping behavior -------------------
#   [POSTS] @ begin iteration
#    -> [COMMENTS] @ start, sleep for 3-7 seconds
#    ----> [REPLIES] @ end, sleep for 10-40 seconds
#    -> [COMMENTS] @ end, sleep for 5-15 seconds
#   [POSTS] @ end, sleep for 37-89 seconds
# -----------------------------------------------------------------

# define pagination info
start_url = None


def handle_pagination_url(url):
    global start_url
    start_url = url


# define pandas dataframe
posts_df_ori = pd.DataFrame(columns=['username', 'time', 'likes', 'comments', 'shares', 'reactions', 'post_text'])
comments_df_ori = pd.DataFrame(
    columns=['post_id', 'commenter_name', 'comment_time', 'comment_reactors', 'replies', 'comment_text'])
replies_df_ori = pd.DataFrame(
    columns=['post_id', 'parent_comment_id', 'commenter_name', 'comment_time', 'comment_reactors', 'comment_text'])

# [ALL_POSTS] retrieve all posts
print("[", datetime.now().strftime("%x %I:%M:%S %p"), "][", fbpage, "] STARTED - Retrieving posts")
pi = 0
all_posts = get_posts(
    fbpage,
    extra_info=True,
    cookies=cookies_file,
    pages=3,
    timeout=60,
    options={
        "comments": "generator",
        "comment_start_url": start_url,
        "comment_request_url_callback": handle_pagination_url
    },
)

# [ALL_POSTS] iterate through using next() pagination
while post := next(all_posts, None):
    pi += 1
    try:
        # [POST] pandas dataframe
        print("[", datetime.now().strftime("%x %I:%M:%S %p"), "][", fbpage, "][", post["post_id"],
              "] Appending post info to 'posts_df_ori' dataframe. Post index: ", pi, " Total comments: ",
              post["comments"])
        post_dataframe = post
        post_df = pd.DataFrame.from_dict(post_dataframe, orient='index')
        post_df = post_df.transpose()
        posts_df_ori = posts_df_ori.append(post_df)

        # [COMMENT] begin loop
        ci = 0
        comments = post["comments_full"]
        for comment in comments:

            # [COMMENT] determine replies
            ci += 1
            comment["replies"] = list(comment["replies"])

            # [COMMENT] pandas dataframe - transpose and add post_id
            comment_dataframe = comment
            comment_df = pd.DataFrame.from_dict(comment_dataframe, orient='index')
            comment_df = comment_df.transpose()
            comment_df.insert(0, 'post_id', post['post_id'])

            # [COMMENT] append new object with post_id and comment* data to master
            sleepCalc = randint(3, 7)
            print("[", datetime.now().strftime("%x %I:%M:%S %p"), "][", fbpage, "][", post["post_id"],
                  "] Appending comments info to 'comments_df_ori' dataframe. Post index: ", pi, " Comment index: ", ci,
                  " Sleeping for: ", sleepCalc)
            comments_df_ori = comments_df_ori.append(comment_df)

            # [COMMENT] determine if replies exist
            if comment["replies"]:
                ri = 0
                replies = comment['replies']
                for reply in replies:
                    ri += 1

                    # [COMMENT][REPLIES] pandas dataframe - transpose and add post_id, parent_comment_id
                    reply_dataframe = reply
                    reply_df = pd.DataFrame.from_dict(reply_dataframe, orient='index')
                    reply_df = reply_df.transpose()
                    reply_df.insert(0, 'post_id', post['post_id'])
                    reply_df.insert(1, 'parent_comment_id', comment['comment_id'])

                    # [COMMENT][REPLIES] append new object with post_id, parent_comment_id, and comment* data to master, sleep
                    sleepCalc = randint(10, 40)
                    print("[", datetime.now().strftime("%x %I:%M:%S %p"), "][", fbpage, "][", post["post_id"], "][",
                          comment['comment_id'], "] Appending replies to 'replies_df_ori' dataframe. Post index: ", pi,
                          " Comment index: ", ci, "Replies index: ", ri, " Sleeping for: ", sleepCalc)
                    replies_df_ori = replies_df_ori.append(reply_df)
                    sleep(sleepCalc)

            # [COMMENT] sleep for sleepCalc duration
            sleepCalc = randint(5, 15)
            sleep(sleepCalc)

        # [POST] increment index, sleep
        sleepCalc = randint(37, 89)
        print("---------------------------sleeping for ", sleepCalc, " seconds-----------------------------")

    except exceptions.TemporarilyBanned:
        print("Temporarily banned..... HALTING")
        break

    except Exception as err:
        print("Error... let's try continuing...?", err)

# [ALL_POSTS] finished looping through all posts
print("========================================================================")
print("-------------------FINISHED LOOPING THROUGH ALL POSTS-------------------")
print("========================================================================")

############################################################
# finish
############################################################
print("[", datetime.now().strftime("%x %I:%M:%S %p"), "][", fbpage, "] COMPLETED - Writing posts and comments to file")
posts_df_ori.to_csv(posts_file, encoding='utf-8', index=False)
comments_df_ori.to_csv(comments_file, encoding='utf-8', index=False)
replies_df_ori.to_csv(replies_file, encoding='utf-8', index=False)

posts_df_ori

URL = df_video['video']

# using   youtube_dl to download videos using the links in the dataframe


def my_hook(d):
    if d['status'] == 'finished':
        print('Done downloading, now converting ...')


ydl_opts = {
    'format': 'bestaudio/best',
    'outtmpl': '%(id)s',
    'noplaylist': True,
    'progress_hooks': [my_hook],
    'ignoreerrors': True

}

with youtube_dl.YoutubeDL(ydl_opts) as ydl:
    ydl.download(URL)






