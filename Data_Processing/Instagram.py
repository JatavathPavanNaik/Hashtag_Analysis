import pandas as pd
import numpy as np
import base64
import json
from haralyzer import HarParser
import openpyxl
from datetime import *

class Instagram():
    def __init__(self, Har_file_path,input_file,output_file, start_date, end_date, hashtag_list):
        self.input_file = input_file
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.hashtag_list = hashtag_list
        self.output_file=output_file
        self.Har_file_path=Har_file_path

    def Harfile_Analysis(self):
        har_parser = HarParser.from_file(self.Har_file_path)
        har_data = har_parser.har_data

        req_api = "https://www.instagram.com/api/v1/tags/web_info/?tag_name"

        required_entries = []
        for entry in har_data["entries"]:
            if req_api in entry["request"]["url"] and entry["request"]["method"] == "GET":
                try:
                    decoded_data = base64.b64decode(entry["response"]["content"]["text"]).decode('utf-8')
                    json_data = json.loads(decoded_data)
                    required_entries.append(json_data)
                except:
                    continue
        data = []
        comments = []

        for entry in required_entries:
            no_of_posts = entry.get("count")
            name = entry["data"].get("name", "")
            sections = entry["data"]["top"].get("sections", [])

            for section in sections:
                medias = section.get("layout_content", {}).get("medias", [])

                for media in medias:
                    caption_data = media["media"].get("caption", {})
                    try:
                        caption = caption_data.get("text", "")
                        user_data = caption_data.get("user", {})
                        username = user_data.get("username", "")
                        full_name = user_data.get("full_name", "")
                    except Exception:
                        break

                    for comment in media.get("media", {}).get("comments", []):
                        try:
                            comments.append(comment.get("text", ""))
                        except Exception:
                            break

                    timestamp = caption_data.get("created_at")
                    date_time = datetime.utcfromtimestamp(timestamp) if timestamp else None

                    likes = media["media"].get("like_count", 0)
                    play_count = media["media"].get("play_count", 0)
                    comment_count = media["media"].get("comment_count", 0)
                    video_duration = media["media"].get("video_duration", 0)
                    media_type = media["media"].get("media_type", "")
                    accessibility_caption = media["media"].get("accessibility_caption", "")

                    data.append({
                        "No_of_posts": no_of_posts,
                        "Name": name,
                        "Caption": caption,
                        "Username": username,
                        "Full_name": full_name,
                        "DateTime": date_time,
                        "Likes": likes,
                        "Play_count": play_count,
                        "Comment_count": comment_count,
                        "Video_duration": video_duration,
                        "Media_type": media_type,
                        "Accessibility_caption": accessibility_caption
                    })

        top_posts = pd.DataFrame(data)
        profiles = top_posts.groupby("Username")["No_of_posts"].count().reset_index().sort_values(by='No_of_posts', ascending=False)
        profiles["Iteration"] = datetime.today().date()

        return top_posts, profiles
    
    def Instaloader_analysis(self):
        df = pd.read_csv(self.input_file)
        df["post_date"] = pd.to_datetime(df["post_date"])

        df = df[(df["post_date"] > self.start_date) & (df["post_date"] < self.end_date)]
        df["post_caption"] = df["post_caption"].str.lower()

        data = []
        hashtag_page_metric = pd.DataFrame()
        for hashtag in self.hashtag_list:
            matched_rows = df[df['post_caption'].str.contains(hashtag,case=False)]

            tempdf = matched_rows.groupby(["username"])[["likes", "comments", "view_count", "is_video"]].sum().reset_index()
            tempdf['hashtag'] = hashtag
            hashtag_profile_metric = pd.concat([hashtag_page_metric, tempdf], axis=0)

            likes = matched_rows["likes"].sum()
            comments = matched_rows["comments"].sum()
            posts = matched_rows.shape[0]
            view_count = matched_rows["view_count"].sum()
            no_of_videos = matched_rows[matched_rows["is_video"] == True].shape[0]
            data.append([hashtag, posts, likes, comments, view_count, no_of_videos])

        hashtags_metrics = pd.DataFrame(data, columns=["hashtag", "Posts", "Likes", "Comments", "View_Count", "No_of_videos"])

        hashtag_profile_metric.reset_index(drop=True, inplace=True)

        df_pivot = df.pivot_table(index=['username'],
                                  aggfunc={'followers': np.median, 'likes': np.sum, 'comments': np.sum, 'post_url': 'nunique'}) \
            .rename(columns={'post_url': 'Posts', 'followers': 'Total Follower'}) \
            .reset_index()

        df_pivot['Total Engagement'] = df_pivot['likes'] + df_pivot['comments']
        df_pivot['Engagement Rate'] = (df_pivot['likes'] + df_pivot['comments']) * 100 / df_pivot['Total Follower']

        df_pivot.drop(['likes', 'comments'], axis=1, inplace=True)

        df_pivot['Average Post Per Day'] = df_pivot['Posts'] / 15
        df_pivot['Average Post Per Week'] = df_pivot['Posts'] / 2
        df_pivot['Average Engagement'] = df_pivot['Total Engagement'] / df_pivot['Posts']

        top10_video = df[df['is_video'] == True].sort_values(by=['username', 'view_count'], ascending=False).groupby('username').head(10)
        top10_video = top10_video[['username', 'post_date', 'view_count']].rename(columns={'view_count': 'Top 10 Video Views'})
        top10_video = top10_video.groupby(['username']).sum().reset_index()

        count_of_active_days = df[['username']].drop_duplicates().groupby(['username']).count().reset_index()
        count_of_active_days.rename(columns={'Date': 'Count of Active Days'}, inplace=True)

        instagram_profile_metrics = pd.merge(df_pivot, count_of_active_days, on=['username'], how='left') \
            .merge(top10_video[['username', 'Top 10 Video Views']], on=['username'], how='left')

        return hashtags_metrics, hashtag_profile_metric, instagram_profile_metrics
    
    def excel_sheet(self, hashtags_metrics,instagram_profile_metrics,profiles,top_posts):
        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            hashtags_metrics.to_excel(writer, sheet_name='Hashtag_Metrics_Instagram', index=False)
            instagram_profile_metrics.to_excel(writer, sheet_name='Profile_Metrics_Instagram', index=False)
            profiles.to_excel(writer, sheet_name='Profiles', index=False)
            top_posts.to_excel(writer, sheet_name='Top Posts', index=False)
    
    def run_instagram_analysis(self):
        hashtags_metrics,hashtag_profile_metric,instagram_profile_metrics=self.Instaloader_analysis()
        top_posts, profiles=self.Harfile_Analysis()
        self.excel_sheet(hashtags_metrics,instagram_profile_metrics,profiles,top_posts)
