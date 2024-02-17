import pandas as pd
import numpy as np

class Youtube:
    def __init__(self, input_file,output_file, start_date, end_date, hashtag_list):
        self.input_file = input_file
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.hashtag_list = hashtag_list
        self.output_file=output_file

    def analyze(self, analysis_type):
        if analysis_type == 'channel':
            return self._channel_analysis()
        elif analysis_type == 'hashtag':
            return self._hashtag_analysis()

    def _channel_analysis(self):
        data = pd.read_excel(self.input_file)
        df = pd.DataFrame(data)
        df['publish_time'] = pd.to_datetime(df['publish_time'])
        df = df[(df["publish_time"] > self.start_date) & (df["publish_time"] < self.end_date)]
        df["title"] = df["title"].str.lower()
        df = df[df['title'].str.contains('|'.join(self.hashtag_list))]
        df_pivot = df.pivot_table(index=['channel_Name'],
                                  aggfunc={'likes': np.sum, 'comments': np.sum, 'video_id': 'nunique',
                                           'views': np.sum}) \
            .rename(columns={'video_id': 'Posts', 'views': "Total Video Views"}) \
            .reset_index()
        df_pivot['Total Engagement'] = df_pivot['likes'] + df_pivot['comments']
        df_pivot.drop(['likes', 'comments'], axis=1, inplace=True)
        df_pivot['Average Post Per Day'] = df_pivot['Posts'] / 15
        df_pivot['Average Post Per Week'] = df_pivot['Posts'] / 2
        df_pivot['Average Engagement'] = df_pivot['Total Engagement'] / df_pivot['Posts']
        return df_pivot

    def _hashtag_analysis(self):
        data = pd.read_excel(self.input_file)
        df = pd.DataFrame(data)
        df["comments"].fillna(0, inplace=True)
        df['published_at'] = pd.to_datetime(df['published_at'])
        df = df[(df["published_at"] > self.start_date) & (df["published_at"] < self.end_date)]
        df["title"] = df["title"].str.lower()
        metrics_hashtags = pd.DataFrame(columns=["hashtag", "Posts", "Likes", "Comments", "Views"])
        hashtag_page_metric = pd.DataFrame()
        for hashtag in self.hashtag_list:
            try:
                matched_rows = df[df['title'].str.contains(hashtag,case=False)]
                tempdf = matched_rows.groupby(["channel_Name"]).agg(
                    likes=('likes', 'sum'),
                    comments=('comments', 'sum'),
                    views=('views', 'sum'),
                    channel_count=('channel_Name', 'count')
                ).reset_index()
                tempdf['hashtag'] = hashtag
                hashtag_page_metric = pd.concat([hashtag_page_metric, tempdf], axis=0)
                metrics_hashtags = metrics_hashtags.append({
                    "hashtag": hashtag,
                    "Posts": matched_rows.shape[0],
                    "Likes": matched_rows["likes"].sum(),
                    "Comments": matched_rows["comments"].sum(),
                    "Views": matched_rows["views"].sum()
                }, ignore_index=True)
            except Exception as e:
                print(f"Error processing hashtag '{hashtag}': {str(e)}")
        return metrics_hashtags

    def excel_sheet(self,channel_metrics, hashtag_metrics):
        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            channel_metrics.to_excel(writer, sheet_name='Channel Metrics', index=False)
            hashtag_metrics.to_excel(writer, sheet_name='Hashtag Metrics', index=False)

    def run_Youtube_analysis(self):
        channel=self.analyze('channel')
        hashtag=self.analyze('hashtag')
        self.excel_sheet(channel,hashtag)

