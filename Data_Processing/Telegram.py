from datetime import datetime, timezone
import pandas as pd
import re
from collections import Counter

class Telegram:
    def __init__(self, input_file, output_file, start_date, end_date):
        self.input_file = input_file
        self.output_file = output_file
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)

    def telegram_hashtags_collection(self):
        data = pd.read_json(self.input_file)
        df = pd.DataFrame(data)
        df['date'] = df['date'].apply(lambda x: x.astimezone(timezone.utc).replace(tzinfo=None) if hasattr(x, 'tzinfo') else x)
        df = df[(df["date"] > self.start_date) & (df["date"] < self.end_date)]
        hashtags_list = []
        for message in df["message"]:
            if isinstance(message, str) and message:
                matches = re.findall(r'#\w+', message)
                hashtags_list.extend(matches)
        word_counts = Counter(hashtags_list)
        sorted_word_counts = word_counts.most_common()
        df = pd.DataFrame(sorted_word_counts, columns=['Word', 'Count'])
        return df
    
    def hashtags_date(self,Hashtag_df):
        data=pd.read_json(self.input_file)
        df=pd.DataFrame(data)
        req=Hashtag_df
        df['date'] = df['date'].apply(lambda x: x.astimezone(timezone.utc).replace(tzinfo=None) if hasattr(x, 'tzinfo') else x)
        df["message"]=df["message"].str.lower()
        hashtags=req["Word"].str.lower().to_list()
        df= df[df['message'].notna() & (df['message'] != '')]
        data=[]
        for i,hashtag in enumerate(hashtags):
            string_to_match = hashtag
            mask = df['message'].str.contains(string_to_match)
            matched_rows = df[mask]

            Start_date=matched_rows["date"].min()
            End_date = matched_rows["date"].max()
            data.append([hashtag,Start_date,End_date])
        result=pd.DataFrame(data,columns={"Hashtag":hashtag,"Start_Date":Start_date,"End_Date":End_date})
        return result
    
    def excel_sheet(self,Hashtag_frequency,hashtag_Date):
        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            Hashtag_frequency.to_excel(writer, sheet_name='Hashtags', index=False)
            hashtag_Date.to_excel(writer, sheet_name='Hashtag Dates', index=False)
    
    def run_telegram_analysis(self):
        df = self.telegram_hashtags_collection()
        result=self.hashtags_date(Hashtag_df=df)
        self.excel_sheet(df, result)
