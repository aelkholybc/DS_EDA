import requests
from openai import AzureOpenAI
import pandas as pd
from tqdm import tqdm
import json
from collections import Counter
import itertools
from itertools import chain
import instructor
from pydantic import BaseModel, Field
from typing import List, Optional
from instructor import Mode

token = 'e81baea7e64033ef4aaa11c77571905c'
api = 'https://api.diffbot.com/v3/article'


def filter_high_overlap_articles(df, column):
    """
    Create a new DataFrame filtering out articles with high text overlap, keeping only the shorter text when overlap is above 70%.

    Args:
    df (pd.DataFrame): DataFrame containing the text data.
    column (str): The column name containing the text.

    Returns:
    pd.DataFrame: A new DataFrame with articles having less than 70% overlap.
    """
    # Function to calculate the overlap percentage between two texts
    def overlap_percentage(text1, text2):
        words1 = Counter(text1.split())
        words2 = Counter(text2.split())
        common_words = sum((words1 & words2).values())
        total_words = sum((words1 | words2).values())
        return 100 * common_words / total_words if total_words != 0 else 0
    # Mark rows for deletion
    to_delete = set()
    # Comparing each pair of articles
    for (idx1, article1), (idx2, article2) in itertools.combinations(df.iterrows(), 2):
        overlap = overlap_percentage(article1[column], article2[column])
        if overlap > 70:
            # Keep the article with the shorter text
            if len(article1[column]) > len(article2[column]) and article1[column] != '':
                to_delete.add(idx1)
            else:
                to_delete.add(idx2)
    # Creating a new dataFrame without the marked rows
    filtered_df = df.drop(index=to_delete).reset_index(drop=True)
    return filtered_df


def get_articles(query, start_time, end_time):
    params = {
        "query": query,
        "STARTDATETIME": start_time,
        "ENDDATETIME": end_time,
        "FORMAT": "json",
        "mode": "artlist",
        "maxrecords": 250
        }
    base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
    response = requests.get(base_url, params=params)
    try:
        return pd.DataFrame.from_dict(response.json()['articles'])
    except:
        print("Query error {} {}".format(query, response.text))
        return None

query = '"dixie fire" AND ("shortage" OR "demand spike" OR "surge in demand") AND -"water shortage" sourcelang:english'
start_time='20210701000000'
end_time='20211201000000'

articles = get_articles(query, start_time, end_time)

scraped = []
for a, row in tqdm(articles.iterrows()):
    params = {
        "token": token,
        "url": row['url']
    }
    scraped.append(requests.get(url=api, params=params).json())

temp_df = pd.DataFrame(scraped)
temp_df.to_csv(f'{query}.csv')
# temp_df = pd.read_csv(f'{query}.csv')
temp_df = temp_df[temp_df['errorCode'].isna()]
# import ast
# temp_df['objects'] = temp_df['objects'].apply(ast.literal_eval)
temp_df['text'] = temp_df['objects'].apply(lambda x: x[0]['text'] if x else None)

deduped = filter_high_overlap_articles(temp_df, 'text')

print(f"Pulled {deduped.shape[0]} unique articles for query {query} in timeframe {start_time} to {end_time}")

client = instructor.patch(AzureOpenAI(api_key='7bcf1af3b56c49fa816f22daeb51a021', api_version="2023-08-01-preview", azure_endpoint="https://brainchainuseast2.openai.azure.com"), mode=Mode.JSON)
# client = AzureOpenAI(api_key='7bcf1af3b56c49fa816f22daeb51a021', api_version="2023-08-01-preview", azure_endpoint="https://brainchainuseast2.openai.azure.com")

# model = 'gpt-4-32k'
# openai.api_type = "azure"
# openai.api_key = '7bcf1af3b56c49fa816f22daeb51a021'
# openai.api_base = "https://brainchainuseast2.openai.azure.com"
# openai.api_version = "2023-08-01-preview"

class Shortage(BaseModel):
    product: Optional[str] = Field(description="This is the product that is in short supply. It should be a product that is in short supply NOT DUE TO COVID OR THE PANDEMIC.")
    reason: Optional[str] = Field(description="This is the reason for the shortage. Go into depth, don't just say 'supply chain issues' if there is a reason for those issues. Leave blank if none specified.")
    location: Optional[str] = Field(description="This is the location of the shortage (e.g. 'in the US' or 'in the EU'). Leave blank if none specified.")
    impact: Optional[str] = Field(description="This is the impact of the shortage (e.g. 'prices are rising' or 'people are hoarding'). Leave blank if none specified.")

class ListShortages(BaseModel):
    shortages: List[Shortage] = Field(description="This is a list of shortages. Each shortage has a product, reason, location, and impact.")

MAX_RETRIES = 5
asdf = []
for idx, row in tqdm(blah.iterrows()):
    text = row.text
    content = """
    Take a deep breath, and read the following instructions carefully.
    1. Go through the attached article text and pick out all products, goods, or commodities that are mentioned. These are tangible goods and NOT services/utilities (e.g. DO NOT MENTION ELECTRICITY or LABOR SHORTAGES).
        I REPEAT: THE SHORTAGE SHOULD NOT BE OF HUMAN LABOR, OR JOB SHORTAGES. IT SHOULD BE OF A PRODUCT OR GOOD.
    2. Check each one. Is it a type of service or labor? If so, ignore it. Is it a type of good or commodity? If so, continue.
    3. Is this related to COVID-19/pandemic related? If so, ignore it. If not, continue.
    4. Is it a SPECIFIC product? For example, 'parts' is not specific, but 'semiconductors' is specific. If it is specific, continue. If not, ignore it.
    6. Is this hyper local to a specific business? For example, a local pizzeria is hyper local, but 'pizza restaurants' is not. If it is hyper local, ignore it. If not, continue.
    5. If one of them is mentioned as being in short supply or having a spike in demand/price, fill out the following JSON with this information (one entry per product):
        [
            {
                "product": "This is the product that is in short supply. It should be a product that is in short supply NOT DUE TO COVID OR THE PANDEMIC.",
                "reason": "This is the reason for the shortage. Go into depth, don't just say 'supply chain issues' if there is a reason for those issues. Leave blank if none specified.",
                "location": "This is the location of the shortage (e.g. 'in the US' or 'in the EU'). Leave blank if none specified.",
                "impact": "This is the impact of the shortage (e.g. 'prices are rising' or 'people are hoarding'). Leave blank if none specified."
            }
        ]
    Note: always return it in this format, where it's a list of dictionaries. If there is no shortage, just return an empty list. If there is not article text attached, return an empty list.
    REPEAT: IF THERE IS NO ARTICLE ATTACHED, RETURN [] (an empty list).
    If you can't find any info to fill one of the keys just leave it blank, don't add "none specified" or anything like that. As long as there is a product meeting the criteria above, just add the product and what ever else you can find.
    Make SURE each entry refers ONLY to a product that people buy or sell. For example, a spike in pneumonia is NOT what you should look for, but a shortage of vaccines IS what you want.
    DOUBLE CHECK that the product mentioned is, in fact, experiencing some kind of shortage or surge in demand. The idea is the product you find should be something people are talking about as being scarce, hard to find, or anticipated to be in short supply.
    AGAIN DO NOT ADD ANY ITEMS THAT ARE COVID OR PANDEMIC RELATED. ONLY ADD ITEMS THAT ARE NOT COVID OR PANDEMIC RELATED.
    ARTICLE: \n
    """ + text
    messages=[
        {"role": "system", "content": "You are an expert analyst that speaks only JSON. You cannot print a character that is not part of a valid json string. Return only JSON. Additionally, you are blind to anything related to covid/the pandemic"},
        {"role": "user", "content": content}
    ]
    retries = 0
    while retries < MAX_RETRIES:
        try:
            res = client.chat.completions.create(
                model="gpt4-turbo",
                messages=messages,
                response_model=ListShortages)
            print(text)
            print(res.model_dump_json())
            if json.loads(res.model_dump_json())['shortages']:
                asdf.extend(json.loads(res.model_dump_json())['shortages'])
                break
        except Exception as e:
            retries += 1
            print("retrying: {}".format(str(e)))
        if retries == MAX_RETRIES:
            print('error')


shortage_df = pd.DataFrame.from_dict(asdf)
shortage_df.to_csv(f'{query}_shortages.csv',index=False)