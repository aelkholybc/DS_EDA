import requests
import pandas as pd
from matplotlib import pyplot as plt
from tqdm import tqdm
import time
from scipy.stats import zscore

def get_timeline(query, start_time, end_time):
    params = {
        "query": query,
        "STARTDATETIME": start_time,
        "ENDDATETIME": end_time,
        "FORMAT": "json",
        "mode": "TimelineVolRaw"
        }
    base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
    response = requests.get(base_url, params=params)
    try:
        return pd.DataFrame.from_dict(response.json()['timeline'][0]['data'])
    except:
        print("Query error {} {}".format(query, response.text))
        return None


def find_peaks(df, zscore_threshold):
    df['zscore'] = zscore(df['value'])
    peak_dates = df[df['zscore'] > zscore_threshold]['date']
    return peak_dates


def get_stats(ts, std=4):
    # assume 'date' and 'value' columns
    # another idea is to summarize the graphs (but let's still keep the graphs) in a certain way so that there is a dataframe we can look at.
    # distance to peak of daily articles from day 0.
    # distance to peak/10 (or less) of daily articles from day 0.
    # total count of shortage articles
    # average number of days to a shortage article from peak of daily articles.
    peak = ts['value'].max()
    peak_date = pd.to_datetime(ts[ts['value']==peak]['date'].values[0]).strftime("%Y-%m-%d")
    non_zero_index = ts['value'].ne(0).idxmax()
    non_zero_date = pd.to_datetime(ts.iloc[non_zero_index]['date']).strftime("%Y-%m-%d")
    peaks = find_peaks(ts, std)
    dist_to_peak = (pd.to_datetime(peak_date) - pd.to_datetime(non_zero_date)).days
    return {"num_peaks": len(peaks), "peak_date": ts[ts['value']==peak]['date'].values[0], "non_zero_date": ts.iloc[non_zero_index]['date'], "dist_to_peak": dist_to_peak}


# data = pd.read_csv('1970-2021_DISASTERS.xlsx - emdat data.csv')
# data = data[data.Year==2021]
# named = data[~data['Event Name'].isna()]

short_query = """{} AND ("shortage" OR "demand spike")"""
disasters = [{'name': '"Filomena"', 'start': 2021, 'end': 2022}, {'name': '"Hurricane Elsa"', 'start': 2021, 'end': 2022}, {'name': '"cylone Niran"', 'start': 2021, 'end': 2022}, {'name': '"cyclone Seroja 21"', 'start': 2021, 'end': 2022}, {'name': '"Cyclone Yaas"', 'start': 2021, 'end': 2022}, {'name': '"storm Ida"', 'start': 2021, 'end': 2022}, {'name': '"Mount Sangay"', 'start': 2021, 'end': 2022}, {'name': '"cyclone Ana"', 'start': 2021, 'end': 2022}, {'name': '"storm Enrique"', 'start': 2021, 'end': 2022}, {'name': '"cyclone Grace"', 'start': 2021, 'end': 2022}, {'name': '"cylone Nora"', 'start': 2021, 'end': 2022}, {'name': '"Cyclone Shaheen"', 'start': 2021, 'end': 2022}, {'name': '"storm Choi Wan"', 'start': 2021, 'end': 2022}, {'name': '"cyclone Conson (Jolina)"', 'start': 2021, 'end': 2022}, {'name': '"storm Dolores"', 'start': 2021, 'end': 2022}, {'name': '"Mount Nyiragongo"', 'start': 2021, 'end': 2022}, {'name': '"storm Koguma"', 'start': 2021, 'end': 2022}, {'name': '"cyclone Eloise"', 'start': 2021, 'end': 2022}, {'name': '"storm Jobo"', 'start': 2021, 'end': 2022}, {'name': '"cyclone Surigae"', 'start': 2021, 'end': 2022}, {'name': '"Marburg fever"', 'start': 2021, 'end': 2022}, {'name': '"Mount Pacaya"', 'start': 2021, 'end': 2022}, {'name': '"Mount Merapi"', 'start': 2021, 'end': 2022}, {'name': '"cyclone Tauktae"', 'start': 2021, 'end': 2022}, {'name': '"Cyclone Gulab"', 'start': 2021, 'end': 2022}, {'name': '"Dengue" AND "outbreak"', 'start': 2021, 'end': 2022}, {'name': '"Cholera" AND "outbreak"', 'start': 2021, 'end': 2022}, {'name': '"storm Dujuan (Auring)"', 'start': 2021, 'end': 2022}, {'name': '"Taal volcano"', 'start': 2021, 'end': 2022}, {'name': '"Cumbre Vieja volcano"', 'start': 2021, 'end': 2022}, {'name': '"Storm Claudette"', 'start': 2021, 'end': 2022}, {'name': '"depression Henri"', 'start': 2021, 'end': 2022}, {'name': '"Dixie fire"', 'start': 2021, 'end': 2022}, {'name': '"Caldor fire"', 'start': 2021, 'end': 2022}, {'name': '"Telegraph and Mescal Fires"', 'start': 2021, 'end': 2022}, {'name': '"Bootleg Fire "', 'start': 2021, 'end': 2022}, {'name': '"La Souffrière"', 'start': 2021, 'end': 2022}, {'name': '"Ituango Dam" AND "collapse"', 'start': 2018, 'end': 2019}, {"name": '"ethiopian" AND "boeing MAX" AND "crash"', 'start': 2019, 'end': 2020}, {"name": '"Morandi Bridge" AND "collapse"', 'start': 2018, 'end': 2019}, {"name": '"oil refinery" AND "explosion" AND "husky energy"', 'start': 2018, 'end': 2019}, {"name": '"oil refinery" AND "fire" AND "sonara"', 'start': 2018, 'end': 2019}, {"name": '"lion air" AND "boeing MAX" AND "crash"', 'start': 2018, 'end': 2019}, {"name": '"Brumadinho dam" AND "collapse"', 'start': 2019, 'end': 2020}]


raw_ts = []
data = []
for entry in tqdm(disasters):
    try:
        name = entry['name']
        s_year = entry['start']
        e_year = entry['end']
        base = get_timeline(f'{name}', f"{s_year}0101000000", f"{e_year}0131000000")
        short = get_timeline(short_query.format(name), f"{s_year}0101000000", f"{e_year}0131000000")
        labor = get_timeline(f'"labor shortage" AND {name}', f"{s_year}0101000000", f"{e_year}0131000000")
        electricity = get_timeline(f'("electric shortage" OR "electricity shortage" OR "shortage of electricity") AND {name}', f"{s_year}0101000000", f"{e_year}0131000000")
        final = short.copy()
        
        if labor is not None:
            assert((short['value']>=labor['value']).all())
            final['value'] = final['value'] - labor['value']
        
        if electricity is not None:
            assert((short['value']>=electricity['value']).all())
            final['value'] = final['value'] - electricity['value']
        
        base_stats = get_stats(base)
        short_stats = get_stats(final)
        data.append({
            "name": name,
            "year": s_year,
            "num_peaks": base_stats['num_peaks'],
            "peak_date": base_stats['peak_date'],
            "first_mention": base_stats['non_zero_date'],
            "dist_to_peak": base_stats['dist_to_peak'],
            "total_articles": base['value'].sum(),
            "total_shortage_articles": final['value'].sum(),
        })

        raw_ts.append({"name": name, "base": base, "short": final})
        
        # Plotting
        fig, ax1 = plt.subplots(figsize=(10, 6))
        # Plot the 'base' data on the primary y-axis
        ax1.plot(base['date'], base['value'], label='searching for disaster', color='blue')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Base Value', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        # Create a second y-axis for the 'short' data
        ax2 = ax1.twinx()
        ax2.plot(final['date'], final['value'], label='searching for disaster + shortage terms', color='red')
        ax2.set_ylabel('Short Value', color='red')
        ax2.tick_params(axis='y', labelcolor='red')
        # Setting up the legend
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')
        plt.title(name)
        plt.savefig(f'plots/{name}.png', dpi=300)
        plt.close(fig)
        time.sleep(5)
    except Exception as e:
        print(e)
        pass



import code; code.interact(local=dict(globals(), **locals()))