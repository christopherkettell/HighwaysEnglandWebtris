import requests
import pandas as pd
import numpy as np


def site_info(site_id):
    """Returns a dictionary giving info on the specified site"""
    r = requests.get('http://webtris.highwaysengland.co.uk/api/v1.0/sites/' +
                     str(site_id))
    return r.json()['sites'][0]


def site_data(site, start_date, end_date):
    """Returns a pandas dataframe containing traffic data in 15 minute chunks.
    Dates are string of the format ddmmyyyy"""
    more_pages = True
    page_num = 1
    data = []

    while more_pages:
        r = requests.get('https://webtris.highwaysengland.co.uk/api/v1/reports/daily',
                         params={'sites': str(site),
                                 'start_date': start_date,
                                 'end_date': end_date,
                                 'page': page_num,
                                 'page_size': 40000})
        json_response = r.json()
        invalid_response = 'Invalid request - has the API version changed?'
        if json_response == invalid_response:

            raise Exception('The API did not return any data' +
                            'but the right arguments were used - was data actually availible?. ')
        for item in json_response['Rows']:
            data.append(item)
        print('Fetched ' + str(len(data)) + 'data points')
        page_num += 1
        links = [item['rel'] for item in json_response['Header']['links']]
        more_pages = 'nextPage' in links

    df = pd.DataFrame(data)

    df2 = pd.DataFrame(df)
    #   site = '9478J'
    df2 = df.apply(lambda x: np.where(x == '', '-99', x))

    df3 = pd.DataFrame({
        'Date': pd.to_datetime(df2['Report Date']),
        'Time': df2['Time Period Ending'],
        'SiteId': df2['Site Name'].astype(str),
        'Interval': df2['Time Interval'].astype(int),
        'TotalFlow': df2['Total Volume'].astype(str).astype(int),
        'LargeVehicleFlow': df2['1160+ cm'].astype(int),
        'AverageSpeedMPH': df2['Avg mph'].astype(int)})

    df3.loc[df3['TotalFlow'] == -99, 'TotalFlow'] = np.nan
    df3.loc[df3['LargeVehicleFlow'] == -99, 'LargeVehicleFlow'] = np.nan
    df3.loc[df3['AverageSpeedMPH'] == -99, 'AverageSpeedMPH'] = np.nan

    return df3
