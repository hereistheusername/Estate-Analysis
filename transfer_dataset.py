import pandas as pd

DATA_DIR = './data/'


def expand_date(record):
    datetime = record['datetime']
    return datetime.year, datetime.month, datetime.day

def fill_deco_year(record):
    decoration_year = record['decoration year']
    return decoration_year if decoration_year else record['building year']


"""
    1. zip code -> latitude, longitude
    2. date     -> timestamp
    3. timestamp-> year, month, day
    4. 0(decoration year) -> building year
    5. drop(['date', 'decoration year', 'district', 'city', 'zip code', 'region', 'int zip code', 'ZIP', 'datetime', 'decoration year'], axis='columns')
    6. move total cost to the last position
"""


def transfer(df, zip_df, fmt='%m/%d/%Y %H:%M'):
    # 1.
    df['int zip code'] = df['zip code'].apply(lambda r: int(r[-5:]))
    df = df.merge(zip_df, left_on='int zip code', right_on='ZIP', how='left')
    # 2.
    df['date'] = df['date'].apply(lambda r: r[:-8] + '20' + r[-8:])
    df['datetime'] = pd.to_datetime(df['date'], format=fmt)
    df['timestamp'] = df['datetime'].values.astype('int') // 10 ** 9
    # 3.
    df[['transaction year', 'transaction month', 'transaction day']] = df.apply(expand_date, axis=1, result_type='expand')
    # 4.
    df['filled decoration year'] = df.apply(fill_deco_year, axis=1)
    # 5.
    df = df.drop(['date', 'decoration year', 'district', 'city', 'zip code', 'region', 'int zip code', 'ZIP', 'datetime', 'decoration year'], axis='columns')
    # 6.
    df['total cost'] = df.pop('total cost')
    return df


if __name__ == '__main__':
    df = pd.read_csv('{}Test_Data_utf8.csv'.format(DATA_DIR))
    zip_df = pd.read_csv('{}2013_us_zipcode_latlng.csv'.format(DATA_DIR))
    df = transfer(df, zip_df)
    df.to_csv('{}Test_Data_Converted&Dropped.csv'.format(DATA_DIR), index=False)
