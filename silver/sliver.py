import pandas as pd
import re
import os
import sys
import numpy as np
from sqlalchemy import create_engine

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from bronze.bronze import bronze_urls


cases_deaths = pd.read_csv(bronze_urls['cases_deaths'])
hospital_admissions = pd.read_csv(bronze_urls['hospital_admissions'])
testing = pd.read_csv(bronze_urls['testing'])
country_response = pd.read_csv(bronze_urls['country_response'])


cases_deaths['country'] = cases_deaths['country'].str.title()

cases_deaths['date'] = pd.to_datetime(cases_deaths['date'], format='%Y-%m-%d')

cases_deaths['year'] = cases_deaths['date'].dt.year
cases_deaths['month'] = cases_deaths['date'].dt.month

cases_deaths['cases'] = cases_deaths['daily_count'].where(
    cases_deaths['indicator'].str.contains('cases', case=False)
)
cases_deaths['deaths'] = cases_deaths['daily_count'].where(
    cases_deaths['indicator'].str.contains('deaths', case=False)
)
cleaned_cases = cases_deaths[
    cases_deaths['indicator'].str.contains('cases|deaths', case=False, na=False)
][['country', 'continent', 'date', 'year', 'month', 'cases', 'deaths']]

hospital_admissions['country'] = hospital_admissions['country'].str.title()
hospital_admissions['date'] = pd.to_datetime(hospital_admissions['date'], format='%Y-%m-%d')
hospital_admissions['year'] = hospital_admissions['date'].dt.year
hospital_admissions['month'] = hospital_admissions['date'].dt.month

cleaned_hospital_admissions = hospital_admissions[hospital_admissions['value'].notnull()][
    ['country', 'indicator', 'date', 'year', 'month', 'value', 'source', 'url']
]

def is_valid_year_week(x):
    match = re.match(r'^(\d{4})W(\d{1,2})$', x.replace('-', '').replace('_', '').replace(' ', ''))
    return bool(match)


testing['year_week'] = testing['year_week'].astype(str)
valid_testing = testing[
    testing['country'].notnull() &
    testing['year_week'].str.contains(r'^\d{4}W\d{1,2}$', na=False)
].copy()


split_year_week = valid_testing['year_week'].str.extract(r'(?P<year_str>\d{4})W(?P<week_str>\d{1,2})')

valid_testing['year_str'] = split_year_week['year_str']
valid_testing['week_str'] = split_year_week['week_str']

valid_testing['year'] = pd.to_numeric(valid_testing['year_str'], errors='coerce')
valid_testing['week'] = pd.to_numeric(valid_testing['week_str'], errors='coerce')

valid_testing.dropna(subset=['year', 'week'], inplace=True)
valid_testing['year'] = valid_testing['year'].astype(int)
valid_testing['week'] = valid_testing['week'].astype(int)

valid_testing['date'] = pd.to_datetime(valid_testing['year'].astype(str) + '-01', format='%Y-%d', errors='coerce') + \
                        pd.to_timedelta((valid_testing['week'] - 1) * 7, unit='d')

valid_testing['month'] = valid_testing['date'].dt.month
valid_testing['country'] = valid_testing['country'].str.title()

cleaned_testing = valid_testing[['country', 'date', 'year', 'month', 'testing_rate']]

country_response = country_response[
    country_response['Country'].notnull() &
    country_response['Response_measure'].notnull()
].copy()

country_response['country'] = country_response['Country'].str.title()
country_response['date_start'] = pd.to_datetime(country_response['date_start'], format='%Y-%m-%d', errors='coerce')
country_response['date_end'] = pd.to_datetime(country_response['date_end'], format='%Y-%m-%d', errors='coerce')
cleaned_country_response = country_response[['country', 'Response_measure', 'change', 'date_start', 'date_end']]
cleaned_country_response = cleaned_country_response.rename(columns={'Response_measure': 'response_measure'})



engine = create_engine("postgresql+psycopg2://postgres:vinay007@localhost:5432/covid-20")

print("📤 Writing silver tables to PostgreSQL...")

cleaned_cases.to_sql('cleaned_cases', engine, schema='silver', if_exists='replace', index=False)
cleaned_hospital_admissions.to_sql('cleaned_hospital_admissions', engine, schema='silver', if_exists='replace', index=False)
cleaned_testing.to_sql('cleaned_testing', engine, schema='silver', if_exists='replace', index=False)
cleaned_country_response.to_sql('cleaned_country_response', engine, schema='silver', if_exists='replace', index=False)

print("✅ Silver tables written.")
