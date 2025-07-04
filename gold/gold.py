import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from silver.sliver import (
    cleaned_cases,
    cleaned_hospital_admissions,
    cleaned_testing,
    cleaned_country_response
)




dim_country = cleaned_cases.copy()
dim_country['country'] = dim_country['country'].str.title()

dim_country = (
    dim_country.groupby('country', as_index=False)
    .agg({'continent': 'max'})  # max is safe if continent is consistent per country
)

dim_date = cleaned_cases[['date']].dropna().drop_duplicates().copy()

dim_date['year'] = dim_date['date'].dt.year
dim_date['month'] = dim_date['date'].dt.month
dim_date['day'] = dim_date['date'].dt.day
dim_date['weekday'] = dim_date['date'].dt.day_name()

fact_cases = (
    cleaned_cases.groupby(['country', 'date'], as_index=False)
    .agg({'cases': 'sum', 'deaths': 'sum'})
    .rename(columns={'cases': 'total_cases', 'deaths': 'total_deaths'})
)

fact_hospital_admissions = (
    cleaned_hospital_admissions.groupby(['country', 'date'], as_index=False)
    .agg({'value': 'sum'})
    .rename(columns={'value': 'total_admissions'})
)

from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://postgres:vinay007@localhost:5432/covid-20")

print("📤 Writing gold tables to PostgreSQL...")

dim_country.to_sql('dim_country', engine, schema='gold', if_exists='replace', index=False)
dim_date.to_sql('dim_date', engine, schema='gold', if_exists='replace', index=False)
fact_cases.to_sql('fact_cases', engine, schema='gold', if_exists='replace', index=False)
fact_hospital_admissions.to_sql('fact_hospital_admissions', engine, schema='gold', if_exists='replace', index=False)

print("✅ Gold tables written.")
