import pandas as pd
from sqlalchemy import create_engine

bronze_urls = {
    "cases_deaths": "https://ecdc.blob.core.windows.net/covid19/cases_deaths.csv",
    "hospital_admissions": "https://ecdc.blob.core.windows.net/covid19/hospital_admissions.csv",
    "testing": "https://ecdc.blob.core.windows.net/covid19/testing.csv",
    "country_response": "https://ecdc.blob.core.windows.net/covid19/country_response.csv",
}

engine = create_engine("postgresql+psycopg2://postgres:vinay007@localhost:5432/covid-20")

for name, url in bronze_urls.items():
    df = pd.read_csv(url)
    df.to_sql(name, engine, schema='bronze', if_exists='replace', index=False)
