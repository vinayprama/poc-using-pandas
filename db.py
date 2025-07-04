from sqlalchemy import create_engine, text


engine = create_engine("postgresql+psycopg2://postgres:vinay007@localhost:5432/covid-20")

with engine.connect() as conn:
    result = conn.execute(text("SELECT version();"))
    for row in result:
        print(row)
