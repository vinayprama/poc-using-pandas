Install the packages required:
from the requirements.txt

Database Setup:
> 📌 Before running the code, install pgAdmin4 & PostgreSQL, then:
> - Create a DB named `covid-20`
> - Inside that DB, create schemas: `silver` and `gold`
> - change the credentials in the create engine place in every level
> - Then you're good to run the scripts


step 1 : Bronze- level
* we will extract the data from the url and load it into the sql database
* by using df = pd.read_csv(url) line we are reading the data from the URL and creating a dataframe
* after that dataframe is uploaded into the sql database by using  df.to_sql() (Intial we have to create a Bronze schema before uploading it into the database)

step 2: Silver level
- Cleans and standardizes:
  - Cases & deaths
  - Hospital admissions
  - Testing rates
  - Country response measures
- Handles date formatting, title casing, missing values, and schema filtering
- Extracts `year`, `month`, `week` from dates where applicable
- Stores cleaned data into PostgreSQL `silver` schema tables

step 3: Gold level

- Extracts cleaned tables from the Silver layer
- Builds:
  - `dim_country` (unique countries & continents)
  - `dim_date` (calendar dimension from case dates)
  - `fact_cases` (daily total cases & deaths)
  - `fact_hospital_admissions` (daily hospital admissions)
- Writes these tables into PostgreSQL under the `gold` schema

---

Tech STACK USED :
- **Python 3**
- **Pandas**
- **SQLAlchemy**
- **PostgreSQL**
