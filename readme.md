# NYC Taxi Trips During Early COVID-19 Pandemic

## Team 1 Final Project

This project analyzes NYC taxi trips during the early months of the COVID-19 pandemic using a data pipeline.

---
## How to Run the Data Pipeline

### 1. Getting the Data
Run the `getData` shell script to fetch the yellow taxi trip data for all 12 months of 2020. The script downloads the data into the `raw_data/yellow_taxi_data` folder. The shapefile and covid data is transferred into EC2 instance via Filezilla (SFTP)

```sh
./getData
```

Below are the links to the dataset\
[NYC Taxi Data including shapefile](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)\
[NYC Covid-19 Data](https://data.cityofnewyork.us/Health/COVID-19-Daily-Counts-of-Cases-Hospitalizations-an/rc75-m7u3/about_data)

### 2. Transforming the Data
Run the `transform` shell script to process the data.

#### Inputs (Optional Command Line Arguments):
- `--upload [True / False]`: Determines whether the script uploads CSV data to a NextCloud instance.
- `--username [string]`: Required if `--upload` is set to `True` (NextCloud username).

#### Outputs:
- Processed views and joined data stored in the `final_csv` folder.
- If `--upload True` is specified, the data is also uploaded to a NextCloud folder.

#### What the Script Does:
1. Runs a Spark submit job that joins and pre-aggregates data. See more in `spark.py`
2. Runs another Spark submit job that combines all previous Spark outputs. See more in `combine.py`
3. Collects all Parquet outputs and moves them into the `processed_data` directory.
4. Runs a DuckDB job to further join/aggregate data and export CSV files into the `final_csv` directory. See more in `views.sql`

```sh
./transform --upload True --username your_nextcloud_username
```

### 3. Serving the Data
Since the public version of Tableau does not support direct database connections, we manually download the data from NextCloud to our local environments for visualization. \
[Link to Data Hosted in NextCloud](https://jzhang502.duckdns.org:502/nextcloud/index.php/s/YTEeEHLoRRBWMRK)

### 4. Tableau Public Link
[Link to dashboard](https://public.tableau.com/app/profile/haofei.zhang/viz/team1_17421126800890/Team1Dashboard?publish=yes)

---
## Configuring NextCloud Authentication
If you choose to upload data to NextCloud, you must configure a `.netrc` file for authentication.

### Sample `.netrc` Configuration:
```
machine your_nextcloud_server
login your_username
password your_secure_password
```
For now, only @jzhang502's server is supported

Make sure to replace `your_nextcloud_server`, `your_username`, and `your_secure_password` with your actual credentials.

> **Security Notice:** Never store plain-text credentials in publicly shared repositories.
