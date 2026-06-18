# US-Energy-Price-Forecasting

## Overview
The Natural Gas Spot Price Forecast application forecasts volatility for Henry Hub natural gas spot prices ($/MMBTU) for 7, 14, 30 and 60-day time horizons using GRU models. The purpose of this application is to support energy companies and traders in the reduction of financial risks by forecasting future price movements. It is an investigative tool that allows users to analyse prices and movements over a given forecasting period.

The GRU model processes 25+ years of historical data from the EIA (Energy Information Administration) and NOAA (National Oceanic and Atmospheric Administration), considering factors such as lagged prices, historical volatility and external factors such as weather, demand and supply-side factors to generate model predictions.

A medallion data lake architecture was created for processing data for this particular application.

Complete documentation can be found here: https://jacobs-organization-35.gitbook.io/us-natural-gas-price-volatility-forecast

## Challenges
As part of developing the Natural Gas Spot Price Forecast application, the three main challenges I faced were designing the data lake architecture, integrating multiple heterogeneous data sources, and ensuring data quality.

### Designing the Data Architecture
As part of designing the data lake architecture, I had to address several considerations, with one such consideration being combining batch processing of historical data with incremental updates as new data becomes available. Designing for both cases was a source of complexity, with the added complexity of dealing with pipeline failures. This was ultimately dealt with by creating a centralised metadata.json file that would keep track of the latest date of data, latest extraction timestamp, and the extracted S3 path, the latest and previous transformed timestamps and S3 file paths for each relevant dataset produced by the pipelines as part of processing. These metadata items would be updated to ensure that only new data is processed in successive runs.

Another design consideration was integrating SageMaker and MLflow to ensure model metrics and artifacts were being logged in MLflow from the SageMaker training jobs. To ensure reproducibility, the pipeline was designed to automatically log training parameters, dataset versions, evaluation metrics and model artifacts from SageMaker into MLFlow, creating a single source of truth for model governance and comparison.

### Integrating Multiple Heterogeneous Data Sources
I was required to integrate data at both daily and monthly granularity from the EIA (Energy Information Administration) and NOAA (National Oceanic and Atmospheric Administration) APIs. Natural gas, heating oil spot prices, and weather data were at the daily level, while the supply and demand variables were at the monthly level, and each of these sources had a different update frequency that had to be handled. 

I decided, as part of the development, to ensure modelling datasets would always have data up to the latest date in the natural gas spot prices dataset. This was to ensure that model training and predictions would always be done on the most up to date data. To accommodate this, it was necessary to convert monthly variables to the day level for any given month and forward fill values, as the latest month for monthly variables lagged behind that of the natural gas spot prices dataset. While this would slightly compromise the accuracy of the monthly variables data used in forecasting, this was a necessary tradeoff that had to be made to ensure the most up-to-date predictions were available.

### Ensuring Data Quality
To ensure the data was of high quality and without bias for ML modelling, careful treatment, particularly in regard to the removal or imputation of missing values. From the initial inspection of weather data from the NOAA (National Oceanic and Atmospheric Administration), there were numerous null values across the variables of minimum temperature, maximum temperature, average wind speed, total precipitation and total snowfall. To ensure accurate, unbiased imputation of missing values for these variables, imputation was based on the underlying distribution of those variables, namely, the arithmetic mean for minimum temperature, maximum temperature and average wind speed, as these variables have an approximately normal distribution, while values for total precipitation and total snowfall are clustered around zero with a lognormal distribution and as such these variables were imputed using median values. As there was weather data for several cities and there is seasonal weather variation, these mean and median measures were calculated at the city and quarter level.

Additionally, there were missing records for natural gas spot prices and monthly LNG import prices data. Given that there was only a single missing record for natural gas spot prices, and prices follow a random walk, the decision was made to remove this particular record. On the other hand, for the missing LNG import data, there was a large variation in LNG import prices on a month-to-month basis. As such, these missing values were ultimately imputed using the median price based on prices from the 6 months before and 6 months succeeding the missing records. 

## Learnings
From creating this particular application, I learned that data engineering is of equal importance compared to building the model when developing machine learning systems. A significant amount of time was necessary to understand the characteristics of the source data, handling different data granularities across the sources, and designing an incremental data processing workflow to ensure effective feature engineering. Additionally, several considerations needed to be made in relation to data quality management to ensure accurate, high-quality data.

Integrating SageMaker for training and MLflow for experiment tracking highlighted the importance of maintaining model lineage and reproducibility. Capturing datasets, parameters, metrics, and model artifacts consistently was necessary to understand and compare the behaviour of models as well as any performance drift that occurs over time. I therefore learned that reproducibility is as much a data engineering and MLOps problem as it is a machine learning problem. 

Moreover, the project involved data ingestion, storage, transformation, feature engineering, model training, experiment tracking, and application delivery. Success depended on understanding how decisions in one layer affected the entire system. I therefore learned that machine learning systems require balancing data engineering, machine learning, and platform considerations rather than optimising any single component in isolation.

