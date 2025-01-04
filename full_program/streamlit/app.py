'''Import relevant modules'''
from datetime import datetime
import sys
import streamlit as st
import pandas as pd
sys.path.append('/opt/airflow')
import plotly.graph_objects as go
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms

# Instantiate classes for Config, S3
config = Config()
s3 = S3(config=config)

# Load Predictions
predictions_7day_json = s3.get_data(folder='full_program/curated/predictions/', object_key='predictions_7day')
predictions_7day_df = EtlTransforms.json_to_df(data=predictions_7day_json, date_as_index=True)
predictions_14day_json = s3.get_data(folder='full_program/curated/predictions/', object_key='predictions_14day')
predictions_14day_df = EtlTransforms.json_to_df(data=predictions_14day_json, date_as_index=True)
predictions_30day_json = s3.get_data(folder='full_program/curated/predictions/', object_key='predictions_30day')
predictions_30day_df = EtlTransforms.json_to_df(data=predictions_30day_json, date_as_index=True)
predictions_60day_json = s3.get_data(folder='full_program/curated/predictions/', object_key='predictions_60day')
predictions_60day_df = EtlTransforms.json_to_df(data=predictions_60day_json, date_as_index=True)

# Calculate % changes for each date in dataframe
predictions_7day_df['pct_change price ($/MMBTU)'] = round(predictions_7day_df['price ($/MMBTU)'].pct_change() * 100, 2)
predictions_14day_df['pct_change price ($/MMBTU)'] = round(predictions_14day_df['price ($/MMBTU)'].pct_change() * 100, 2)
predictions_30day_df['pct_change price ($/MMBTU)'] = round(predictions_30day_df['price ($/MMBTU)'].pct_change() * 100, 2)
predictions_60day_df['pct_change price ($/MMBTU)'] = round(predictions_60day_df['price ($/MMBTU)'].pct_change() * 100, 2)

# Create Title
st.markdown("<h1 style='text-align: center;'>Natural Gas Spot Price Forecast</h1>", unsafe_allow_html=True)

# Create start date and slider for number of days 
default_start_date = predictions_7day_df.index[0] # Each dataframe has the same start date
start_date = st.date_input("Select Start Date", value=default_start_date)

# Option 1: Use date input for manually selecting the end date
end_date_manual = st.date_input("Select End Date (optional)", value=start_date)

# Convert start_date and end_date_manual to pandas.Timestamp
start_date = pd.to_datetime(start_date)
end_date_manual = pd.to_datetime(end_date_manual)

# Option 2: Use slider to adjust the range (affects the end date)
range_days = st.slider("Select Date Range (days)", min_value=7, max_value=365, value=60, step=1)

# Calculate end date based on slider
if end_date_manual >= start_date and end_date_manual <= pd.to_datetime(start_date) + pd.Timedelta(days=365):
    end_date = pd.to_datetime(start_date) + pd.Timedelta(days=range_days)
else:
    end_date = pd.to_datetime(end_date_manual)

# Filter each dataframe based on the date range
filtered_7day_df = predictions_7day_df[(predictions_7day_df.index >= start_date) & (predictions_7day_df.index <= end_date)]
filtered_14day_df = predictions_14day_df[(predictions_14day_df.index >= start_date) & (predictions_14day_df.index <= end_date)]
filtered_30day_df = predictions_30day_df[(predictions_30day_df.index >= start_date) & (predictions_30day_df.index <= end_date)]
filtered_60day_df = predictions_60day_df[(predictions_60day_df.index >= start_date) & (predictions_60day_df.index <= end_date)]

# Create Plotly figure
spot_prices = go.Figure()

# Create graph lines
def add_line(figure: go.Figure, y_value: str, df: pd.DataFrame, color: str, label: str, after_threshold: bool):
    ''' Function to create lines for visualisation '''
    threshold_date = predictions_7day_df.index[-7]  # Split at date when predictions start (same for all forecast horizons)

    if after_threshold is True:
        after_threshold = df[df.index >= threshold_date]
        # Add dashed line trace (after threshold)
        if not after_threshold.empty:
            figure.add_trace(go.Scatter(
                x=after_threshold.index,
                y=after_threshold[f'{y_value}'],
                mode='lines+markers',
                line=dict(color=f'{color}', width=2),  # 'dash' for a dashed line
                name=f"{label}",
            ))
    else:
        before_threshold = df[df.index <= threshold_date]
        if not before_threshold.empty:
            figure.add_trace(go.Scatter(
                x=before_threshold.index,
                y=before_threshold[f'{y_value}'],
                mode='lines',
                line=dict(color=f'{color}', width=2),
                name=f"{label}",
            ))


# Add forecast lines for each dataset
add_line(figure=spot_prices, y_value='price ($/MMBTU)', df=filtered_7day_df, color='#a9a9a9', label='Historical Price', after_threshold=False)
add_line(figure=spot_prices, y_value='price ($/MMBTU)', df=filtered_7day_df, color='red', label='7 day forecast', after_threshold=True)
add_line(figure=spot_prices, y_value='price ($/MMBTU)', df=filtered_14day_df, color='orange', label='14 day forecast', after_threshold=True)
add_line(figure=spot_prices, y_value='price ($/MMBTU)', df=filtered_30day_df, color='yellow', label='30 day forecast', after_threshold=True)
add_line(figure=spot_prices, y_value='price ($/MMBTU)', df=filtered_60day_df, color='pink', label='60 day forecast', after_threshold=True)


# Layout for sport prices visualisation
spot_prices.update_layout(
    title="Natural Gas Spot Prices",
    title_x=0.4,
    xaxis_title="Date",
    yaxis_title="Price ($/MMBTU)",
    xaxis=dict(
        range=[pd.to_datetime(start_date), end_date]  # Set the x-axis to range from start_date to end_date
    ),
    template="plotly_white",
    legend_title="Legend",
)

# Display spot prices visualisation in streamlit
st.plotly_chart(spot_prices, use_container_width=True)

pct_change_plot = go.Figure()
add_line(figure=pct_change_plot, y_value='pct_change price ($/MMBTU)', df=filtered_7day_df, color='#a9a9a9', label='Historical Price', after_threshold=False)
add_line(figure=pct_change_plot, y_value='pct_change price ($/MMBTU)', df=filtered_7day_df, color='#22a7f0', label='7 day forecast', after_threshold=True)
add_line(figure=pct_change_plot, y_value='pct_change price ($/MMBTU)', df=filtered_14day_df, color='#48b5c4', label='14 day forecast', after_threshold=True)
add_line(figure=pct_change_plot, y_value='pct_change price ($/MMBTU)', df=filtered_30day_df, color='#76c68f', label='30 day forecast', after_threshold=True)
add_line(figure=pct_change_plot, y_value='pct_change price ($/MMBTU)', df=filtered_60day_df, color='#a6d75b', label='60 day forecast', after_threshold=True)

# Customize layout
pct_change_plot.update_layout(
    title="% Changes Natural Gas Spot Prices",
    title_x=0.4,
    xaxis_title="Date",
    yaxis_title="% Change Price ($/MMBTU)",
    xaxis=dict(
        range=[pd.to_datetime(start_date), end_date]  # Set the x-axis to range from start_date to end_date
    ),
    template="plotly_white",
    legend_title="Legend",
)

# Display % changes visualisation in streamlit
st.plotly_chart(pct_change_plot, use_container_width=True)

# Display data in individual dataframes
st.markdown("<h3 style='text-align: center;'>7 Day Forecast Data</h3>", unsafe_allow_html=True)
st.dataframe(filtered_7day_df[['price ($/MMBTU)', 'pct_change price ($/MMBTU)']], use_container_width=True)

st.markdown("<h3 style='text-align: center;'>14 Day Forecast Data</h3>", unsafe_allow_html=True)
st.dataframe(filtered_14day_df[['price ($/MMBTU)', 'pct_change price ($/MMBTU)']], use_container_width=True)

st.markdown("<h3 style='text-align: center;'>30 Day Forecast Data</h3>", unsafe_allow_html=True)
st.dataframe(filtered_30day_df[['price ($/MMBTU)', 'pct_change price ($/MMBTU)']], use_container_width=True)

st.markdown("<h3 style='text-align: center;'>60 Day Forecast Data</h3>", unsafe_allow_html=True)
st.dataframe(filtered_60day_df[['price ($/MMBTU)', 'pct_change price ($/MMBTU)']], use_container_width=True)



