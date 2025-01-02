import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc, year

# Initialize Spark session
spark = SparkSession.builder.appName("Spark Dashboard").getOrCreate()

# Read the CSV file (ensure the path is correct)
df1 = spark.read.csv(r"E:\\shahmeerdockerwd\\shahmeerhadoop\\docker-spark\\updated.csv", header=True, inferSchema=True)

# Add a Year column if "Load Time" exists
if "Load Time" in df1.columns:
    df1 = df1.withColumn("Year", year(df1["Load Time"]))

# Streamlit Interface
st.title("Agriculture Data Dashboard")
st.sidebar.title("Select a Query")

# Sidebar options for queries
query_options = [
    "Crops with Highest Production",
    "Agriculture Production by Commodity and Utilization",
    "Relationship Between Harvested Area and Practices",
    "State and County Contributions Over Time",
    "Production Statistics by Year and Region",
    "Underutilized Agricultural Regions"
]
selected_query = st.sidebar.selectbox("Choose a Query:", query_options)

# Query 1: Crops with Highest Production
if selected_query == "Crops with Highest Production":
    crop_production_df = df1.groupBy("Commodity") \
        .agg(sum("Value (Production/Utilization)").alias("Total_Production")) \
        .orderBy(desc("Total_Production"))

    crop_production_pandas = crop_production_df.toPandas()
    st.subheader("Crops with Highest Production")
    st.write(crop_production_pandas)

    # Bar plot
    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=crop_production_pandas.head(10),
        x="Total_Production",
        y="Commodity",
        palette="magma"
    )
    plt.title("Top 10 Crops by Production")
    plt.xlabel("Total Production")
    plt.ylabel("Commodity")
    st.pyplot(plt)

# Query 2: Agriculture Production by Commodity and Utilization
elif selected_query == "Agriculture Production by Commodity and Utilization":
    commodity_utilization_df = df1.groupBy("Commodity", "Utilization Practice") \
        .agg(sum("Value (Production/Utilization)").alias("Total_Production")) \
        .orderBy(desc("Total_Production"))

    commodity_utilization_pandas = commodity_utilization_df.toPandas()
    st.subheader("Production by Commodity and Utilization Practice")
    st.write(commodity_utilization_pandas)

    # Heatmap
    pivot_table = commodity_utilization_pandas.pivot(
        index="Commodity", columns="Utilization Practice", values="Total_Production"
    )
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot_table, cmap="YlGnBu", annot=False)
    plt.title("Heatmap of Production by Commodity and Utilization Practice")
    st.pyplot(plt)

# Query 3: Relationship Between Harvested Area and Practices (Top 15)
elif selected_query == "Relationship Between Harvested Area and Practices":
    area_practice_df = df1.groupBy("Production Practice") \
        .agg(sum("Value (Production/Utilization)").alias("Total_Harvested_Area")) \
        .orderBy(desc("Total_Harvested_Area"))

    area_practice_pandas = area_practice_df.toPandas()
    st.subheader("Top 15 Harvested Areas by Production Practice")
    st.write(area_practice_pandas.head(15))

    # Horizontal bar chart for the top 15
    plt.figure(figsize=(10, 8))
    sns.barplot(
        data=area_practice_pandas.head(15).sort_values(by="Total_Harvested_Area", ascending=False),
        x="Total_Harvested_Area",
        y="Production Practice",
        palette="viridis"
    )
    plt.title("Top 15 Harvested Areas by Production Practices")
    plt.xlabel("Total Harvested Area")
    plt.ylabel("Production Practice")
    st.pyplot(plt)

# Query 4: State and County Contributions Over Time (Pie Chart)
elif selected_query == "State and County Contributions Over Time":
    state_county_trend_df = df1.groupBy("State Name") \
        .agg(sum("Value (Production/Utilization)").alias("Total_Production")) \
        .orderBy(desc("Total_Production"))

    state_county_trend_pandas = state_county_trend_df.toPandas()
    st.subheader("Top 10 State Contributions")
    top_10_states = state_county_trend_pandas.head(10)
    st.write(top_10_states)

    # Pie chart for top 10 states
    plt.figure(figsize=(10, 8))
    plt.pie(
        top_10_states["Total_Production"],
        labels=top_10_states["State Name"],
        autopct='%1.1f%%',
        startangle=140,
        colors=sns.color_palette("tab10")
    )
    plt.title("Top 10 State Contributions (Total Production)")
    st.pyplot(plt)

# Query 5: Production Statistics by Year and Region
elif selected_query == "Production Statistics by Year and Region":
    region_year_df = df1.groupBy("ASD Description", "Year") \
        .agg(sum("Value (Production/Utilization)").alias("Total_Production")) \
        .orderBy("Year", desc("Total_Production"))

    region_year_pandas = region_year_df.toPandas()
    st.subheader("Production Statistics by Region and Year")
    st.write(region_year_pandas)

    # Line plot by region
    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=region_year_pandas,
        x="Year",
        y="Total_Production",
        hue="ASD Description",
        palette="tab10"
    )
    plt.title("Production Trends by Region")
    plt.xlabel("Year")
    plt.ylabel("Total Production")
    st.pyplot(plt)

# Query 6: Underutilized Agricultural Regions
elif selected_query == "Underutilized Agricultural Regions":
    underutilized_df = df1.filter(df1["Value (Production/Utilization)"] < 10000) \
        .groupBy("County Name") \
        .agg(sum("Value (Production/Utilization)").alias("Total_Underutilized")) \
        .orderBy(desc("Total_Underutilized"))

    underutilized_pandas = underutilized_df.toPandas()
    st.subheader("Underutilized Agricultural Regions")
    st.write(underutilized_pandas)

    # Pie chart
    plt.figure(figsize=(10, 8))
    plt.pie(
        underutilized_pandas["Total_Underutilized"],
        labels=underutilized_pandas["County Name"],
        autopct='%1.1f%%',
        startangle=140
    )
    plt.title("Underutilized Agricultural Regions")
    st.pyplot(plt)