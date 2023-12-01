# Import necessary libraries
import streamlit as st
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import seaborn as sns
import plotly.express as px
import json
import requests

# Function to establish a Mysql database connection
def connect_to_database(database_name):
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="12345",
            database=database_name
        )
        print("Connected to the database successfully")
        return mydb
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None




#Funtions and Query's

# Function to fetch user data for the selected year
def fetch_user_data(selected_year, mydb):
    # Execute an SQL query to fetch user data for the selected year
    cursor = mydb.cursor()
    query = f"SELECT `Brand`, `Count`, `Percentage` FROM users_{selected_year}"
    cursor.execute(query)
    result = cursor.fetchall()

    # Create a DataFrame from the SQL result
    user_data = pd.DataFrame(result, columns=["Brand", "Count", "Percentage"])

    return user_data

# Function to fetch transaction data for the selected year
def fetch_transaction_data(selected_year, mydb):
    # Execute an SQL query to fetch transaction data for the selected year
    cursor = mydb.cursor()
    query = f"SELECT `Payment Type`, SUM(`Count`) as Count, SUM(`Amount`) as Amount FROM india_transactions_{selected_year} GROUP BY `Payment Type`"
    cursor.execute(query)
    result = cursor.fetchall()

    # Create a DataFrame from the SQL result
    transaction_data = pd.DataFrame(result, columns=["Payment Type", "Count", "Amount"])

    return transaction_data

# Function to fetch state-wise transaction data for the selected state and year
def fetch_state_transaction_data(selected_state, selected_year, mydb):
    cursor = mydb.cursor()
    query = f"SELECT `Payment Type`, SUM(`Count`) as Count, SUM(`Amount`) as Amount FROM `{selected_state}_transactions_{selected_year}` GROUP BY `Payment Type`"
    cursor.execute(query)
    result = cursor.fetchall()
    state_transaction_data = pd.DataFrame(result, columns=["Payment Type", "Count", "Amount"])
    return state_transaction_data

#Brand, Count, Percentage, Registered_Users, App_Opens
def fetch_state_users_data(selected_state,selected_year,mydb):
    cursor = mydb.cursor()
    query = f"SELECT `Brand` , SUM(`Count`) as Count, SUM(`Percentage`) as Percentage ,MAX(`Registered_Users`) as Registered_Users , MAX(`App_Opens`) as App_Opens FROM `{selected_state}_users_{selected_year}` GROUP BY `Brand`"
    cursor.execute(query)
    result = cursor.fetchall()
    state_users_data = pd.DataFrame(result,columns=["Brand", "Count", "Percentage", "Registered_Users", "App_Opens"])
    return state_users_data

#map overall transactions database
def fetch_data_for_year(selected_year, mydb, data_type):
    cursor = mydb.cursor()
    query = f"SELECT State, {data_type} FROM map_overall_transactions_{selected_year}"
    cursor.execute(query)
    result = cursor.fetchall()
    data = pd.DataFrame(result, columns=['State', data_type])
    return data

#map overall registeredusers ,appopens
def fetch_data_for_overall_users(selected_year_slider,mydb,data_type):
    cursor = mydb.cursor()
    query = f"SELECT State, {data_type} FROM  map_overall_users_{selected_year_slider}"  
    cursor.execute(query)
    result = cursor.fetchall()
    map_users_data = pd.DataFrame(result,columns=['State', data_type])
    return map_users_data

# Function to create a radar-style Choropleth map
def create_radar_map(data, geojson, featureidkey, data_type, title):
    fig = px.choropleth(
        data,
        geojson=geojson,
        featureidkey=featureidkey,
        locations='State',
        color=data_type,
        color_continuous_scale='Viridis',
        labels={data_type: title},
        hover_name='State',
        hover_data={data_type: ':,.2f'},
    )
    # Update map settings to make it radar-style
    fig.update_geos(
        projection_type="azimuthal equidistant",
        center={"lat": 24, "lon": 80},
        projection_rotation={"lon": 60, "lat": 10, "roll": 0},
        fitbounds="locations",
        visible=False
    )

    return fig

# Function to create a radar-style Choropleth map for(Regisetred users,APP opens)
def create_radar_map_two(map_users_data, geojson, featureidkey, data_type, title):
    fig = px.choropleth(
        map_users_data,
        geojson=geojson,
        featureidkey=featureidkey,
        locations='State',
        color=data_type,
        color_continuous_scale='RdBu',
        labels={data_type: title},
        hover_name='State',
        hover_data={data_type: ':,.2f'},
    )

    

    #map settings to make it radar-style
    fig.update_geos(
        projection_type="azimuthal equidistant",
        center={"lat": 24, "lon": 80},
        projection_rotation={"lon": 60, "lat": 10, "roll": 0},
        fitbounds="locations",
        visible=False
    )

    return fig

# Function to fetch statewise registered users
def statewise_users(selected_state, selected_year, mydb):
    cursor = mydb.cursor()
    query_map = f"SELECT State, District, SUM(`Registered Users`) as `Registered Users` FROM map_state_users WHERE Year = {selected_year} AND State = '{selected_state}' GROUP BY State, District"
    cursor.execute(query_map)
    data = cursor.fetchall()
    data_statewise_users = pd.DataFrame(data, columns=["State", "District", "Registered Users"])
    return data_statewise_users

# Function to fetch statewise App Opens
def fetch_appopens(selected_state, selected_year, mydb):
    cursor = mydb.cursor()
    query = f"SELECT State, District, SUM(`App Opens`) as `App Opens` FROM map_state_users WHERE Year = {selected_year} AND State = '{selected_state}' GROUP BY State, District"
    cursor.execute(query)
    data = cursor.fetchall()
    data_statewise_users_appopens = pd.DataFrame(data, columns=["State", "District", "App Opens"])
    return data_statewise_users_appopens

#Function for below Map transaction count statewise
def fetch_statewise_transaction_count(selected_year,selected_state,mydb):
    cursor=mydb.cursor()
    query = f"SELECT State, District, SUM(`Count`) AS Count FROM map_stateswise_transactions WHERE Year = {selected_year} AND State = '{selected_state}' GROUP BY State, District"
    cursor.execute(query)
    data = cursor.fetchall()
    data_statewise_count_transactions = pd.DataFrame(data,columns=["State" , "District" , "Count"])
    return data_statewise_count_transactions


#Function for below  Map Transaction Amount Statewise
def fetch_statewise_transaction_amount(selected_year,selected_state,mydb):
    cursor = mydb.cursor()
    query = f"SELECT State, District, SUM(`Amount`) AS Amount FROM map_stateswise_transactions WHERE Year = {selected_year} AND State = '{selected_state}' GROUP BY State, District"
    cursor.execute(query)
    data = cursor.fetchall()
    data_statewise_amount_transactions = pd.DataFrame(data,columns=["State" , "District", "Amount"])
    return data_statewise_amount_transactions


#query's for data overview page   --------------------------------------------------------------------------------------------------



# Function to fetch top transaction counts
def fetch_top_transaction_counts(year, category, mydb):
    cursor = mydb.cursor()
    query = f"SELECT Locations, Type, Count FROM top_transactions_year_wise WHERE Year = {year} AND Category = '{category}' ORDER BY Count DESC LIMIT 5"
    cursor.execute(query)
    data = cursor.fetchall()
    table_data = pd.DataFrame(data, columns=['Locations', 'Type', 'Count'])
    return table_data


#Function to fetch top transactions Amounts
def fetch_top_transaction_amount(year,category,mydb):
    cursor = mydb.cursor()
    query = f"SELECT Locations, Type, Amount From top_transactions_year_wise WHERE Year = {year} AND Category = '{category}' ORDER BY Amount DESC LIMIT 10"
    cursor.execute(query)
    data = cursor.fetchall()
    table_data_amount = pd.DataFrame(data, columns=['Locations', 'Type', 'Amount'])
    return table_data_amount

#Function to fetch count for north and south states
def fetch_count_north_south(year,category,mydb):
    cursor = mydb.cursor()
    query = f"SELECT Locations, Type ,Count from top_transactions_year_wise WHERE Year = {year} AND Category = 'State'"
    cursor.execute(query)
    data = cursor.fetchall()
    table_data_north_south = pd.DataFrame(data,columns=['Locations', 'Type', 'Count'])
    return table_data_north_south


#function to fetch which district,pincode has hightest count from states folder
def fetch_higest_count(year,category,mydb):
    cursor = mydb.cursor()
    query = f"SELECT Locations, Type, Count FROM top_transactions_year_wise WHERE Year = {year} AND Category = '{category}' ORDER BY Count DESC LIMIT 1"    
    cursor.execute(query)
    data = cursor.fetchall()
    table_topone_category = pd.DataFrame(data,columns=['Locations', 'Type' , 'Count'])
    return table_topone_category

#function to fetch which district,pincode has hightest amount from states folder
def fetch_highest_amount(year,category,mydb):
    cursor = mydb.cursor()
    query = f"SELECT Locations , Type , Amount  FROM top_transactions_year_wise WHERE Year = {year} AND Category = '{category}'  ORDER BY Amount DESC LIMIT 1 "
    cursor.execute(query)
    data = cursor.fetchall()
    table_topone_amount = pd.DataFrame(data,columns=['Locations', 'Type', 'Amount'])
    return table_topone_amount

#funtion to fetch high and least count state wise and catagory wise
def fetch_statewise_highcount(year, state, category, mydb):
    cursor = mydb.cursor()
    query = f"SELECT Locations, Type, Count FROM transactions_all_states WHERE Year = {year} AND Category = '{category}' AND State = '{state}' ORDER BY Count  DESC"
    cursor.execute(query)
    data = cursor.fetchall()
    table_statewise_top = pd.DataFrame(data, columns=['Locations', 'Type', 'Count'])
    return table_statewise_top

#Function to fetch high and least amount statewise and catagory wise
def fetch_statewise_highamount(year,state,category,mydb):
    cursor = mydb.cursor()
    query = f"SELECT Locations, Type, Amount FROM transactions_all_states WHERE Year = {year} AND Category = '{category}' AND State = '{state}' ORDER BY Amount  DESC"
    cursor.execute(query)
    data = cursor.fetchall()
    table_statewise_top_amount = pd.DataFrame(data, columns=['Locations', 'Type', 'Amount'])
    return table_statewise_top_amount
    






#Top users

def fetch_top_users(year,category,mydb):
    cursor = mydb.cursor()
    query = f"SELECT Locations, SUM(`RegisteredUsers`) AS `Registered Users` FROM top_users_year_wise WHERE Year = {year} AND Category = '{category}' GROUP BY Locations ORDER BY `Registered Users` DESC LIMIT 5"
    cursor.execute(query)
    data = cursor.fetchall()
    tabl_top_users = pd.DataFrame(data,columns=['Locations','Registered Users'])
    return tabl_top_users


def fetch_least_users(year,category,mydb):
    cursor = mydb.cursor()
    query = f"Select Locations , RegisteredUsers AS `Registered Users` FROM  top_users_year_wise WHERE Year = {year} AND Category = '{category}' ORDER BY `Registered Users` ASC LIMIT 1"
    cursor.execute(query)
    data = cursor.fetchall()
    tabl_least_users = pd.DataFrame(data,columns=['Locations','Registered Users'])
    return tabl_least_users


   




# Establish a database connection with the databases in mysql 
users_db = connect_to_database("users")
transactions_db = connect_to_database("transaction")
transactions_states_db = connect_to_database("india_state_transaction")
users_states_db = connect_to_database("india_state_users")
map_overall_transactions_db = connect_to_database("map_overall_transactions")
map_overall_users_db = connect_to_database("map_overall_users")
map_statewise_users_db=connect_to_database("map_users_state_db")




# Sidebar for user interaction for pages 
st.sidebar.title("Select Data Type")
selected_data_type = st.sidebar.selectbox("Choose Data Type", ["Introduction","Users", "Transactions","Data Visualization","Data Overview"])




# Introduction page 
if selected_data_type == "Introduction":
    
    # Define PhonePe brand colors
    phonepe_colors = {
        "primary": "#15AB2A",  # Green
        "secondary": "#EC1C24",  # Red
        "accent": "#FFD101",  # Yellow
    }

    # Custom CSS for styling
    custom_css = f"""
        <style>
            .phonepe-primary-text {{
                color: {phonepe_colors["primary"]};
            }}
            .phonepe-secondary-text {{
                color: {phonepe_colors["secondary"]};
            }}
            .phonepe-accent-text {{
                color: {phonepe_colors["accent"]};
            }}
        </style>
    """

    # Render custom CSS
    st.markdown(custom_css, unsafe_allow_html=True)

    # Introduction page content
    st.title("Welcome to PhonePe Pulse Analytics")
    st.markdown(
        """
        <p class="phonepe-primary-text">PhonePe Pulse is your personalized financial insight center.</p>
        <p class="phonepe-secondary-text">Understand Your Transactions, Manage Your Finances!</p>
        """
        ,
        unsafe_allow_html=True
    )

    # Introduction about PhonePe Pulse
    st.header("Introduction About PhonePe Pulse:")
    st.write(
        """
        PhonePe Pulse is a feature within the PhonePe mobile payments app in India, offering insights and trends on user transactions and spending behavior. Users access PhonePe Pulse to understand their financial activities and track spending habits.
        """
    )

    # PhonePe Payment Types
    st.header("PhonePe Payment Types:")
    st.markdown(
        """
        - **Peer-to-Peer Payments:** Users can send money to friends and family.
            - *Examples:* Splitting a meal bill, sharing expenses, or assisting someone in need.

        - **Recharge & Bill Payments:** Users can conveniently pay utility bills, mobile recharges, and more.
            - *Examples:* Settling electricity, water, gas bills, mobile top-ups, and credit card payments.

        - **Merchant Payments:** Users can make purchases and payments at various merchants and businesses, online and offline.
            - *Examples:* Grocery shopping, online retail, dining at restaurants, or paying for transportation services.

        - **Financial Services:** PhonePe offers a range of financial services, including mutual fund investments, insurance purchases, and digital gold buying.
            - *Examples:* Investing in mutual funds, acquiring health or life insurance, and purchasing digital gold.

        - **Others:** This category covers miscellaneous transactions and services.
            - *Examples:* Donations to charities, online shopping returns, or unique financial transactions.
        """
    )

    # Objective
    st.header("Objective:")
    st.write(
        """
        The project aims to extract, process, and visualize data from the PhonePe Pulse GitHub repository. The primary objectives include data extraction, transformation, MySQL database storage, and the creation of an interactive geo visualization dashboard using Streamlit and Plotly. Users can explore various metrics and statistics with dropdown options.
        """
    )

    # Data Sources
    st.header("Data Sources:")
    st.write(
        """
        The primary data source is the PhonePe Pulse GitHub repository, containing transaction and spending behavior data.
        """
    )

    # Scope
    st.header("Scope:")
    st.write(
        """
        The project scope includes data extraction, transformation, MySQL database storage, dashboard development, data integration, user interaction, security, and efficiency.
        """
    )

    # Methodology
    st.header("Methodology:")
    st.write(
        """
        - **Data Loading:** Loaded data from the Git repository into Pandas DataFrames.
        - **Data Transformation:** Cleaned and prepare data, handle missing values, and ensure data consistency.
        - **Data Storage:** Created a MySQL database to store processed data securely.
        - **Visualization Dashboard:** Developed an interactive dashboard with dropdown menus and interactive components.
        - **Data Integration:** Connected the dashboard to the MySQL database for data retrieval.
        - **Data Visualization:** Generated charts and maps using Matplotlib and Seaborn.
        - **Security and Efficiency:** Ensured data security and optimize data retrieval and visualization.
        - **User-Friendly Interface:** Designed a visually appealing and user-friendly dashboard.
        """
    )

    # Benefits
    st.header("Benefits:")
    st.write(
        """
        The project provides users with insightful financial tracking, a user-friendly dashboard, a personalized experience, data security, convenient access to financial insights, and efficient financial management.
        """
    )

#============================================================================================================================================

# Check if the user selected "Users" data type
if selected_data_type == "Users":
    
    # Add a selectbox to choose the year within the "Users" section
    selected_year = st.selectbox("Choose a Year", ["2018", "2019", "2020", "2021", "2022", "2023"])

    # Fetch user data based on the selected year
    user_data = fetch_user_data(selected_year, users_db)

    # Create a bar plot for user data
    st.subheader(f"User Data for {selected_year}")
    fig1, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 8))

    # First subplot for 'Count'
    user_data.plot(kind='bar', x='Brand', y='Count', ax=ax1)
    ax1.set_xlabel('Brand')
    ax1.set_ylabel('Count')
    ax1.set_title(f'User Data for {selected_year}')

    # Format y-axis labels for 'Count'
    def format_count(value, _):
        if value >= 1e6:
            return f'{int(value / 1e6)}M'
        elif value >= 1e3:
            return f'{int(value / 1e3)}K'
        else:
            return str(int(value))

    ax1.get_yaxis().set_major_formatter(FuncFormatter(format_count))
    ax1.tick_params(axis='x', labelrotation=45)  # Rotate x-axis labels for better readability

    # Second subplot for 'Percentage'
    sns.barplot(data=user_data, x='Brand', y='Percentage', ax=ax2)
    ax2.set_xlabel('Brand')
    ax2.set_ylabel('Percentage')
    ax2.set_title(f'Percentage of User Data for {selected_year}')
    ax2.tick_params(axis='x', labelrotation=45)  # Rotate x-axis labels for better readability

    plt.tight_layout()

    # Display the subplots using Streamlit
    st.pyplot(fig1)

    # Add a selectbox to choose a state
    selected_state = st.selectbox("Choose a State", ["Andaman", "Andhra_pradesh","Arunachal_pradesh","Assam","Bihar","Chandigarh","Dadra_nagar_haveli_daman_diu","Delhi","Goa","Gujarat","Haryana","Himachal_pradesh","Jammu_kashmir","Jharkhand","Karnataka","Kerala","Ladakh","Lakshadweep","Madhya_pradesh","Maharashtra","Manipur","Meghalaya","Mizoram","Nagaland","Odisha","Puducherry","Punjab","Rajasthan","Sikkim","Tamil_nadu","Telangana","Tripura","Uttar_pradesh","Uttarakhand","West_bengal"])
    
    if selected_state != "Other States":
       
            
        # Fetch state-wise transaction data based on the selected state and year
        state_users_data = fetch_state_users_data(selected_state, selected_year, users_states_db)
        
        # Create side-by-side pie plots for Registered Users and App Opens
        st.subheader(f"User Data for {selected_state}, {selected_year}")
        fig4, (ax5, ax6) = plt.subplots(1, 2, figsize=(10, 6))
        

        # Registered Users(pie chart)
        registered_users_value = state_users_data['Registered_Users'].iloc[0]  # Assuming the value is the same for all rows
        ax5.pie([registered_users_value], labels=[f'Registered Users'], startangle=90,colors='red', wedgeprops={'edgecolor': 'black', 'linewidth': 1, 'linestyle': 'solid', 'antialiased': True})
        ax5.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
        ax5.set_title(f'Registered Users for {selected_state}, {selected_year}')
        ax5.text(0, 0, registered_users_value, fontsize=18, color='white', va='center', ha='center')

        # App Opens([pie chart])
        app_opens_value = state_users_data['App_Opens'].iloc[0]  # Assuming the value is the same for all rows
        ax6.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        ax6.set_title(f'App Opens for {selected_state}, {selected_year}')
        if app_opens_value > 0:
            ax6.pie([app_opens_value], labels=[f'App Opens'], startangle=90,colors = ['#FFD300', 'lightblue', 'lightcoral'],wedgeprops={'edgecolor': 'black', 'linewidth': 1, 'linestyle': 'solid', 'antialiased': True})
            ax6.text(0, 0, app_opens_value, fontsize=18, color='black', va='center', ha='center')
        else:
            # Display a blank pie plot with "0" label
            ax6.pie([1], labels=['App Opens'], startangle=90)
            ax6.text(0, 0, '0', fontsize=18, color='white', va='center', ha='center')

        plt.tight_layout()

        # Display the subplots using Streamlit
        st.pyplot(fig4)

        #Bar plot side by side
        fig5,(ax7,ax8) =  plt.subplots(1,2,figsize=(8,6))

        #Brand and count
        sns.barplot(data = state_users_data,x='Brand',y='Count',ax=ax7)
        ax7.set_xlabel('Brands')
        ax7.set_ylabel('Count')
        ax7.set_title(f'Count of {selected_state} for Year {selected_year}')
        ax7.tick_params(axis='x',labelrotation=75)


        #Brand and percentage
        sns.barplot(data = state_users_data,x='Brand',y='Percentage',ax=ax8)
        ax8.set_xlabel("Brands")
        ax8.set_ylabel("Percentage")
        ax8.set_title(f"Percentage of {selected_state} for Year {selected_year}")
        ax8.tick_params(axis='x',labelrotation=75)

        plt.tight_layout()

        # Display the subplots using Streamlit
        st.pyplot(fig5)


#=========================================================================================================================================


#Transactions


# Check if the user selected "Transactions" data type

if selected_data_type == "Transactions":
    
    # Add a selectbox to choose the year within the "Transactions" section
    selected_year = st.selectbox("Choose a Year", ["2018", "2019", "2020", "2021", "2022", "2023"])

    # Fetch transaction data based on the selected year
    transaction_data = fetch_transaction_data(selected_year, transactions_db)

    # Create a bar plot for transaction data
    st.subheader(f"Transaction Data for {selected_year}")
    fig2, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 8), gridspec_kw={'hspace': 1.10})

    # First subplot for 'Count'
    sns.barplot(data=transaction_data, x='Payment Type', y='Count', ax=ax1)
    ax1.set_xlabel('Payment Type')
    ax1.set_ylabel('Count (in millions)')
    ax1.set_title(f'Count of Payment Types for {selected_year}')
    ax1.tick_params(axis='x', labelrotation=45)  # Rotate x-axis labels for better readability

    # Format y-axis labels for 'Count'
    def format_count(value, _):
        if value >= 1e6:
            return f'{int(value / 1e6)}M'
        elif value >= 1e3:
            return f'{int(value / 1e3)}K'
        else:
            return str(int(value))

    ax1.get_yaxis().set_major_formatter(FuncFormatter(format_count))

    # Second subplot for 'Amount'
    sns.barplot(data=transaction_data, x='Payment Type', y='Amount', ax=ax2)
    ax2.set_xlabel('Payment Type')
    ax2.set_ylabel('Amount (in millions)')
    ax2.set_title(f'Amount of Payment Types for {selected_year}')
    ax2.tick_params(axis='x', labelrotation=45)  # Rotate x-axis labels for better readability

    # Format y-axis labels for 'Amount'
    def format_amount(value, _):
        if value >= 1e6:
            return f'{int(value / 1e6)}M'
        elif value >= 1e3:
            return f'{int(value / 1e3)}K'
        else:
            return str(int(value))

    ax2.get_yaxis().set_major_formatter(FuncFormatter(format_amount))

    plt.tight_layout()

    # Display the subplots using Streamlit
    st.pyplot(fig2)

    # Add a selectbox to choose a state
    selected_state = st.selectbox("Choose a State", ["Andaman", "Andhra_pradesh","Arunachal_pradesh","Assam","Bihar","Chandigarh","Dadra_nagar_haveli_daman_diu","Delhi","Goa","Gujarat","Haryana","Himachal_pradesh","Jammu_kashmir","Jharkhand","Karnataka","Kerala","Ladakh","Lakshadweep","Madhya_pradesh","Maharashtra","Manipur","Meghalaya","Mizoram","Nagaland","Odisha","Puducherry","Punjab","Rajasthan","Sikkim","Tamil_nadu","Telangana","Tripura","Uttar_pradesh","Uttarakhand","West_bengal"])
    
    if selected_state != "Other States":
        
        # Fetch state-wise transaction data based on the selected state and year
        state_transaction_data = fetch_state_transaction_data(selected_state, selected_year, transactions_states_db)
        
        # Create a bar plot for state-wise transaction data
        st.subheader(f"Transaction Data for {selected_state}, {selected_year}")
        fig3, (ax3, ax4) = plt.subplots(2, 1, figsize=(8, 8), gridspec_kw={'hspace': 1.10})
        
        # First subplot for 'Count'
        sns.barplot(data=state_transaction_data, x='Payment Type', y='Count', ax=ax3)
        ax3.set_xlabel('Payment Type')
        ax3.set_ylabel('Count (in millions)')
        ax3.set_title(f'Count of Payment Types for {selected_state}, {selected_year}')
        ax3.tick_params(axis='x', labelrotation=45)  # Rotate x-axis labels for better readability
        
        # Format y-axis labels for 'Count'
        def format_count(value, _):
            if value >= 1e6:
                return f'{int(value / 1e6)}M'
            elif value >= 1e3:
                return f'{int(value / 1e3)}K'
            else:
                return str(int(value))
        
        ax3.get_yaxis().set_major_formatter(FuncFormatter(format_count))
        
        # Second subplot for 'Amount'
        sns.barplot(data=state_transaction_data, x='Payment Type', y='Amount', ax=ax4)
        ax4.set_xlabel('Payment Type')
        ax4.set_ylabel('Amount (in millions)')
        ax4.set_title(f'Amount of Payment Types for {selected_state}, {selected_year}')
        ax4.tick_params(axis='x', labelrotation=45)  # Rotate x-axis labels for better readability
        
        # Format y-axis labels for 'Amount'
        def format_amount(value, _):
            if value >= 1e6:
                return f'{int(value / 1e6)}M'
            elif value >= 1e3:
                return f'{int(value / 1e3)}K'
            else:
                return str(int(value))
        
        ax4.get_yaxis().set_major_formatter(FuncFormatter(format_amount))
        
        plt.tight_layout()
        
        # Display the subplots using Streamlit
        st.pyplot(fig3)


if selected_data_type == "Data Visualization":
    
    # Fetch the state map data from the URL using requests
    india_states_map_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(india_states_map_url)
    india_states_map = response.json()

    # Create a Streamlit app
    st.title("Indian States Wise Choropleth Maps")

    #slider for years
    selected_year_slider = st.select_slider("Choose a Year", options=["2018", "2019", "2020", "2021", "2022", "2023"])

    
    # Fetch data for the amount map
    data_amount = fetch_data_for_year(selected_year_slider, map_overall_transactions_db, "Amount")
    
    # Create a radar-style map for the amount
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader(f"Overall Transaction Amount Indian State Wise for Year {selected_year_slider}")
        st.write("Amount represents the overall transaction amount for each state")
        st.plotly_chart(create_radar_map(data_amount, india_states_map, 'properties.ST_NM', 'Amount', 'Amount'), use_container_width=True)

    # Fetch data for the count map
    data_count = fetch_data_for_year(selected_year_slider, map_overall_transactions_db, "Count")

    # Create a radar-style map for the count
    with col2:
        st.subheader(f"Overall Transaction Count Indian State Wise for Year {selected_year_slider}")
        st.write("Count represents the overall transaction count for each state")
        st.plotly_chart(create_radar_map(data_count, india_states_map, 'properties.ST_NM', 'Count', 'Count'), use_container_width=True)



    col3,col4 =st.columns(2)

    map_registeredusers = fetch_data_for_overall_users(selected_year_slider,map_overall_users_db, "RegisteredUsers") 

    
    with col3:
        st.subheader(f"Overall Registered Users Indian State Wise for Year {selected_year_slider}")
        st.write("Registered Users represents the overall registered users of phonepay pulse for each state")
        st.plotly_chart(create_radar_map_two(map_registeredusers, india_states_map, 'properties.ST_NM', 'RegisteredUsers', 'Registered Users'), use_container_width=True)


    map_app_opens=fetch_data_for_overall_users(selected_year_slider,map_overall_users_db, "AppOpens")

    with col4:
        st.subheader(f"Overall App Opens Indian State Wise for Year {selected_year_slider} ")
        st.write("App Opens represents the overall app opens of phonepay pulse for each state")
        st.plotly_chart(create_radar_map_two(map_app_opens, india_states_map, 'properties.ST_NM', 'AppOpens', 'App Opens'), use_container_width=True)

    
    
    # Get user input for the year and state
    selected_year = st.selectbox("Choose a Year", ["2018", "2019", "2020", "2021", "2022", "2023"], key="year_selector")
    selected_state = st.selectbox("Choose a State", ["Andaman & Nicobar Islands", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chandigarh", "Dadra & Nagar Haveli & Daman & Diu", "Delhi", "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jammu & Kashmir", "Jharkhand", "Karnataka", "Kerala", "Ladakh", "Lakshadweep", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal"], key="state_selector")

    # Map statewise users data
    st.title(f"Registered Users and App Opens for Districts of  {selected_state} in year {selected_year} ")

    # Connect to the database
    mydb = connect_to_database("map_users_state_db")

    if mydb:
        data_statewise_users = statewise_users(selected_state, selected_year, mydb)
        data_statewise_users_appopens = fetch_appopens(selected_state, selected_year, mydb)

        if not data_statewise_users.empty:
            # Set the style for the plot
            sns.set(style="whitegrid")

            # Create a bar plot for Registered Users
            plt.figure(figsize=(12, 6))
            ax = sns.barplot(x='District', y='Registered Users', data=data_statewise_users)

            # Format the y-axis labels as integers (not scientific notation)
            ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

            # Customize the plot labels and title
            ax.set(xlabel="Districts", ylabel="Registered Users")
            plt.title(f'Registered Users by District for {selected_state} in {selected_year}')

            # Rotate x-axis labels for better visibility
            plt.xticks(rotation=90)

            # Display the plot using Streamlit
            st.pyplot(plt)
        else:
            st.write("No data available for Registered Users in the selected state and year.")

        if not data_statewise_users_appopens.empty:
            # Set the style for the plot
            sns.set(style="whitegrid")

            # Create a bar plot for App Opens
            plt.figure(figsize=(12, 6))
            ax7 = sns.barplot(x='District', y='App Opens', data=data_statewise_users_appopens)

            # Format the y-axis labels as integers (not scientific notation)
            ax7.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

            # Customize the plot labels and title
            ax7.set(xlabel="Districts", ylabel="App Opens")
            plt.title(f'App Opens by District for {selected_state} in {selected_year}')

            # Rotate x-axis labels for better visibility
            plt.xticks(rotation=90)

            # Display the plot using Streamlit
            st.pyplot(plt)
        else:   
            st.write("No data available for App Opens in the selected state and year")

        # # Close the database connection
        # mydb.close()

    else:
        st.write("Failed to connect to the database.")



    # Title for the transaction section
    st.title(f"Count  and Amount for Districts of  {selected_state} in year {selected_year} ")

    # Connect to the database
    mydb = connect_to_database("map_states_transactions")

    if mydb:
        data_statewise_count_transactions = fetch_statewise_transaction_count(selected_year, selected_state, mydb)
        data_statewise_amount_transactions = fetch_statewise_transaction_amount(selected_year, selected_state, mydb)

        if not data_statewise_count_transactions.empty:
            sns.set(style="whitegrid")

            plt.figure(figsize=(12, 6))
            ax8 = sns.barplot(x='District', y='Count', data=data_statewise_count_transactions)

            ax8.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

            ax8.set_xlabel("Districts")
            ax8.set_ylabel("Count")

            plt.title(f"Count by District for {selected_state} in {selected_year}")

            plt.xticks(rotation=90)

            st.pyplot(plt)
        else:
            st.write("No data available for Count in the selected state and year")

        if not data_statewise_amount_transactions.empty:
            sns.set(style="whitegrid")

            plt.figure(figsize=(12, 6))
            ax9 = sns.barplot(x='District', y='Amount', data=data_statewise_amount_transactions)

            ax9.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

            ax9.set_xlabel("Districts")
            ax9.set_ylabel("Amount")

            plt.title(f"Amount by District for {selected_state} in {selected_year}")

            plt.xticks(rotation=90)

            st.pyplot(plt)
        else:
            st.write("No data available for Amount in the selected state and year")

    else:
        st.write("Failed to connect to the database")

#========================================================================================================================================

if selected_data_type == "Data Overview":

    # selecting the year (2018-2023)
    year = st.sidebar.number_input("Select a year (2018-2023)", min_value=2018, max_value=2023, key="year")

    # selecting the category (States, Districts, Pin Codes)
    categories = ["State", "District", "Pincode"]
    category = st.sidebar.selectbox("Select a category", categories, key="category")

    # State to perform state-related queries
    state = st.sidebar.selectbox("Choose a State", [
        "andaman-&-nicobar-islands", "andhra-pradesh", "arunachal-pradesh", "assam", "bihar", "chandigarh", "chhattisgarh",
        "dadra-&-nagar-haveli-&-daman-&-diu", "delhi", "goa", "gujarat", "haryana", "himachal-pradesh", "jammu-&-kashmir",
        "jharkhand", "karnataka", "kerala", "ladakh", "lakshadweep", "madhya-pradesh", "maharashtra", "manipur", "meghalaya",
        "mizoram", "nagaland", "odisha", "puducherry", "punjab", "rajasthan", "sikkim", "tamil-nadu", "telangana",
        "tripura", "uttar-pradesh", "uttarakhand", "west-bengal"], key="state_selector_top")
    

    st.markdown("<h2 style='text-align :center; color:purple;'>Transaction Data Analysis</h2",unsafe_allow_html = True)

    mydb = connect_to_database("top_overall_transactions")  #states districts pincodes 

    if mydb:
        # st.write("Connected to Data Base")
        

        questions = [
            f"Top 5 Overall Transaction Counts in Year {year} for {category}",
            f"Top 10 Overall Transaction Amounts in Year {year} for {category}",
            f"What are Overall Transaction Counts in North Indian states and  South Indian states in Year {year}",
            f"Which {category} had the Highest Total Transaction count in the {year}, and what was the Count?",
            f"Which {category} had the Highest Total Transaction Amount in the {year}, and what was the Amount?",
            f"In {state} which {category} has Highest and Least Transaction Count in the {year}, and what was the Count?",
            f"In {state} which {category} has Highest and Least Transaction Amount in the {year}, and what was the Amount?"

            
        ]

        selected_question = st.selectbox("Select a question", questions)

        if selected_question == f"Top 5 Overall Transaction Counts in Year {year} for {category}":
            table_data = fetch_top_transaction_counts(year, category, mydb)
            st.write(table_data)
        
        elif selected_question == f"Top 10 Overall Transaction Amounts in Year {year} for {category}":
            table_data_amount = fetch_top_transaction_amount(year,category,mydb)
            st.write(table_data_amount)

        elif selected_question == f"What are Overall Transaction Counts in North Indian states and  South Indian states in Year {year}":

            table_data_north_south = fetch_count_north_south(year, category, mydb)

            # lists of North and South Indian states
            north_states = ['maharashtra', 'uttar pradesh', 'rajasthan', 'madhya pradesh', 'bihar', 'odisha', 'west bengal']
            south_states = ['karnataka', 'telangana', 'andhra pradesh', 'tamil nadu']


            north_states_df = table_data_north_south[table_data_north_south['Locations'].str.strip().isin(north_states)]
            south_states_df = table_data_north_south[table_data_north_south['Locations'].str.strip().isin(south_states)]


            st.write(north_states_df)
            st.write(south_states_df)


            # Sum counts for North states
            north_states_count = north_states_df['Count'].sum()
            south_states_count = south_states_df['Count'].sum()

        
            st.write(f"Total Transaction Counts in North Indian states: {north_states_count}")
            st.write(f"Total Transaction Counts in South Indian states: {south_states_count}")


        elif selected_question == f"Which {category} had the Highest Total Transaction count in the {year}, and what was the Count?":
            
            table_topone_category = fetch_higest_count(year,category,mydb)
            st.write(table_topone_category)

        elif selected_question == f"Which {category} had the Highest Total Transaction Amount in the {year}, and what was the Amount?":

            table_topone_amount=fetch_highest_amount(year,category,mydb)
            st.write(table_topone_amount)
        
        elif selected_question == f"In {state} which {category} has Highest and Least Transaction Count in the {year}, and what was the Count?":

            table_statewise_top = fetch_statewise_highcount(year, state, category, mydb)

            # pivot table
            heatmap = table_statewise_top.pivot(index='Locations', columns='Type', values='Count')

            # heatmap using Seaborn
            plt.figure(figsize=(12, 8))  # Adjust the figure size as needed
            sns.heatmap(heatmap, annot=True, fmt="", cmap="YlGnBu")

           
            plt.xlabel('Type')
            plt.ylabel(f"{category}")
            plt.title(f'Heatmap of Transaction Counts at {state}  in {year}')

            st.pyplot(plt)

        elif selected_question == f"In {state} which {category} has Highest and Least Transaction Amount in the {year}, and what was the Amount?":
            
            table_statewise_top_amount = fetch_statewise_highamount(year,state,category,mydb)
            
            plt.figure(figsize = (12,6))

            cus_palette = ["#0C356A" ,"#0174BE", "#FFC436", "#FFF0CE","#ECE3CE","#739072","#4F6F52","#3A4D39" ,"#61A3BA" ,"#FFFFDD","#D2DE32","#A2C579" , "#CE5A67" ,"#1F1717" ,"#495E57","#45474B"]
    
            sns.set_palette(cus_palette)

            
            ax10 = sns.barplot(data = table_statewise_top_amount,x ='Locations' , y = 'Amount')

            ax10.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

            ax10.set_xlabel(f"{category}")
            ax10.set_ylabel("Amount")

            plt.title(f"Amount by {category} for {state} in {year}")

            plt.xticks(rotation=70)

            st.pyplot(plt)

    else:
        st.write("Database connection failed")


   



#Users Data Analysis Code

    st.markdown("<br><br>", unsafe_allow_html=True)


    st.markdown("<h2 style='text-align: center; color: purple;'>Users Data Analysis</h2>", unsafe_allow_html=True)

    mydb = connect_to_database("top_users_data")

    if mydb:

        questions = [
            f"Top 5 Overall {category} Registered Users for Year {year}",
            f"Which {category} has the least Registered Users for Year {year}"
        ]

        selected_question_users = st.selectbox("Select a question", questions)

        if selected_question_users == f"Top 5 Overall {category} Registered Users for Year {year}":


            tabl_top_users = fetch_top_users(year,category,mydb)
            st.write(tabl_top_users)

        if selected_question_users == f"Which {category} has the least Registered Users for Year {year}":

            tabl_least_users = fetch_least_users(year,category,mydb)
            st.write(tabl_least_users)






















    

# Display a DataFrame table for more details
if selected_data_type == "Users":
    st.write(f"Overall User Data for {selected_year}")
    st.write(user_data)
    st.write(f"User Data of {selected_state} for Year {selected_year}")
    st.write(state_users_data)
elif selected_data_type == "Transactions":
    st.write(f"Transaction Data for {selected_year}")
    st.write(transaction_data)
    st.write(f"Transaction Data for {selected_state}, {selected_year}")
    st.write(state_transaction_data)




