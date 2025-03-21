# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col

# Write directly to the app
st.title(":cup_with_straw: Customize Your Smoothie!:cup_with_straw:")
st.write("Choose the fruits you want in your custom Smoothie!")

# Get user input
name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your Smoothie will be:", name_on_order)

# Establish Snowflake connection
cnx = st.connection("snowflake")
session = cnx.session()

# Fetch fruit options from Snowflake
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))

# Display multiselect for ingredients
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    my_dataframe.to_pandas()['FRUIT_NAME'].tolist()
)

# Process order
if ingredients_list:
    ingredients_string = ', '.join(ingredients_list)  # Cleaner way to join ingredients

    # Check your table structure before adjusting the INSERT statement
    table_columns = [col.name for col in session.table("smoothies.public.orders").schema.fields]

    if 'NAME_ON_ORDER' in table_columns:  # If 'NAME_ON_ORDER' is a valid column
        my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        VALUES ('{ingredients_string}', '{name_on_order}')
        """
    else:  # If only one column (ingredients)
        my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients)
        VALUES ('{ingredients_string} - {name_on_order}')
        """

    # Submit order
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered!', icon="✅")
import requests
smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/watermelon")
st.text(smoothiefroot_response)
st.stop()
