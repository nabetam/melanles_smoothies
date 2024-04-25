# Import python packages
from snowflake.snowpark.functions import col,when_matched
import streamlit as st
from snowflake.snowpark.context import get_active_session


# Write directly to the app
st.title("Pending Smoothie Orders :balloon:")
st.write(
    """Orders that need to filled!
    """
)

#name_on_order = st.text_input('Name on smoothie:')
#st.write('The name on your Smootie will be:', name_on_order)

session = get_active_session()
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

if my_dataframe:

    editable_df = st.experimental_data_editor(my_dataframe)
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
#    session.sql(my_insert_stmt).collect()

        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
        try:
            og_dataset.merge(edited_dataset
                             , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                             , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                            )
            st.success('Update orders!')

        except Exception as e:
            st.error(f"An unexpected error occurred: {str(e)}")
            st.success('Something went wrong')
else:
     st.success('There are no pending orders right now')
