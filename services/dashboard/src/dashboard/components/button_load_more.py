import streamlit as st

def load_more_button(len_news, state_key, initial_limit):
    if len_news > st.session_state[state_key] + initial_limit:
        col1, col2, col3 = st.columns(3)
        with col2:
            if st.button('Load More', key=f"load_more_{state_key}"):
                st.session_state[state_key] += initial_limit
                st.rerun()