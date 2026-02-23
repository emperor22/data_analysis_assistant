import streamlit as st
from utils import submit_login_request, get_otp, register_user, is_valid_email
import time

st.set_page_config(layout="centered")

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if st.session_state.authenticated:
    st.write("You are logged in.")
    st.stop()


login_tab, register_tab = st.tabs(["Login", "Register"])

with login_tab:
    st.markdown("**Login**")
    otp_form = st.form("otp_form", enter_to_submit=True)

    with otp_form:
        col1, col2 = st.columns([10, 2])
        with col1:
            username = st.text_input("Username", max_chars=10)
            otp_msg_container = st.empty()
        with col2:
            if st.form_submit_button("Get OTP") and username:
                res = get_otp(username=username)

                if res != "success":
                    otp_msg_container.error(res)
                    time.sleep(1)
                    st.rerun()

                otp_msg_container.success("OTP has been sent.")
                time.sleep(1)
                st.rerun()

    login_form = st.form("login_form", enter_to_submit=True)

    with login_form:
        otp = st.text_input("Input OTP", max_chars=6)

        if st.form_submit_button("Submit"):
            login_data = submit_login_request(username, otp)
            if login_data:
                st.session_state["access_token"] = login_data["access_token"]
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Invalid username/password")

with register_tab:
    register_form = st.form("register_form")

    with register_form:
        username_reg = st.text_input("Username", max_chars=10)
        first_name = st.text_input("First Name", max_chars=20)
        last_name = st.text_input("Last Name", max_chars=20)
        email = st.text_input("Email", max_chars=30)
        st.warning(
            "Please input a valid email as the OTPs for logins will be sent to your email."
        )

        if st.form_submit_button("Register"):
            if not all([username_reg, first_name, last_name, email]):
                st.error("Please fill in all the fields")
                time.sleep(1)
                st.rerun()

            if not is_valid_email(email):
                st.error("Please fill in a valid email.")
                time.sleep(1)
                st.rerun()

            res = register_user(
                username=username_reg,
                first_name=first_name,
                last_name=last_name,
                email=email,
            )

            if res != "success":
                st.error(res)
                time.sleep(1)
                st.rerun()

            st.success(
                f"Your account with username {username} has been successfully created."
            )
            time.sleep(1)
            st.rerun()
