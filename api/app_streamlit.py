import streamlit as st
import requests
import pandas as pd
import plotly.express as px

API_URL = "http://127.0.0.1:8000"

# config de la page streamlit
st.set_page_config(page_title="Dashboard Yellow Cabs", layout="wide")
st.title("Dashboard Yellow Cabs NYC")
st.caption("Donnees : annee 2024 | Source : NYC Yellow Cab Trip Records")

# session pour garder le token jwt entre les interactions
if 'token' not in st.session_state:
    st.session_state['token'] = None

# barre de connexion a gauche
with st.sidebar:
    st.header("Connexion API")
    if st.session_state['token'] is None:
        with st.form("login_form"):
            username = st.text_input("login")
            password = st.text_input("mdp", type="password")
            submit = st.form_submit_button("ok")
            if submit:
                # on envoie les identifiants a l'api pour avoir un token
                res = requests.post(f"{API_URL}/auth/token", data={"username": username, "password": password})
                if res.status_code == 200:
                    st.session_state['token'] = res.json().get("access_token")
                    st.success("ok")
                    st.rerun()
                else:
                    st.error("erreur login")
    else:
        st.success("connecte")
        if st.button("deco"):
            st.session_state['token'] = None
            st.rerun()

# affichage des graphiques si on est connecte
if st.session_state['token']:
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}

    # graphique 1 : courbe du nb de courses par heure
    st.header("1. Affluence des taxis par heure")
    st.caption("A quels moments de la journee les taxis sont-ils les plus sollicites ?")
    r_hour = requests.get(f"{API_URL}/datamarts/hourly_demand?limit=24&skip=0", headers=headers)
    df_hour = pd.DataFrame(r_hour.json()["data"])
    df_hour = df_hour.rename(columns={"pickup_hour": "Heures", "total_trips": "Nombre de courses"})
    fig1 = px.line(df_hour, x="Heures", y="Nombre de courses")
    fig1.update_layout(xaxis_title="Heure de la journee", yaxis_title="Nombre de courses", separators=",.")
    st.plotly_chart(fig1, use_container_width=True)

    col1, col2 = st.columns(2)

    # graphique 2 : repartition par type de paiement
    with col1:
        st.header("2. Comment les clients paient")
        st.caption("Quel est le mode de paiement prefere des clients ?")
        r_pay = requests.get(f"{API_URL}/datamarts/payment_analysis?limit=10&skip=0", headers=headers)
        df_pay = pd.DataFrame(r_pay.json()["data"])
        df_pay = df_pay.rename(columns={"payment_method": "Type de Paiement", "total_trips": "Nombre de courses"})
        fig2 = px.bar(df_pay, x="Type de Paiement", y="Nombre de courses")
        fig2.update_layout(xaxis_title="Moyen de paiement", yaxis_title="Nombre de courses", separators=",.")
        st.plotly_chart(fig2, use_container_width=True)

    # tableau 3 : top 10 zones par revenus
    with col2:
        st.header("3. Top 10 des zones les plus rentables")
        st.caption("Quelles zones de depart generent le plus de chiffre d'affaires ?")
        r_zone = requests.get(f"{API_URL}/datamarts/zone_performance?limit=10&skip=0", headers=headers)
        df_zone = pd.DataFrame(r_zone.json()["data"])
        df_zone = df_zone[["total_revenue", "pickup_zone", "total_trips"]]
        df_zone = df_zone.rename(columns={"total_revenue": "Revenus Totaux (en $)", "pickup_zone": "Zone de Depart", "total_trips": "Nb Courses"})
        # formatage des chiffres avec espaces pour faciliter la lecture
        styled_df = df_zone.style.format({
            "Nb Courses": lambda x: f"{x:,.0f}".replace(",", " "),
            "Revenus Totaux (en $)": lambda x: f"{x:,.2f}".replace(",", " ")
        })
        st.dataframe(styled_df, use_container_width=True, hide_index=True)

    # graphique 4 : tarif moyen par heure
    st.header("4. Prix moyen d'une course selon l'heure de départ")
    st.caption("Les courses de nuit ou de pointe sont-elles plus rentables par trajet ?")
    df_fare = pd.DataFrame(r_hour.json()["data"])
    df_fare = df_fare.rename(columns={"pickup_hour": "Heures", "avg_fare": "Tarif moyen ($)"})
    df_fare["Tarif moyen ($)"] = df_fare["Tarif moyen ($)"].round(2)
    fig4 = px.bar(df_fare, x="Heures", y="Tarif moyen ($)")
    # ligne de reference : moyenne generale pour voir les heures au dessus ou en dessous
    moyenne = df_fare["Tarif moyen ($)"].mean()
    fig4.add_hline(y=moyenne, line_dash="dash", line_color="red", annotation_text=f"Moyenne : {moyenne:.2f} $", annotation_position="top right", annotation_x=0.5, annotation_xref="paper")
    fig4.update_layout(xaxis_title="Heure de la journee", yaxis_title="Tarif moyen ($)", separators=",.")
    st.plotly_chart(fig4, use_container_width=True)

else:
    st.write("connectez vous a gauche")
