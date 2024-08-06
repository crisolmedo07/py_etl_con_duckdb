import streamlit as st
from pipeline import pipeline

st.title('Procesador de Archivo')

if st.button('Procesar'):
    with st.spinner('Procesando.....'):
        logs = pipeline()
        # Escribe el log en el streamlit
        for log in logs:
            st.write(log)