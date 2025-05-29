import streamlit as st  

st.set_page_config(
    page_title="SIAMM - Conulta Productividad",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

import pandas as pd
from datetime import datetime
from sqlalchemy import inspect, text
import os
import tempfile
from src.utils.fondo import set_background

# ==============================================================================
# ESTILOS PERSONALIZADOS 
# ==============================================================================
st.markdown("""
    <style>
    /* Sidebar */
    [data-testid=stSidebar] {
        background-color: #A6DEB7 !important;
    }
    .main-container {
        padding: 2rem;
            
    /* Radio buttons */
    div[role=radiogroup] {
        gap: 0.5rem;
    }
    
    /* Títulos */
    .sidebar .sidebar-title {
        color: #2c3e50;
        font-size: 1.2rem;
        margin-bottom: 1rem;
        font-weight: 600;
    }
    
    /* Instrucciones */
    .sidebar-instructions {
        color: #4a5568;
        font-size: 0.9rem;
        line-height: 1.5;
    }
    
    /* Hover effects */
    div[role=radiogroup] label:hover {
        background-color: #e2e8f0 !important;
    }
    </style>
""", unsafe_allow_html=True)
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")


# ==============================================================================
