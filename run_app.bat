@echo off
echo Iniciando SIMM App Streamlit...
docker run --rm -p 8501:8501 simm_app_streamlit-app:latest
echo.
echo La aplicación está corriendo en: http://localhost:8501
echo Presiona Ctrl+C para detener
pause