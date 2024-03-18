import prefect
from prefect import task, Flow
import requests
import pandas as pd

# Definir la URL de la API
API_URL = "https://api.example.com/data"

@task
def download_data(url):
    """
    Tarea para descargar datos de la API.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        raise prefect.TaskFailedException(f"Error al descargar datos: {e}")

@task
def clean_data(raw_data):
    """
    Tarea para limpiar los datos.
    """
    # Ejemplo de limpieza básica, puedes personalizar según tus necesidades
    cleaned_data = pd.DataFrame(raw_data)
    cleaned_data = cleaned_data.dropna()  # Eliminar filas con valores nulos
    cleaned_data = cleaned_data.drop_duplicates()  # Eliminar filas duplicadas
    return cleaned_data

@task
def analyze_data(cleaned_data):
    """
    Tarea para analizar los datos limpios.
    """
    # Ejemplo de análisis básico, puedes personalizar según tus necesidades
    summary_stats = cleaned_data.describe()
    return summary_stats

with Flow("DataPipeline") as flow:
    # Definir el flujo de tareas
    raw_data = download_data(API_URL)
    cleaned_data = clean_data(raw_data)
    analysis_result = analyze_data(cleaned_data)

# Ejecutar el flujo
flow.run()
