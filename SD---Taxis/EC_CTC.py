from fastapi import FastAPI
import requests

app = FastAPI() 

# Configuración de la API de OpenWeather
API_KEY = "8d86a7a1c78b3eb7e89b9a0b201ea97d"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

@app.get("/traffic-status") 
def traffic_status(city: str):
    """
    Endpoint que devuelve el estado del tráfico basado en la temperatura de la ciudad.
    """
    try:
        # Solicitar la temperatura actual a OpenWeather
        response = requests.get(BASE_URL, params={"q": city, "appid": API_KEY})
        data = response.json() 

        if response.status_code == 200:
            # Convertir temperatura de Kelvin a Celsius
            temperature_kelvin = data["main"]["temp"]
            temperature_celsius = temperature_kelvin - 273.15

            # Determinar estado del tráfico
            if temperature_celsius < 0:
                return {"status": "KO", "reason": "Temperature below 0°C"}
            else:
                return {"status": "OK", "reason": "Temperature is acceptable"}
        else:
            return {
                "status": "KO",
                "reason": f"Error fetching data: {data.get('message', 'Unknown error')}",
            }
    except Exception as e:
        return {"status": "KO", "reason": f"Unexpected error: {str(e)}"}
