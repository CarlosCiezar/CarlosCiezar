import azure.functions as func
from eurostat import getEurostatData
import datetime
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ADMIN)

def eurostat_data() -> func.HttpResponse:
    try:
        response = getEurostatData(None)  # Llamamos a la función externa
        if not isinstance(response, func.HttpResponse):  # Validamos el retorno
            return func.HttpResponse("Error interno: respuesta no válida.", status_code=500)
        return response
    except Exception as e:
        return func.HttpResponse(f"Error en la función: {str(e)}", status_code=500)
    

# --- TIMER Trigger (se ejecuta el día 1 de cada mes) ---
@app.function_name(name="eurostat_monthly_trigger")
@app.schedule(schedule="0 0 0 1 * *", arg_name="timer", run_on_startup=False, use_monitor=True)
def eurostat_monthly_trigger(timer: func.TimerRequest) -> None:
    logging.info("Ejecutando función mensual desde el Timer Trigger...")
    eurostat_data()

    