import azure.functions as func
from eurostat import getEurostatData

app = func.FunctionApp(http_auth_level=func.AuthLevel.ADMIN)

@app.function_name(name="eurostat_function")
@app.route(route="eurostat")
def eurostat(req: func.HttpRequest) -> func.HttpResponse:
    try:
        response = getEurostatData(req)  # Llamamos a la función externa
        if not isinstance(response, func.HttpResponse):  # Validamos el retorno
            return func.HttpResponse("Error interno: respuesta no válida.", status_code=500)
        return response
    except Exception as e:
        return func.HttpResponse(f"Error en la función: {str(e)}", status_code=500)
    