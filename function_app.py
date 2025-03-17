import azure.functions as func
import datetime
import json
import logging
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import BytesIO
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ADMIN)

# Configuración de Azure Blob Storage
BLOB_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
BLOB_CONTAINER_NAME = "etl-powerbi"

# Crear cliente de blob
blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)

@app.function_name(name="eurostat_function")
@app.route(route="eurostat")
def eurostat(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Procesando la funcion HTTP de Eurostat.')

    # Endpoint proporcionado
    baseUrl = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    
    geo_values = [
        "EU27_2020", "BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR",
        "HR", "IT", "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT",
        "RO", "SI", "SK", "FI", "SE"
    ]

    time_values = [
        "2017", "2018", "2019", "2020", "2021", "2022", "2023"
    ]

    # Configuraciones de peticiones
    requests_params = [
        {
            "dataSetCode": "nama_10_gdp", 
            "name": "GDP_annual",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2014",
                "geo": geo_values,
                "unit": "CP_MEUR",
                "na_item": "B1GQ",
                "lang": "en"
            }
        },
        {
        "dataSetCode": "namq_10_gdp", 
            "name": "GDP_quarter",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2014-Q1",
                "geo": geo_values,
                "unit": "CP_MEUR",
                "s_adj": "SCA",
                "na_item": "B1GQ",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "sdg_08_10", 
            "name": "GDP_per_capita",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2013",
                "geo": geo_values,
                "unit": "CLV10_EUR_HAB",
                "na_item": "B1GQ",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "gov_10dd_edpt1",
            "name": "Debt_to_GDP",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2013",
                "geo": geo_values,
                "unit": "PC_GDP",
                "sector": "S13",
                "na_item": "B9",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "tipsgo20",
            "name": "Debt_National_Currency",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2014-Q1",
                "geo": geo_values,
                "unit": "MIO_NAC",
                "sector": "S13",
                "na_item": "GD",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "prc_hicp_midx",
            "name": "HICP_Monthly_Data",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2019-01",
                "geo": geo_values,
                "unit": "I15",
                "coicop": "CP00",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "prc_fsc_idx",
            "name": "Food_Monitoring",
            "params": {
                "format": "JSON",
                "lastTimePeriod": "48",
                "geo": geo_values,
                "unit": "I15",
                "indx": "HICP",
                "coicop": "CP011",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "prc_hpi_q",
            "name": "House_Price_Index",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2014-Q1",
                "geo": geo_values,
                "unit": "I15_Q",
                "purchase": "TOTAL",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "prc_ppp_ind",
            "name": "PPP",
            "params": {
                "format": "JSON",
                "time": time_values,
                "na_item": "PLI_EU27_2020",
                "ppp_cat": "A01",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "nrg_pc_204",
            "name": "Electricity_Price",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2014-S1",
                "geo": geo_values,
                "unit": "KWH",
                "product": "6000",
                "nrg_cons": "TOT_KWH",
                "tax": "I_TAX",
                "currency": "EUR",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "sts_inpp_m",
            "name": "Precios_Industriales",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2017-01",
                "unit": "I15",
                "indic_bt": "PRC_PRR",
                "nace_r2": "B-E36_X_FOOD",
                "s_adj": "NSA",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "sts_sepp_q",
            "name": "Precios servicios",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2017-Q1",
                "geo": geo_values,
                "unit": "I15",
                "indic_bt": "PRC_PRR",
                "nace_r2": "H-N_STS",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "ext_st_27_2020msbec",
            "name": "Trade_Balance",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2019-01",
                "geo": geo_values,
                "stk_flow": "BAL_RT",
                "indic_et": "TRD_VAL",
                "partner": "WORLD",
                "bclas_bec": "TOTAL",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "teilm140",
            "name": "Labour_Cost",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2021-Q4",
                "geo": geo_values,
                "unit": "I20",
                "s_adj": "SCA",
                "nace_r2": "B-S",
                "lcstruct": "D1_D4_MD5",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "tour_occ_arm",
            "name": "Tourist_Arrivals",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2019-01",
                "geo": geo_values,
                "unit": "NR",
                "c_resid": "TOTAL",
                "nace_r2": "I551-I553",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "sts_rb_m",
            "name": "Busines_demographics",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2019-01",
                "unit": "I15",
                "indic_bt": "REG",
                "indic_bt": "BKRT",
                "nace_r2": "B-S_X_O_S94",
                "s_adj": "SCA",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "teibs020",
            "name": "Busines_confidence",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2023-12",
                "geo": geo_values,
                "indic": "BS-ICI-BAL",
                "s_adj": "SA",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "teiis500",
            "name": "Production_in_construction",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2023-11",
                "geo": geo_values,
                "unit": "I21_SCA",
                "indic_bt": "PRD",
                "nace_r2": "F",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "sts_sepr_m",
            "name": "Production_in_service",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2019-01",
                "geo": geo_values,
                "unit": "I15",
                "indic_bt": "PRD",
                "nace_r2": "G-N_STS",
                "s_adj": "SCA",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "une_rt_m",
            "name": "Youth unemployement",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2015-01",
                "geo": geo_values,
                "unit": "PC_ACT",
                "s_adj": "NSA",
                "age": "Y_LT25",
                "sex": "T",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "earn_nt_net",
            "name": "Wages",
            "params": {
                "format": "JSON",
                "geo": geo_values,
                "time": time_values,
                "currency": "EUR",
                "estruct": "NET",
                "ecase": "P1_NCH_AW100",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "migr_imm8",
            "name": "Immigration",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2008",
                "geo": geo_values,
                "unit": "NR",
                "agedef": "COMPLET",
                "age": "TOTAL",
                "sex": "T",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "ei_bsco_m",
            "name": "Consumer_confidence",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2019-01",
                "geo": geo_values,
                "unit": "BAL",
                "indic": "BS-CSMCI",
                "s_adj": "SA",
                "lang": "en"
            }
        },
        {
            "dataSetCode": "une_rt_m",
            "name": "Unemployment",
            "params": {
                "format": "JSON",
                "sinceTimePeriod": "2015-01",
                "geo": geo_values,
                "unit": "PC_ACT",
                "s_adj": "SA",
                "age": "TOTAL",
                "sex": "T",
                "lang": "en"
            }
        },
    ]

    month_dict = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre"
    }

    # Lista para almacenar todos los DataFrames
    all_dataframes = []

    # Función para realizar la petición, procesar los datos y guardar el Excel
    def fetch_and_save_data(request_name, dataSetCode, params):
        logging.info(f"Procesando: {request_name}")
     
        try:
            response = requests.get(f"{baseUrl}{dataSetCode}", params=params, headers={"Accept": "application/json"})
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error en la solicitud HTTP: {e}")
            return

        try:
            data = response.json()
            if "dimension" not in data or "geo" not in data["dimension"]:
                logging.error(f"Datos incorrectos para {request_name}")
                return

            # Mapear las dimensiones geo y time
            geo_map = {v: k for k, v in data["dimension"]["geo"]["category"]["index"].items()}
            geo_map_label = {k: v for k, v in data["dimension"]["geo"]["category"]["label"].items()}
            time_map = {v: k for k, v in data["dimension"]["time"]["category"]["index"].items()}

            # Total de trimestres
            num_time_periods = len(time_map)
            # Decodificar los datos
            decoded_data = []

            for index, value in data["value"].items():
                index = int(index)
                geo_idx = index // num_time_periods
                time_idx = index % num_time_periods
                country_code = geo_map.get(geo_idx, "Desconocido")  # Código del país (e.g., "BE")
                country_name = geo_map_label.get(country_code, "Desconocido")  # Nombre del país (e.g., "Belgium")
                yearTemporal = time_map.get(time_idx, "Desconocido")
                
                # Inicializar variables para Año y Periodo
                year = ""
                periodo = ""
                month = ""
                semester= ""

                # Verificar si 'yearTemporal' contiene un guion para determinar el formato
                if '-' in yearTemporal:
                    # Dividir en dos partes: año y periodo/mes
                    segment = yearTemporal.split('-', 1)
                    year = segment[0]  # Extrae el año, por ejemplo, "2013"
                    periodo_month_semester = segment[1]  # Extrae "Q1" o "1"

                    if periodo_month_semester.startswith('Q'):
                        # Caso Trimestral, por ejemplo, "Q1"
                        periodo = periodo_month_semester  # Asigna "Q1", "Q2", etc.
                        month = ""  # No aplica mes para trimestres
                        semester= ""
                    elif periodo_month_semester.startswith('S'):
                        # Caso Trimestral, por ejemplo, "Q1"
                        periodo = "Y"   # Asigna "Q1", "Q2", etc.
                        month = ""  # No aplica mes para trimestres
                        semester = periodo_month_semester
                    else:
                        # Intentar convertir la segunda parte a entero para meses
                        month_num = int(periodo_month_semester)
                        if 1 <= month_num <= 12:
                            month = month_dict.get(month_num, "Desconocido")  # Obtener el nombre del mes
                            periodo = "Y"  # Opcional: puedes usar solo el número o prefijarlo
                        else:
                            # Si el número de mes está fuera de rango
                            month = ""
                            periodo = "Y"
                            semester= ""
                else:
                    # Caso Anual, por ejemplo, "2012"
                    year = yearTemporal
                    periodo = "Y"  # Indica un período anual
                    month = ""  # No aplica mes para datos anuales
                    semester= ""

                decoded_data.append({
                    "Año": year,
                    "Periodo": periodo,
                    "Mes":month,
                    "Semestre": semester,
                    "Codigo": country_code,
                    "País": country_name,
                    "Valor": value,
                    "Unidad": params.get("unit", ""),
                    "Indicador": request_name,
                })

            # Crear DataFrame
            df = pd.DataFrame(decoded_data)

            # Guardar el archivo en Azure Blob Storage
            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)

            blob_path = f"EuroStat/Eurostat_{request_name}.xlsx"
            blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_path)
            blob_client.upload_blob(excel_buffer, overwrite=True)

            logging.info(f"Archivo subido a Azure Blob Storage: Eurostat_{request_name}.xlsx")

            return df  # Retornar el DataFrame para su posterior concatenación
        
        except Exception as e:
            logging.error(f"Error procesando {request_name}: {e}")
            return
        
    # Iterar sobre las configuraciones y ejecutar la función
    for request in requests_params:
        df = fetch_and_save_data(request["name"], request["dataSetCode"], request["params"])
        if df is not None:
            all_dataframes.append(df)

    # Concatenar todos los DataFrames si la lista no está vacía
    if all_dataframes:
        try:
            merged_df = pd.concat(all_dataframes, ignore_index=True)
           
            # Guardar archivo combinado en Blob Storage
            merged_excel_buffer = BytesIO()
            merged_df.to_excel(merged_excel_buffer, index=False)
            merged_excel_buffer.seek(0)
            
            merged_blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob="Eurostat.xlsx")
            merged_blob_client.upload_blob(merged_excel_buffer, overwrite=True)
            logging.info(f"Archivo fusionado subido a Azure Blog Storage")
            logging.info("Proceso terminado correctamente.")
        except Exception as e:
            logging.error(f"Error al concatenar y guardar el archivo fusionado: {e}")
    else:
        logging.info("No se encontraron DataFrames para concatenar.")

    return func.HttpResponse("Proceso terminado correctamente.", status_code=200)
   