# migrateVolumesACS
# Migrate Volumes to  Primary Storage to Other in Apache CloudStack (API)
import requests
import logging
import hmac
import hashlib
import base64
import time
from urllib.parse import urlencode, quote_plus
import os
from datetime import datetime
import getpass  # Importamos el módulo getpass para ocultar la Secret Key

# Variable global para verificar si el logging ya fue configurado
logging_configured = False

# Configuración del logging
def setup_logging(log_file, debug_mode=False):
    global logging_configured  # Usamos la variable global para evitar duplicados
    try:
        if logging_configured:
            return logging.getLogger("main_logger"), logging.getLogger("console_logger")
        
        log_file = os.path.normpath(log_file)
        log_dir = os.path.dirname(log_file)
        
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
            except Exception as e:
                print(f"Error al crear el directorio '{log_dir}': {str(e)}. Usando la carpeta actual.")
                log_file = "volMigrate.log"
        
        if not os.access(os.path.dirname(log_file) or ".", os.W_OK):
            print(f"No se tienen permisos de escritura en el directorio '{os.path.dirname(log_file)}'. Usando la carpeta actual.")
            log_file = "volMigrate.log"
        
        if not log_file.strip():
            log_file = "volMigrate.log"
            print(f"No se especificó una ruta válida para el archivo de log. Se creará automáticamente: {os.path.abspath(log_file)}")
        
        logger = logging.getLogger("main_logger")
        logger.setLevel(logging.DEBUG)
        
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        console_logger = logging.getLogger("console_logger")
        console_logger.setLevel(logging.DEBUG)
        
        for handler in console_logger.handlers[:]:
            console_logger.removeHandler(handler)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        
        if not debug_mode:
            console_handler.addFilter(lambda record: record.levelno >= logging.INFO)
        
        console_logger.addHandler(console_handler)
        
        logger.info(f"Logging configurado correctamente. Archivo de log: {log_file}")
        console_logger.info(f"Logging configurado correctamente. Archivo de log: {log_file}")
        
        logging_configured = True
        
        return logger, console_logger
    except Exception as e:
        print(f"Error al configurar el logging: {str(e)}")
        exit(1)

# Función para firmar la solicitud
def sign_request(params, secret_key, logger):
    sorted_params = sorted(params.items(), key=lambda x: x[0].lower())
    query_string = "&".join([f"{k}={quote_plus(str(v))}" for k, v in sorted_params])
    logger.debug(f"Query string antes de firmar: {query_string}")
    signature = hmac.new(
        secret_key.encode('utf-8'),
        query_string.lower().encode('utf-8'),
        hashlib.sha1
    ).digest()
    signature = base64.b64encode(signature).decode('utf-8').strip()
    logger.debug(f"Firma generada: {signature}")
    params["signature"] = signature
    return params

# Función para llamar a la API de CloudStack
def call_cloudstack_api(cloudstack_url, api_key, secret_key, command, params, logger):
    params.update({
        "apiKey": api_key,
        "response": "json",
        "command": command
    })
    params = sign_request(params, secret_key, logger)
    
    headers = {
        "User-Agent": "CloudStack-Migration-Tool",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        session = requests.Session()
        response = session.post(cloudstack_url, data=params, headers=headers, allow_redirects=True)
        response.raise_for_status()
        logger.debug(f"Respuesta completa de la API: {response.text}")
        return response.json()
    except requests.exceptions.ProxyError as e:
        logger.error(f"Error de proxy: {str(e)}")
        logger.error("Verifica la configuración del proxy o desactívalo si no es necesario.")
        return {}
    except requests.exceptions.RequestException as e:
        logger.error(f"Error en la llamada a la API: {str(e)}")
        logger.error(f"URL de la solicitud: {cloudstack_url}")
        if 'response' in locals() and response is not None:
            logger.error(f"Respuesta del servidor: {response.text}")
        else:
            logger.error("No se recibió respuesta del servidor.")
        return {}

# Función para validar credenciales
def validate_credentials(cloudstack_url, api_key, secret_key, logger):
    response = call_cloudstack_api(cloudstack_url, api_key, secret_key, "listAccounts", {}, logger)
    if response and "listaccountsresponse" in response:
        logger.info("Credenciales válidas.")
        return True
    else:
        logger.error("Credenciales inválidas o URL incorrecta.")
        return False

# Función para validar un Primary Storage ID (solo verifica que exista)
def validate_primary_storage(cloudstack_url, api_key, secret_key, storage_id, logger):
    response = call_cloudstack_api(
        cloudstack_url, api_key, secret_key, "listStoragePools",
        {"id": storage_id}, logger
    )
    if response and "liststoragepoolsresponse" in response and len(response["liststoragepoolsresponse"]["storagepool"]) > 0:
        storage = response["liststoragepoolsresponse"]["storagepool"][0]
        logger.info(f"Primary Storage ID válido: {storage_id}")
        return True
    else:
        logger.error(f"Primary Storage ID inválido: {storage_id}")
        return False

# Función para obtener volúmenes de un almacenamiento primario
def get_volumes_from_storage(cloudstack_url, api_key, secret_key, storage_id, project_id=None, logger=None):
    params = {
        "listall": "true",
        "storageid": storage_id
    }
    if project_id:
        params["projectid"] = project_id
    
    response = call_cloudstack_api(cloudstack_url, api_key, secret_key, "listVolumes", params, logger)
    if response and "listvolumesresponse" in response and "volume" in response["listvolumesresponse"]:
        return response["listvolumesresponse"]["volume"]
    return []

# Función para migrar un volumen
def migrate_volume(cloudstack_url, api_key, secret_key, volume_id, volume_name, dest_storage_id, logger, console_logger):
    try:
        start_time = datetime.now()
        logger.info(f"Iniciando migración del volumen {volume_name} ({volume_id}) a las {start_time.strftime('%H:%M:%S')}")
        console_logger.info(f"Iniciando migración del volumen '{volume_name}' (ID: {volume_id}) a las {start_time.strftime('%H:%M:%S')}...")
        
        response = call_cloudstack_api(
            cloudstack_url, api_key, secret_key, "migrateVolume",
            {
                "volumeid": volume_id,
                "storageid": dest_storage_id,
                "livemigrate": "true"
            }, logger
        )
        if response and "errortext" in response:
            logger.error(f"Error al migrar volumen {volume_name} ({volume_id}): {response['errortext']}")
            console_logger.error(f"Error al migrar volumen '{volume_name}' (ID: {volume_id}).")
            return False
        else:
            job_id = response.get("migratevolumeresponse", {}).get("jobid", "")
            if job_id:
                logger.info(f"Migración iniciada para el volumen {volume_name} ({volume_id}), Job ID: {job_id}")
                while True:
                    status_response = check_migration_status(cloudstack_url, api_key, secret_key, job_id, logger)
                    job_status = status_response.get("jobstatus", 0)
                    if job_status == 1:
                        end_time = datetime.now()
                        logger.info(f"Migración completada para el volumen {volume_name} ({volume_id}) a las {end_time.strftime('%H:%M:%S')}")
                        console_logger.info(f"Migración completada para el volumen '{volume_name}' (ID: {volume_id}) a las {end_time.strftime('%H:%M:%S')}.")
                        return True
                    elif job_status == 2:
                        end_time = datetime.now()
                        logger.error(f"Migración fallida para el volumen {volume_name} ({volume_id}) a las {end_time.strftime('%H:%M:%S')}: {status_response.get('jobresult', {}).get('errortext', 'Error desconocido')}")
                        console_logger.error(f"Migración fallida para el volumen '{volume_name}' (ID: {volume_id}) a las {end_time.strftime('%H:%M:%S')}.")
                        return False
                    else:
                        logger.info(f"Progreso de migración para el volumen {volume_name} ({volume_id}): En proceso...")
                        time.sleep(5)
            else:
                logger.error(f"No se pudo obtener el Job ID para el volumen {volume_name} ({volume_id})")
                console_logger.error(f"No se pudo iniciar la migración para el volumen '{volume_name}' (ID: {volume_id}).")
                return False
    except Exception as e:
        logger.error(f"Excepción al migrar volumen {volume_name} ({volume_id}): {str(e)}")
        console_logger.error(f"Excepción al migrar volumen '{volume_name}' (ID: {volume_id}).")
        return False

# Función para verificar el estado de la migración
def check_migration_status(cloudstack_url, api_key, secret_key, job_id, logger):
    response = call_cloudstack_api(
        cloudstack_url, api_key, secret_key, "queryAsyncJobResult", {"jobid": job_id}, logger
    )
    if response and "queryasyncjobresultresponse" in response:
        return response.get("queryasyncjobresultresponse", {})
    return {}

# Función principal
def main():
    cloudstack_url = ""
    api_key = ""
    secret_key = ""
    source_storage_id = ""
    dest_storage_id = ""
    log_file = ""

    while True:
        while not cloudstack_url.strip():
            cloudstack_url = input("Ingresa la URL del servidor de CloudStack (ejemplo: https://cloud.arsat.com.ar/client/api): ").strip()
        
        while not api_key.strip():
            api_key = input("Ingresa tu API Key: ").strip()
        
        while not secret_key.strip():
            secret_key = getpass.getpass("Ingresa tu Secret Key (se mantendrá oculta): ").strip()
        
        debug_mode = input("¿Activar modo debug? (s/n): ").lower() == "s"
        
        while True:
            log_file = input("Ingresa la ruta completa para el archivo de log (deja en blanco para usar la carpeta actual): ").strip()
            if not log_file.strip():
                log_file = "volMigrate.log"
                print(f"No se especificó una ruta válida para el archivo de log. Se creará automáticamente: {os.path.abspath(log_file)}")
                break
            
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                print(f"El directorio '{log_dir}' no existe. Intenta nuevamente.")
                continue
            
            if not os.access(log_dir, os.W_OK):
                print(f"No tienes permisos de escritura en el directorio '{log_dir}'. Intenta nuevamente.")
                continue
            
            break
        
        logger, console_logger = setup_logging(log_file, debug_mode)
        
        if not validate_credentials(cloudstack_url, api_key, secret_key, logger):
            console_logger.error("Credenciales inválidas o URL incorrecta. Por favor, inténtalo nuevamente.")
            cloudstack_url = ""
            api_key = ""
            secret_key = ""
            continue
        
        while not source_storage_id.strip() or not validate_primary_storage(cloudstack_url, api_key, secret_key, source_storage_id, logger):
            source_storage_id = input("Ingresa el ID del almacenamiento primario de origen: ").strip()
            if not validate_primary_storage(cloudstack_url, api_key, secret_key, source_storage_id, logger):
                console_logger.error("El ID del almacenamiento primario de origen no es válido o no está en estado 'Up'. Inténtalo nuevamente.")
        
        while not dest_storage_id.strip() or not validate_primary_storage(cloudstack_url, api_key, secret_key, dest_storage_id, logger):
            dest_storage_id = input("Ingresa el ID del almacenamiento primario de destino: ").strip()
            if not validate_primary_storage(cloudstack_url, api_key, secret_key, dest_storage_id, logger):
                console_logger.error("El ID del almacenamiento primario de destino no es válido o no está en estado 'Up'. Inténtalo nuevamente.")
        
        while True:
            volumes = []
            projects = get_projects(cloudstack_url, api_key, secret_key, logger)
            
            # Obtener volúmenes regulares
            regular_volumes = get_volumes_from_storage(cloudstack_url, api_key, secret_key, source_storage_id, logger=logger)
            volumes.extend(regular_volumes)
            
            # Obtener volúmenes por proyecto
            for project in projects:
                project_id = project["id"]
                project_name = project.get("name", "Sin nombre")
                project_volumes = get_volumes_from_storage(cloudstack_url, api_key, secret_key, source_storage_id, project_id=project_id, logger=logger)
                volumes.extend(project_volumes)
            
            ready_volumes = [v for v in volumes if v.get("state", "Unknown") == "Ready"]
            migrating_volumes = [v for v in volumes if v.get("state", "Unknown") == "Migrating"]
            
            console_logger.info("\nResumen de volúmenes en el almacenamiento de origen:")
            console_logger.info(f"- Volúmenes en estado 'Ready': {len(ready_volumes)}")
            console_logger.info(f"- Volúmenes en estado 'Migrating': {len(migrating_volumes)}")
            logger.info(f"Volúmenes listos para migrar: {len(ready_volumes)}")
            logger.info(f"Volúmenes en estado 'Migrating': {len(migrating_volumes)}")
            
            if len(ready_volumes) == 0:
                console_logger.info("\nNo quedan volúmenes en estado 'Ready' para migrar.")
                if len(migrating_volumes) > 0:
                    console_logger.info(f"Aún hay {len(migrating_volumes)} volúmenes en estado 'Migrating'. Esperando a que finalicen...")
                    logger.info(f"Aún hay {len(migrating_volumes)} volúmenes en estado 'Migrating'. Esperando a que finalicen...")
                    while True:
                        still_migrating = False
                        for volume in migrating_volumes:
                            volume_id = volume["id"]
                            volume_name = volume.get("name", "Sin nombre")
                            current_state = get_volume_state(cloudstack_url, api_key, secret_key, volume_id, logger)
                            if current_state == "Migrating":
                                still_migrating = True
                                logger.info(f"Volumen '{volume_name}' (ID: {volume_id}) aún en estado 'Migrating'. Revisando nuevamente en 10 segundos...")
                                time.sleep(10)
                            else:
                                logger.info(f"Volumen '{volume_name}' (ID: {volume_id}) ha cambiado de estado a '{current_state}'.")
                        if not still_migrating:
                            break
                console_logger.info("\nProceso de migración completado.\n")
                logger.info("Proceso de migración completado.")
                break
            
            total_volumes = len(ready_volumes)
            console_logger.info(f"\nTotal de volúmenes en estado 'Ready' disponibles para migrar: {total_volumes}\n")
            migrated_volumes = 0
            for volume in ready_volumes:
                volume_id = volume["id"]
                volume_name = volume.get("name", "Sin nombre")
                if migrate_volume(cloudstack_url, api_key, secret_key, volume_id, volume_name, dest_storage_id, logger, console_logger):
                    migrated_volumes += 1
                
                progress_percentage = (migrated_volumes / total_volumes) * 100
                remaining_percentage = 100 - progress_percentage
                logger.info(f"Progreso global: {migrated_volumes}/{total_volumes} volúmenes migrados ({progress_percentage:.2f}% completado, {remaining_percentage:.2f}% restante)")
            
            volumes = get_volumes_from_storage(cloudstack_url, api_key, secret_key, source_storage_id, logger=logger)
            ready_volumes = [v for v in volumes if v.get("state", "Unknown") == "Ready"]
            if len(ready_volumes) == 0:
                console_logger.info("\nNo quedan volúmenes en estado 'Ready' para migrar.")
                break
        
        choice = input("¿Deseas migrar volúmenes de otro primary storage? (s/n): ").lower()
        if choice != "s":
            break
        
        source_storage_id = ""
        dest_storage_id = ""

# Función para obtener proyectos
def get_projects(cloudstack_url, api_key, secret_key, logger):
    response = call_cloudstack_api(cloudstack_url, api_key, secret_key, "listProjects", {"listall": "true"}, logger)
    if response and "listprojectsresponse" in response and "project" in response["listprojectsresponse"]:
        return response["listprojectsresponse"]["project"]
    return []

# Función para obtener el estado actual de un volumen
def get_volume_state(cloudstack_url, api_key, secret_key, volume_id, logger):
    response = call_cloudstack_api(
        cloudstack_url, api_key, secret_key, "listVolumes", {"id": volume_id}, logger
    )
    if response and "listvolumesresponse" in response and len(response["listvolumesresponse"]["volume"]) > 0:
        return response["listvolumesresponse"]["volume"][0].get("state", "Unknown")
    return "Unknown"

if __name__ == "__main__":
    main()
