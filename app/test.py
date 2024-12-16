from dotenv import load_dotenv
import os
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
import logging

# Configuración de logging
#logging.basicConfig(filename='/var/log/script.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Cargar variables de entorno
load_dotenv()

# Diccionarios para transformar los valores
TIPOS_ATENCION = {
    "1": 'Médica',
    "2": 'Psicológica',
    "3": 'Quirúrgica',
    "4": 'Psiquiátrica',
    "5": 'Consejería',
    "6": 'Otra',
    "7": 'Píldora anticonceptiva de emergencia',
    "8": 'Profilaxis VIH',
    "9": 'Profilaxis otras ITS',
    "10": 'IVE (Interrupción Voluntaria del Embarazo)',
    "11": 'Vacuna VPH',
}

TIPOS_VIOLENCIA = {
    "6": 'Violencia física',
    "7": 'Violencia sexual',
    "8": 'Violencia psicológica',
    "9": 'Violencia económica/patrimonial',
    "10": 'Abandono y/o negligencia',
}

DESTINOS_ATENCION = {
    "1": 'Domicilio',
    "2": 'Traslado a otra unidad',
    "3": 'Servicio especializado atención a violencia',
    "4": 'Consulta externa',
    "5": 'Defunción',
    "6": 'Refugio o albergue',
    "7": 'DIF',
    "8": 'Hospitalización',
    "9": 'Ministerio público',
    "10": 'Grupo de ayuda mutua',
    "11": 'Otro',
}

INTENCIONALIDAD={
    "1": 'Accidental',
    "2": 'Violencia familiar',
    "3": 'Violencia no familiar',
    "4": 'Autoinfligido',
    "11": 'Trata de personas',}

def transformar_datos(valores_raw, diccionario):
    """
    Convierte una lista de valores en sus descripciones correspondientes.
    """
    if valores_raw is None:  # Si el valor es NULL
        return "No especificado"

    try:
        if isinstance(valores_raw, int):  # Si el valor es un entero, conviértelo a lista
            valores = [str(valores_raw)]
        elif isinstance(valores_raw, str):  # Si es una cadena, evalúa el contenido
            valores = eval(valores_raw) if valores_raw.strip() else []
        elif isinstance(valores_raw, list):  # Si ya es una lista, úsala directamente
            valores = valores_raw
        else:  # Cualquier otro tipo no es esperado
            logging.warning(f"Tipo inesperado para transformar: {type(valores_raw)} - {valores_raw}")
            return "Desconocido"
        # Manejar listas de valores como ["1", "2", "5"]
        descripciones = [diccionario.get(v, f"Desconocido ({v})") for v in valores]
        return ", ".join(descripciones)
    except Exception as e:
        print("aqui esto")
        logging.error(f"Error al transformar datos: {e}")
        return "Error al transformar"

def procesar_filas():
    """
    Conecta a MySQL y PostgreSQL, transforma los datos y los inserta en PostgreSQL.
    """
    try:
        # Conexión a MySQL
        mysql_conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE")
        )
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        # Consulta en MySQL
        mysql_cursor.execute("SELECT fechaOcurrencia,diaFestivo,agenteLesion,areaAnatomica,COALESCE(intencionalidadEvento, '[]') AS intencionalidadEvento,COALESCE(tipoAtencion, '[]') as tipoAtencion, COALESCE(tipoViolencia, '[]') as tipoViolencia, COALESCE(destinoAtencion, '[]') as destinoAtencion  FROM sima.violencia_lesiones_copia")  # Cambia 'tabla_origen' al nombre real
        filas = mysql_cursor.fetchall()

        # Conexión a PostgreSQL
        postgres_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            user=os.getenv("POSTGRES_USER"),
	    port=os.getenv("POSTGRES_PORT"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DB")
        )
        postgres_cursor = postgres_conn.cursor()

        # Procesar y transformar las filas
        datos_transformados = []
        for fila in filas:
            intencionalidad_raw = fila.get("intencionalidadEvento", "[]")
            tipo_atencion_raw = fila.get("tipoAtencion", "[]")
            tipo_violencia_raw = fila.get("tipoViolencia", "[]")
            destino_raw = fila.get("destinoAtencion", "[]")

            # Transformar los datos
            intencionalidad = transformar_datos(eval(intencionalidad_raw), INTENCIONALIDAD)
            tipo_atencion = transformar_datos(eval(tipo_atencion_raw), TIPOS_ATENCION)
            tipo_violencia = transformar_datos(eval(tipo_violencia_raw), TIPOS_VIOLENCIA)
            destino = transformar_datos(eval(destino_raw), DESTINOS_ATENCION)

            # Crear nueva fila transformada
            datos_transformados.append((
                  # Suponiendo que hay un campo 'id'
                intencionalidad,
                tipo_atencion,
                tipo_violencia,
                destino
                #fila["detalle"]  # Sustituye por otros campos relevantes
            ))

        # Inserción en PostgreSQL
        insert_query = """
        INSERT INTO dataware.test (intencionalidad, tipo_atencion, tipo_violencia, destino_atencion)
        VALUES %s
        """
        execute_values(postgres_cursor, insert_query, datos_transformados)
        postgres_conn.commit()

        # Cerrar conexiones
        mysql_cursor.close()
        mysql_conn.close()
        postgres_cursor.close()
        postgres_conn.close()

        #logging.info("Proceso completado exitosamente.")

    except Exception as e:
        logging.error(f"Error en el proceso: {e}")

if __name__ == "__main__":
    procesar_filas()
