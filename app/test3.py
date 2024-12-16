from dotenv import load_dotenv
import os
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
import logging

# Configuración de logging
# logging.basicConfig(filename='/var/log/script.log', level=logging.INFO, format='%(asctime)s - %(message)s')

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

INTENCIONALIDAD = {
    "1": 'Accidental',
    "2": 'Violencia familiar',
    "3": 'Violencia no familiar',
    "4": 'Autoinfligido',
    "11": 'Trata de personas',
}

def transformar_datos(valores_raw, diccionario):
    """
    Convierte una lista de valores en sus descripciones correspondientes.
    """
    if valores_raw is None:  # Si el valor es NULL
        return []

    try:
        if isinstance(valores_raw, int):  # Si el valor es un entero, conviértelo a lista
            valores = [str(valores_raw)]
            print(valores)
        elif isinstance(valores_raw, str):  # Si es una cadena, evalúa el contenido
            valores = eval(valores_raw) if valores_raw.strip() else []
        elif isinstance(valores_raw, list):  # Si ya es una lista, úsala directamente
            valores = valores_raw
        else:  # Cualquier otro tipo no es esperado
            logging.warning(f"Tipo inesperado para transformar: {type(valores_raw)} - {valores_raw}")
            return []
        # Manejar listas de valores como ["1", "2", "5"]
        descripciones = [diccionario.get(v, f"Desconocido ({v})") for v in valores]
        return descripciones
    except Exception as e:
        logging.error(f"Error al transformar datos: {e}")
        return []

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
        mysql_cursor.execute("SELECT fechaOcurrencia, diaFestivo, agenteLesion, areaAnatomica, "
                             "COALESCE(intencionalidadEvento, '[]') AS intencionalidadEvento, "
                             "COALESCE(tipoAtencion, '[]') as tipoAtencion, "
                             "COALESCE(tipoViolencia, '[]') as tipoViolencia, "
                             "COALESCE(destinoAtencion, '[]') as destinoAtencion "
                             "FROM sima.violencia_lesiones_copia")  # Cambia 'tabla_origen' al nombre real
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
            # Convertir los datos con los diccionarios
            diafestivo = fila.get("diaFestivo")
            intencionalidad_raw = fila.get("intencionalidadEvento", "[]")
            tipo_atencion_raw = fila.get("tipoAtencion", "[]")
            tipo_violencia_raw = fila.get("tipoViolencia", "[]")
            destino_raw = fila.get("destinoAtencion", "[]")

            intencionalidad = transformar_datos(eval(intencionalidad_raw), INTENCIONALIDAD)
            tipo_atencion = transformar_datos(eval(tipo_atencion_raw), TIPOS_ATENCION)
            tipo_violencia = transformar_datos(eval(tipo_violencia_raw), TIPOS_VIOLENCIA)
            destino = transformar_datos(eval(destino_raw), DESTINOS_ATENCION)

            # Creamos una lista vacía para almacenar las asignaciones
            tipo_atencion_assigned = ["No Aplica"] * 11  # Hay 11 campos tipo_atencion (1-11)

            if "Médica" in tipo_atencion:
                tipo_atencion_assigned[0] = "Médica"
                tipo_atencion.remove("Médica")  # Eliminamos para no volver a asignarla
            # Asignamos "Psicológica" siempre a tipo_atencion_2_des si está presente
            if "Psicológica" in tipo_atencion:
                tipo_atencion_assigned[1] = "Psicológica"
                tipo_atencion.remove("Psicológica")  # Eliminamos para no volver a asignarla
            if "Quirúrgica" in tipo_atencion:
                tipo_atencion_assigned[2] = "Quirúrgica"  
                tipo_atencion.remove("Quirúrgica")  # Eliminamos para no volver a asignarla
            if "Psiquiátrica" in tipo_atencion:
                tipo_atencion_assigned[3] = "Psiquiátrica"
                tipo_atencion.remove("Psiquiátrica")  # Eliminamos para no volver a asignarla
            if "Consejería" in tipo_atencion:
                tipo_atencion_assigned[4] = "Consejería"
                tipo_atencion.remove("Consejería")  # Eliminamos para no volver a asignarla
            if "Otra" in tipo_atencion:
                tipo_atencion_assigned[5] = "Otra"
                tipo_atencion.remove("Otra")  # Eliminamos para no volver a asignarla
            if "Píldora anticonceptiva de emergencia" in tipo_atencion:
                tipo_atencion_assigned[6] = "Píldora anticonceptiva de emergencia"
                tipo_atencion.remove("Píldora anticonceptiva de emergencia")  # Eliminamos para no volver a asignarla
            if "Profilaxis VIH" in tipo_atencion:
                tipo_atencion_assigned[7] = "Profilaxis VIH"
                tipo_atencion.remove("Profilaxis VIH")  # Eliminamos para no volver a asignarla
            if "Profilaxis otras ITS" in tipo_atencion:
                tipo_atencion_assigned[8] = "Profilaxis otras ITS"
                tipo_atencion.remove("Profilaxis otras ITS")  # Eliminamos para no volver a asignarla
            if "IVE (Interrupción Voluntaria del Embarazo)" in tipo_atencion:
                tipo_atencion_assigned[9] = "IVE (Interrupción Voluntaria del Embarazo)"
                tipo_atencion.remove("IVE (Interrupción Voluntaria del Embarazo)")  # Eliminamos para no volver a asignarla
            if "Vacuna VPH" in tipo_atencion:
                tipo_atencion_assigned[10] = "Vacuna VPH"
                tipo_atencion.remove("Vacuna VPH")  # Eliminamos para no volver a asignarla



            # Asignamos el resto de los valores de tipo_atencion a los campos correspondientes
            for i, atencion in enumerate(tipo_atencion):
                if i < 11:
                    tipo_atencion_assigned[i] = atencion

            tipo_violencia_assigned = ["No Aplica"] * 5  # Hay 5 campos tipo_violencia (1-5)

            if "Violencia física" in tipo_violencia:
                tipo_violencia_assigned[0] = "Violencia física"
                tipo_violencia.remove("Violencia física")
            if "Violencia sexual" in tipo_violencia:
                tipo_violencia_assigned[1] = "Violencia sexual"
                tipo_violencia.remove("Violencia sexual")
            if "Violencia psicológica" in tipo_violencia:
                tipo_violencia_assigned[2] = "Violencia psicológica"
                tipo_violencia.remove("Violencia psicológica")
            if "Violencia económica/patrimonial" in tipo_violencia:
                tipo_violencia_assigned[3] = "Violencia económica/patrimonial"
                tipo_violencia.remove("Violencia económica/patrimonial")
            if "Abandono y/o negligencia" in tipo_violencia:
                tipo_violencia_assigned[4] = "Abandono y/o negligencia"
                tipo_violencia.remove("Abandono y/o negligencia")

            # Asignamos el resto de los valores de tipo_violencia a los campos correspondientes
            for i, violencia in enumerate(tipo_violencia):
                if i < 5:
                    tipo_violencia_assigned[i] = violencia

            # Crear el diccionario de datos con la asignación correcta
            datos = {
                "intencionalidad": intencionalidad[0],
                "tipo_atencion_1_des": tipo_atencion_assigned[0],
                "tipo_atencion_2_des": tipo_atencion_assigned[1],
                "tipo_atencion_3_des": tipo_atencion_assigned[2],
                "tipo_atencion_4_des": tipo_atencion_assigned[3],
                "tipo_atencion_5_des": tipo_atencion_assigned[4],
                "tipo_atencion_6_des": tipo_atencion_assigned[5],
                "tipo_atencion_7_des": tipo_atencion_assigned[6],
                "tipo_atencion_8_des": tipo_atencion_assigned[7],
                "tipo_atencion_9_des": tipo_atencion_assigned[8],
                "tipo_atencion_10_des": tipo_atencion_assigned[9],
                "tipo_atencion_11_des": tipo_atencion_assigned[10],
                "tipo_violencia_1_des": tipo_violencia_assigned[0],
                "tipo_violencia_2_des": tipo_violencia_assigned[1],
                "tipo_violencia_3_des": tipo_violencia_assigned[2],
                "tipo_violencia_4_des": tipo_violencia_assigned[3],
                "tipo_violencia_5_des": tipo_violencia_assigned[4],
                "destino_atencion": destino[0]  # En este caso, solo uno, no necesitamos separar en varios campos
            }

            # Convertir los datos en una lista para la inserción
            datos_transformados.append((
                datos["intencionalidad"],
                datos["tipo_atencion_1_des"], datos["tipo_atencion_2_des"], datos["tipo_atencion_3_des"],
                datos["tipo_atencion_4_des"], datos["tipo_atencion_5_des"], datos["tipo_atencion_6_des"],
                datos["tipo_atencion_7_des"], datos["tipo_atencion_8_des"], datos["tipo_atencion_9_des"],
                datos["tipo_atencion_10_des"], datos["tipo_atencion_11_des"],
                datos["tipo_violencia_1_des"], datos["tipo_violencia_2_des"], datos["tipo_violencia_3_des"],
                datos["tipo_violencia_4_des"], datos["tipo_violencia_5_des"],
                datos["destino_atencion"]
            ))

        # Inserción en PostgreSQL
        insert_query = """
        INSERT INTO dataware.test (
            intencionalidad, tipo_atencion_1_des, tipo_atencion_2_des, tipo_atencion_3_des, tipo_atencion_4_des,
            tipo_atencion_5_des, tipo_atencion_6_des, tipo_atencion_7_des, tipo_atencion_8_des, tipo_atencion_9_des,
            tipo_atencion_10_des, tipo_atencion_11_des, tipo_violencia_1_des, tipo_violencia_2_des, tipo_violencia_3_des,
            tipo_violencia_4_des, tipo_violencia_5_des, destino_atencion
        ) VALUES %s
        """
        execute_values(postgres_cursor, insert_query, datos_transformados)
        postgres_conn.commit()

        # Cerrar conexiones
        mysql_cursor.close()
        mysql_conn.close()
        postgres_cursor.close()
        postgres_conn.close()

        logging.info("Proceso completado exitosamente.")

    except Exception as e:
        logging.error(f"Error en el proceso: {e}")

if __name__ == "__main__":
    procesar_filas()
