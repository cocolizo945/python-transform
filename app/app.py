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

SEXO = {
    "1": 'HOMBRE',
    "2": 'MUJER',
}

INDIGENA = {
    "1": 'SI',
    "2": 'NO',
}

MINISTERIO = {
    "1": 'SI',
    "2": 'NO',
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
        mysql_cursor.execute("""SELECT v.id, TIMESTAMP(v.fechaOcurrencia, v.horaOcurrencia) AS fechaOcurrencia, v.diaFestivo, v.agenteLesion, v.areaAnatomica, 
                             v.sitioOcurrencia, v.entidadFed, v.municipio, v.localidad, v.ministerioPublico, v.intencionalidadEvento, 
                             v.consecuenciaResultante, v.id_patient,v.updated_at, COALESCE(v.intencionalidadEvento, '[]') AS intencionalidadEvento,
                             COALESCE(v.tipoAtencion, '[]') AS tipoAtencion, COALESCE(v.tipoViolencia, '[]') AS tipoViolencia,
                             COALESCE(v.destinoAtencion, '[]') AS destinoAtencion, p.sexo, p.edad, p.seConsideraIndigena
                             FROM sima.violencia_lesiones v JOIN sima.patients p ON v.id_patient = p.id;""")  # Cambia 'tabla_origen' al nombre real
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
            # Convertir los datos con los 
            id = fila.get("id")
            fechaOcurrencia = fila.get('fechaOcurrencia')
            diaFestivo = fila.get("diaFestivo")

            agenteLesion = fila.get("agenteLesion")
            areaAnatomica = fila.get("areaAnatomica")
            sitioOcurrencia = fila.get("sitioOcurrencia")
            entidadFed = fila.get("entidadFed")
            municipio = fila.get("municipio")
            localidad = fila.get("localidad")
            ministerioPublico = fila.get("ministerioPublico")
            consecuenciaResultante = fila.get("consecuenciaResultante")
            sexo = fila.get("sexo")
            edad = fila.get("edad")
            seConsideraIndigena = fila.get("seConsideraIndigena")
            updated_at = fila.get("updated_at")

            intencionalidad_raw = fila.get("intencionalidadEvento", "[]")
            tipo_atencion_raw = fila.get("tipoAtencion", "[]")
            tipo_violencia_raw = fila.get("tipoViolencia", "[]")
            destino_raw = fila.get("destinoAtencion", "[]")

            intencionalidad = transformar_datos(eval(intencionalidad_raw), INTENCIONALIDAD)
            tipo_atencion = transformar_datos(eval(tipo_atencion_raw), TIPOS_ATENCION)
            tipo_violencia = transformar_datos(eval(tipo_violencia_raw), TIPOS_VIOLENCIA)
            sexo_t = transformar_datos(eval(sexo), SEXO)
            print(sexo_t)
            print(sexo)
            ministerio = transformar_datos(eval(ministerioPublico), MINISTERIO)
            print(ministerio)
            print(ministerioPublico)
            seindigena = transformar_datos(eval(seConsideraIndigena), INDIGENA)
            print(seindigena)
            print(seConsideraIndigena)
            destino = transformar_datos(eval(destino_raw), DESTINOS_ATENCION)

            postgres_cursor.execute("SELECT id_control FROM dataware.newdata WHERE id_control = %s", (str(id)))
            existing_record = postgres_cursor.fetchone()
            if not existing_record:
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
                    "id_insert":id,
                    "fechaOcurrencia":fechaOcurrencia,
                    "updated_at":updated_at,
                    "diaFestivo":diaFestivo,
                    "agenteLesion":agenteLesion,
                    "areaAnatomica":areaAnatomica,
                    "sitioOcurrencia":sitioOcurrencia,
                    "entidadFed":entidadFed,
                    "municipio":municipio,
                    "localidad":localidad,
                    "ministerioPublico":ministerio[0],
                    "consecuenciaResultante":consecuenciaResultante,
                    "sexo":sexo_t[0],
                    "edad":edad,
                    "seConsideraIndigena":seindigena[0],
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
                    datos["id_insert"],
                    datos["fechaOcurrencia"],
                    datos["updated_at"],
                    datos["diaFestivo"],
                    datos["agenteLesion"],
                    datos["areaAnatomica"],
                    datos["intencionalidad"],
                    datos["sitioOcurrencia"],
                    datos["entidadFed"],
                    datos["localidad"],
                    datos["municipio"],
                    datos["ministerioPublico"],
                    datos["consecuenciaResultante"],
                    datos["sexo"],
                    datos["edad"],
                    datos["seConsideraIndigena"],
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
        INSERT INTO dataware.newdata (id_control,fecha_evento,fecha_atencion,dia_festivo_des,agente_lesion_des, area_anatomica_des,
            intencionalidad_des, sitio_ocurrencia_des, entidad_ocurrencia_des, localidad_ocurrencia_des, municipio_ocurrencia_des,
            ministerio_publico_des, consecuencia_gravedad_des, sexo_des, edad, se_considera_indigena_des,
            tipo_atencion_1_des, tipo_atencion_2_des, tipo_atencion_3_des, tipo_atencion_4_des,
            tipo_atencion_5_des, tipo_atencion_6_des, tipo_atencion_7_des, tipo_atencion_8_des, tipo_atencion_9_des,
            tipo_atencion_10_des, tipo_atencion_11_des, tipo_violencia_1_des, tipo_violencia_2_des, tipo_violencia_3_des,
            tipo_violencia_4_des, tipo_violencia_5_des, despues_atencion_des
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
