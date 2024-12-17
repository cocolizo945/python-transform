from dotenv import load_dotenv
import os
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
import logging

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
        mysql_cursor.execute("""SELECT v.id,  TIMESTAMP(v.fechaOcurrencia, v.horaOcurrencia) AS fechaOcurrencia, v.diaFestivo, v.agenteLesion, v.areaAnatomica, 
                             v.sitioOcurrencia, v.entidadFed, v.municipio, v.localidad, v.ministerioPublico, v.intencionalidadEvento, 
                             v.consecuenciaResultante, v.id_patient,v.updated_at, COALESCE(v.intencionalidadEvento, '[]') AS intencionalidadEvento,
                             COALESCE(v.tipoAtencion, '[]') AS tipoAtencion, COALESCE(v.tipoViolencia, '[]') AS tipoViolencia,
                             COALESCE(v.destinoAtencion, '[]') AS destinoAtencion, p.sexo, p.edad, p.seConsideraIndigena
                             FROM sima.violencia_lesiones_copia v JOIN sima.patients p ON v.id_patient = p.id;""")
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

        # Obtener todos los id_control existentes
        postgres_cursor.execute("SELECT id_control FROM dataware.nuevo_test")
        existing_ids = set(row[0] for row in postgres_cursor.fetchall())  # Todos los id_control existentes

        # Si la tabla está vacía, podemos proceder a insertar sin filtrar por id_control
        if not existing_ids:
            logging.info("La tabla de destino está vacía. Procediendo con la inserción completa.")
        
        # Procesar y transformar las filas
        datos_transformados = []
        for fila in filas:
            id = fila.get("id")
            if id in existing_ids:  # Si el id ya existe, no lo insertamos
                continue

            # Aquí puedes transformar los datos como ya lo haces en tu código

            # Convertir los datos en una lista para la inserción
            datos = {
                "id_insert": id,
                "fechaOcurrencia": fila.get('fechaOcurrencia'),
                "updated_at": fila.get('updated_at'),
                "diaFestivo": fila.get("diaFestivo"),
                "agenteLesion": fila.get("agenteLesion"),
                "areaAnatomica": fila.get("areaAnatomica"),
                "sitioOcurrencia": fila.get("sitioOcurrencia"),
                "entidadFed": fila.get("entidadFed"),
                "municipio": fila.get("municipio"),
                "localidad": fila.get("localidad"),
                "ministerioPublico": fila.get("ministerioPublico"),
                "consecuenciaResultante": fila.get("consecuenciaResultante"),
                "sexo": fila.get("sexo"),
                "edad": fila.get("edad"),
                "seConsideraIndigena": fila.get("seConsideraIndigena"),
                "intencionalidad": fila.get("intencionalidadEvento"),
                "tipo_atencion_1_des": fila.get("tipoAtencion_1_des"),
                "tipo_atencion_2_des": fila.get("tipoAtencion_2_des"),
                "tipo_atencion_3_des": fila.get("tipoAtencion_3_des"),
                # ... (agregar los demás campos)
            }

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
                # ... (agregar los demás campos)
            ))

        # Insertar solo los registros nuevos
        insert_query = """
        INSERT INTO dataware.nuevo_test (
            id_control, fecha_evento, fecha_atencion, dia_festivo_des, agente_lesion_des, area_anatomica_des,
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
