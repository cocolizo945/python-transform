import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
import os

# Mapeos
TIPO_ATENCION = {
    1: 'Médica',
    2: 'Psicológica',
    3: 'Quirúrgica',
    4: 'Psiquiátrica',
    5: 'Consejería',
    6: 'Otra',
    7: 'Píldora anticonceptiva de emergencia',
    8: 'Profilaxis VIH',
    9: 'Profilaxis otras ITS',
    10: 'IVE (Interrupción Voluntaria del Embarazo)',
    11: 'Vacuna VPH'
}

TIPO_VIOLENCIA = {
    6: 'Violencia física',
    7: 'Violencia sexual',
    8: 'Violencia psicológica',
    9: 'Violencia económica/patrimonial',
    10: 'Abandono y/o negligencia'
}

DESTINO_ATENCION = {
    1: 'Domicilio',
    2: 'Traslado a otra unidad',
    3: 'Servicio especializado atención a violencia',
    4: 'Consulta externa',
    5: 'Defunción',
    6: 'Refugio o albergue',
    7: 'DIF',
    8: 'Hospitalización',
    9: 'Ministerio público',
    10: 'Grupo de ayuda mutua',
    11: 'Otro'
}

# Conexiones a las bases de datos
def connect_mysql():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )

def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DATABASE")
    )

# Transformación de datos
def transform_data(row):
    return {
        "id": row["id"],
        "tipo_atencion": TIPO_ATENCION.get(row["tipo_atencion"], "Desconocido"),
        "tipo_violencia": TIPO_VIOLENCIA.get(row["tipo_violencia"], "Desconocido"),
        "destino_atencion": DESTINO_ATENCION.get(row["destino_atencion"], "Desconocido"),
        "detalle": row["detalle"]
    }

# Proceso principal
def main():
    try:
        mysql_conn = connect_mysql()
        postgres_conn = connect_postgres()

        with mysql_conn.cursor(dictionary=True) as mysql_cursor, postgres_conn.cursor() as postgres_cursor:
            # Obtener datos de MySQL
            mysql_cursor.execute("SELECT * FROM ta")
            rows = mysql_cursor.fetchall()

            # Transformar datos
            transformed_data = [transform_data(row) for row in rows]

            # Insertar datos en PostgreSQL
            insert_query = """
            INSERT INTO tabla_destino (id, tipo_atencion, tipo_violencia, destino_atencion, detalle)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                tipo_atencion = EXCLUDED.tipo_atencion,
                tipo_violencia = EXCLUDED.tipo_violencia,
                destino_atencion = EXCLUDED.destino_atencion,
                detalle = EXCLUDED.detalle
            """
            execute_values(postgres_cursor, insert_query, [
                (row["id"], row["tipo_atencion"], row["tipo_violencia"], row["destino_atencion"], row["detalle"])
                for row in transformed_data
            ])
            postgres_conn.commit()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        mysql_conn.close()
        postgres_conn.close()

if __name__ == "__main__":
    main()

