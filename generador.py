import pandas as pd
import sqlalchemy
from config import CON_URL
import templates


def generar_seccion(seccion: str, *args) -> str:
    """Genera una sección de un script SQL a partir de un template y argumentos."""
    secciones = {
        "encabezado": templates.encabezado,
        "final": templates.final,
        "insert": templates.insert,
        "encabezado_kodak": templates.encabezado_kodak,
        "final_kodak": templates.final,
        "insert_kodak": templates.insert_kodak,
    }

    template = secciones.get(seccion)
    campos = template.get_identifiers()
    valores = args
    mapeo = {campo: valores[i] for i, campo in enumerate(campos)}

    return template.substitute(**mapeo)


def escribir_archivo(nom_archivo: str, contenido: str, modo: str) -> None:
    """Escribe un archivo con el contenido indicado."""
    with open(f"./data/{nom_archivo}", modo) as file:
        file.write(contenido)


def obtener_ultimo_id(connetion, query) -> int:
    """Obtiene el último ID de la base de datos."""
    try:
        data = pd.read_sql(query, connetion)
        id = data.loc[0]["id"]
        return id
    except:
        raise Exception("Error al obtener el último ID en la consulta: ", query)


def obtener_cantidad_inserts(df: pd.DataFrame) -> dict:
    """Obtiene la cantidad de inserts a realizar."""
    cant_inserts_delegaciones = 0
    cant_inserts_kodak = 0

    for _, row in df.iterrows():
        if row["TIPO_REGISTRO"] in (3, 5, 6):
            cant_inserts_kodak += 1
        cant_inserts_delegaciones += 1

    return {"delegaciones": cant_inserts_delegaciones, "kodak": cant_inserts_kodak}


def main():
    df = pd.read_excel("./data/delegaciones.xlsx")
    archivo_delegaciones = "PROD - 1 - INSERT DELEGACIONES.sql"
    archivo_kodak = "PROD - 2 - INSERT DELEGACIONES KODAK.sql"
    escribir_kodak = False

    engine = sqlalchemy.create_engine(CON_URL)

    cantidad_inserts = obtener_cantidad_inserts(df)

    if (cantidad_inserts["delegaciones"] == 0):
        return

    with engine.connect() as conn:
        
        query = sqlalchemy.text(
            """SELECT ID_CIRCUNSCRIPCION AS ID FROM 
            RCE_GED.SYS_CIRCUNSCRIPCION ORDER BY ID_CIRCUNSCRIPCION DESC
            fetch first 1 rows only"""
        )
        id_delegacion = obtener_ultimo_id(conn, query)
        contenido = generar_seccion("encabezado", cantidad_inserts["delegaciones"])
        escribir_archivo(archivo_delegaciones, contenido, "w+")
        
        if (cantidad_inserts["kodak"] > 0):
            escribir_kodak = True
            query = sqlalchemy.text(
                """SELECT ID_KODAK_CIRCUNSCRIPCION AS ID FROM RCE_GED.SYS_KODAK_CIRCUNSCRIPCION 
                ORDER BY ID_KODAK_CIRCUNSCRIPCION DESC
                fetch first 1 rows only"""
            )
            id_kodak = obtener_ultimo_id(conn, query)
            contenido = generar_seccion("encabezado_kodak", cantidad_inserts["kodak"], id_kodak)
            escribir_archivo(archivo_kodak, contenido, "w+")

        for _, row in df.iterrows():
            id_delegacion += 1
            delegacion = str(row["DELEGACION"]).strip()
            tipo_registro = str(row["TIPO_REGISTRO"]).strip()
            codigo_renaper = str(row["CODIGO_RENAPER"]).strip()
            localidad = str(row["LOCALIDAD"]).strip().upper()
            departamento = str(row["DEPARTAMENTO"]).strip().upper()
            
            query = sqlalchemy.text(
                f"""SELECT LOC.ID
                FROM EU_GED.EU_LOCALIDAD  LOC
                INNER JOIN EU_GED.EU_DEPARTAMENTO DEP ON LOC.ID_DEPARTAMENTO = DEP.ID
                WHERE LOC.NOMBRE = '{localidad}' AND DEP.NOMBRE = '{departamento}' AND LOC.ID_PROVINCIA = 97
                """
            )
            id_localidad = obtener_ultimo_id(conn, query)
            
            contenido = generar_seccion(
                "insert",
                id_delegacion,
                id_delegacion,
                delegacion,
                tipo_registro,
                codigo_renaper,
                id_localidad,
            )
            escribir_archivo(archivo_delegaciones, contenido, "a")

            if int(tipo_registro) in (3, 5, 6):
                contenido = generar_seccion("insert_kodak", tipo_registro, codigo_renaper, id_delegacion)
                escribir_archivo(archivo_kodak, contenido, "a")


        contenido = generar_seccion("final")
        escribir_archivo(archivo_delegaciones, contenido, "a")
        if escribir_kodak:
            contenido = generar_seccion("final_kodak")
            escribir_archivo(archivo_kodak, contenido, "a")


if __name__ == "__main__":
    main()
