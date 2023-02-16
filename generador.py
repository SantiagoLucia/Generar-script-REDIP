import pandas as pd
from tabulate import tabulate
import sqlalchemy
from config import CON_URL
import strings


def escribir_archivo(nom_archivo, seccion, modo, *args):
    
    secciones = {
        'encabezado': strings.encabezado,
        'final': strings.final,
        'insert': strings.insert,
        'encabezado_kodak': strings.encabezado_kodak,
        'final_kodak': strings.final,
        'insert_kodak': strings.insert_kodak
    }
    
    template = secciones.get(seccion)
    identificadores = template.get_identifiers()
    mapeo = { identificador: args[i] for i, identificador in enumerate(identificadores) }
    
    with open(nom_archivo, modo) as file:
        file.write(template.substitute(**mapeo))


def crear_scripts_sql(df):
    flag_encabezado_kodak = 1
    cant1 = 0
    cant2 = 0
    nombre_archivo_d = "PROD - 1 - INSERT DELEGACIONES.sql"

    engine = sqlalchemy.create_engine(CON_URL)
    with engine.connect() as conn:
        
        print("Obteniendo último ID de delegación...")
        query = """SELECT ID_CIRCUNSCRIPCION FROM 
            RCE_GED.SYS_CIRCUNSCRIPCION ORDER BY ID_CIRCUNSCRIPCION DESC
            fetch first 1 rows only"""        
        data = pd.read_sql(query, conn)
        id_cir = data.iloc[0][0]

        print("Obteniendo último ID KODAK...")
        query = """SELECT ID_KODAK_CIRCUNSCRIPCION FROM RCE_GED.SYS_KODAK_CIRCUNSCRIPCION 
            ORDER BY ID_KODAK_CIRCUNSCRIPCION DESC
            fetch first 1 rows only"""
        data = pd.read_sql(query, conn)
        
        k_ini = data.iloc[0][0]
        k_ini += 1
        k_ini = str(k_ini)
        print("OK")

        for _, row in df.iterrows():
            if row['TIPO REGISTRO'] in (3, 5, 6):
                cant2 += 1
            cant1 += 1

        escribir_archivo(nombre_archivo_d, 'encabezado', 'w+', cant1)

        for _, row in df.iterrows():
            id_cir += 1
            id_cir_str = str(id_cir)

            nom = row['NOMBRE DELEGACION']
            nom = str(nom).strip()

            reg = row['TIPO REGISTRO']
            reg = str(reg).strip()

            renaper = row['COD RENAPER']
            renaper = str(renaper).strip()

            nom_loc = row['LOCALIDAD']
            nom_loc = str(nom_loc).strip()
            nom_loc = nom_loc.upper()

            nom_dep = row['PARTIDO']
            nom_dep = str(nom_dep).strip()
            nom_dep = nom_dep.upper()

            try:
                print(f"Obteniendo ID de localidad {nom_loc}...")
                query = f"""SELECT LOC.ID
                    FROM EU_GED.EU_LOCALIDAD  LOC
                    INNER JOIN EU_GED.EU_DEPARTAMENTO DEP ON LOC.ID_DEPARTAMENTO = DEP.ID
                    WHERE LOC.NOMBRE = '{nom_loc}' AND DEP.NOMBRE = '{nom_dep}' AND LOC.ID_PROVINCIA = 97
                    """
                data = pd.read_sql(query, conn)
                res = data.iloc[0][0]
                loc = str(res)

            except:
                print("Error al obtener ID")
                loc = 'xxxxx'

            escribir_archivo(nombre_archivo_d, 'insert', 'a', id_cir_str, id_cir_str, nom, reg, renaper, loc)

            if int(reg) in (3, 5, 6):
                if flag_encabezado_kodak == 1:
                    flag_encabezado_kodak = 0
                    nombre_archivo_k = "PROD - 2 - INSERT DELEGACIONES KODAK.sql"
                    escribir_archivo(nombre_archivo_k, 'encabezado_kodak', 'w+', cant2, k_ini)

                escribir_archivo(nombre_archivo_k, 'insert_kodak', 'a', reg, renaper, id_cir_str)

        escribir_archivo(nombre_archivo_d, 'final', 'a')
        print(f"{nombre_archivo_d} generado correctamente")

        if flag_encabezado_kodak == 0:
            escribir_archivo(nombre_archivo_k, 'final_kodak', 'a')
            print(f"{nombre_archivo_k} generado correctamente")


if __name__ == "__main__":
    
    # df = pd.read_excel('delegaciones.xlsx')
    df = pd.read_csv('delegaciones.csv')
    pdtabulate=lambda df:tabulate(df,headers='keys',tablefmt='psql')
    print(pdtabulate(df))
    crear_scripts_sql(df)