"""
Taller Guiado ETL - Distribuidora Andina S.A.S.
Plantilla para estudiantes

Integrantes:
    1.
    2.

Uso:
    python etl_andina.py datos/ventas_2026-03_lote1.csv

Reglas del taller:
    - Cada decisión que tome el código (rechazar, corregir, imputar) debe quedar
      registrada en la bitácora de decisiones con su justificación de negocio.
    - Nada se borra en silencio: todo lo que no se carga va a etl_rechazos con un motivo.
    - El script debe poder correrse dos veces sobre el mismo archivo sin cambiar los totales.
"""
import sys, json
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DATOS = Path(__file__).resolve().parent / "datos"
DB_URL = "postgresql+psycopg2://andina:andina@localhost:5432/andina_dw"


# =========================================================== 1. EXTRACT
def extraer_tiendas():
    ruta = DATOS / "tiendas.csv"
    return pd.read_csv(ruta, sep=";", dtype=str, encoding="cp1252")

def extraer_productos():
    ruta = DATOS / "productos.json"
    with open(ruta, "r", encoding="utf-8-sig") as archivo:
        datos = json.load(archivo)

    return pd.json_normalize(datos["productos"], sep="_")

def extraer_ventas(ruta):
    return pd.read_csv(ruta, dtype=str, encoding="utf-8-sig")

def perfilar(df, nombre):
    """TODO E4: imprimir un perfil rápido de un DataFrame:
    filas, columnas, nulos por columna, duplicados exactos y valores distintos
    de las columnas categóricas (cod_tienda, medio_pago)."""
    ...


# ========================================================= 2. TRANSFORM
def transformar_tiendas(df):
    df = df.copy()

    for columna in ["cod_tienda", "nombre_tienda", "ciudad", "departamento",
                    "administrador", "activa"]:
        if columna in df.columns:
            df[columna] = df[columna].astype("string").str.strip()

    df["cod_tienda"] = df["cod_tienda"].str.upper()
    df["ciudad"] = df["ciudad"].str.title()

    df["fecha_apertura"] = pd.to_datetime(
        df["fecha_apertura"], format="%Y-%m-%d", errors="coerce"
    )
    df["activa"] = df["activa"].str.upper().eq("S")

    df = df.drop_duplicates(subset=["cod_tienda"], keep="first")

    return df

def transformar_productos(df):
    df = df.copy()

    df["sku"] = df["sku"].astype("string").str.strip().str.upper()
    df["nombre"] = df["nombre"].astype("string").str.strip()
    df["categoria"] = df["categoria"].fillna("Sin categoría")
    df["precio_lista"] = pd.to_numeric(df["precio_lista"], errors="coerce")


    df = df.drop_duplicates(subset=["sku"], keep="first")

    return df

def transformar_ventas(df, productos, lote):
    """Limpia ventas y devuelve (ventas_limpias, rechazos)."""
    import unicodedata

    original = df.copy().reset_index(drop=True)
    df = original.copy()
    df["_fila_etl"] = range(len(df))
    rechazos = []

    def rechazar(mask, motivo):
        nonlocal df
        mask = pd.Series(mask, index=df.index).fillna(False).astype(bool)

        if mask.any(): 
            posiciones = df.loc[mask, "_fila_etl"].astype(int).tolist()
            registros = original.iloc[posiciones].copy()
            registros["motivo"] = motivo
            rechazos.append(registros)
            df = df.loc[~mask].copy()


    columnas_originales = list(original.columns)
    duplicados = df[columnas_originales].duplicated(keep="first")
    rechazar(duplicados, "duplicado_exacto")

    df["id_transaccion"] = df["id_transaccion"].astype("string").str.strip()
    df["cod_tienda"] = df["cod_tienda"].astype("string").str.strip().str.upper()
    df["sku"] = df["sku"].astype("string").str.strip().str.upper()

    rechazar(
    df.duplicated(subset=["id_transaccion"], keep="first"),
    "id_transaccion_repetido"
)

    rechazar(
        df["id_transaccion"].isna() | df["id_transaccion"].eq(""),
        "id_transaccion_vacio"
    )
    rechazar(
        df["cod_tienda"].isna() | df["cod_tienda"].eq(""),
        "cod_tienda_vacio"
    )

    productos = productos.copy()
    productos["sku"] = productos["sku"].astype("string").str.strip().str.upper()
    precios_lista = pd.to_numeric(productos["precio_lista"], errors="coerce")
    mapa_precios = dict(zip(productos["sku"], precios_lista))

    rechazar(
        ~df["sku"].isin(mapa_precios),
        "producto_no_encontrado"
    )

    fecha_texto = df["fecha"].astype("string").str.strip()
    fecha = pd.to_datetime(
        fecha_texto, format="%Y-%m-%d", errors="coerce"
    )
    fecha_alternativa = pd.to_datetime(
        fecha_texto, format="%d/%m/%Y", errors="coerce"
    )
    fecha = fecha.fillna(fecha_alternativa)
    df["_fecha"] = fecha

    rechazar(df["_fecha"].isna(), "fecha_invalida")

    rechazar(
        (df["_fecha"].dt.year != 2026) | (df["_fecha"].dt.month != 3),
        "fecha_fuera_marzo_2026"
    )


    df["_cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce")
    df["_precio"] = pd.to_numeric(df["precio_unitario"], errors="coerce")

    rechazar(
        df["_cantidad"].isna()
        | (df["_cantidad"] == 0)
        | (df["_cantidad"] % 1 != 0),
        "cantidad_invalida"
    )

    precio_faltante = df["_precio"].isna()
    df["_precio"] = df["_precio"].fillna(df["sku"].map(mapa_precios))
    df["_precio_imputado"] = precio_faltante & df["_precio"].notna()

    rechazar(
        df["_precio"].isna() | (df["_precio"] < 0),
        "precio_invalido"
    )


    descuento = pd.to_numeric(df["descuento"], errors="coerce")
    descuento = descuento.where(descuento <= 1, descuento / 100)

    rechazar(
        descuento.isna() | (descuento < 0) | (descuento > 1),
        "descuento_invalido"
    )
    df["_descuento"] = descuento

   
    def normalizar_pago(valor):
        texto = str(valor).strip()
        texto = (
            texto.replace("Ã©", "é")
                 .replace("Ã‰", "É")
                 .replace("Ã­", "í")
                 .replace("Ã³", "ó")
        )
        clave = unicodedata.normalize("NFKD", texto.lower())
        clave = "".join(
            c for c in clave if not unicodedata.combining(c)
        )
        clave = " ".join(clave.split())

        equivalencias = {
            "efectivo": "Efectivo",
            "td": "Tarjeta débito",
            "debito": "Tarjeta débito",
            "tarjeta debito": "Tarjeta débito",
            "tarjeta de debito": "Tarjeta débito",
            "tc": "Tarjeta crédito",
            "credito": "Tarjeta crédito",
            "tarjeta credito": "Tarjeta crédito",
            "tarjeta de credito": "Tarjeta crédito",
            "transferencia": "Transferencia",
            "nequi": "Nequi",
            "daviplata": "Daviplata",
        }
        return equivalencias.get(clave, texto.title())

    df["medio_pago"] = df["medio_pago"].astype("string").str.strip()
    rechazar(
        df["medio_pago"].isna() | df["medio_pago"].eq(""),
        "medio_pago_vacio"
    )
    df["medio_pago"] = df["medio_pago"].apply(normalizar_pago)

    df["fecha"] = df["_fecha"]
    df["fecha_key"] = (
        df["fecha"].dt.year * 10000
        + df["fecha"].dt.month * 100
        + df["fecha"].dt.day
    ).astype(int)

    df["cantidad"] = df["_cantidad"].astype(int)
    df["precio_unitario"] = df["_precio"].round(2)
    df["descuento"] = df["_descuento"]
    df["precio_imputado"] = df["_precio_imputado"].astype(bool)
    df["es_devolucion"] = df["cantidad"] < 0

    df["valor_bruto"] = (
        df["cantidad"] * df["precio_unitario"]
    ).round(2)

    df["valor_descuento"] = (
        df["valor_bruto"] * df["descuento"]
    ).round(2)

    df["valor_neto"] = (
        df["valor_bruto"] - df["valor_descuento"]
    ).round(2)

    df["lote_origen"] = lote

    columnas_salida = [
        "id_transaccion", "fecha", "fecha_key",
        "cod_tienda", "sku", "cantidad", "precio_unitario",
        "descuento", "medio_pago", "vendedor",
        "valor_bruto", "valor_descuento", "valor_neto",
        "es_devolucion", "precio_imputado", "lote_origen"
    ]

    ventas = df[columnas_salida].copy()

    if rechazos:
        rechazos_df = pd.concat(rechazos, ignore_index=True)
    else:
        rechazos_df = original.iloc[0:0].copy()
        rechazos_df["motivo"] = pd.Series(dtype="string")

    return ventas, rechazos_df

# ============================================================== 3. LOAD


def cargar(ventas, rechazos, tiendas, productos, lote, filas_leidas, inicio):
    import json
    from datetime import datetime
    from sqlalchemy import text

    engine = create_engine(DB_URL)

    def valor_seguro(valor):
        if valor is None:
            return None
        try:
            if pd.isna(valor):
                return None
        except (TypeError, ValueError):
            pass
        if hasattr(valor, "item"):
            valor = valor.item()
        if hasattr(valor, "to_pydatetime"):
            valor = valor.to_pydatetime()
        if hasattr(valor, "isoformat") and not isinstance(valor, str):
            return valor.isoformat()
        return valor

    def es_verdadero(valor):
        return str(valor).strip().lower() in {
            "true", "1", "s", "si", "sí", "yes"
        }

    try:
        with engine.begin() as con:

            ruta_sql = Path(__file__).resolve().parent / "modelo_estrella.sql"
            contenido_sql = ruta_sql.read_text(encoding="utf-8-sig")

            for sentencia in contenido_sql.split(";"):
                if "CREATE TABLE" in sentencia.upper():
                    con.execute(text(sentencia.strip()))

 
            tiendas = tiendas.drop_duplicates(
                subset=["cod_tienda"], keep="first"
            )

            for _, fila in tiendas.iterrows():
                con.execute(text("""
                    INSERT INTO dim_tienda
                    (cod_tienda, nombre_tienda, ciudad, departamento,
                     administrador, fecha_apertura, activa, es_inferida)
                    VALUES
                    (:cod, :nombre, :ciudad, :departamento,
                     :administrador, :apertura, :activa, FALSE)
                    ON CONFLICT (cod_tienda) DO UPDATE SET
                      nombre_tienda = EXCLUDED.nombre_tienda,
                      ciudad = EXCLUDED.ciudad,
                      departamento = EXCLUDED.departamento,
                      administrador = EXCLUDED.administrador,
                      fecha_apertura = EXCLUDED.fecha_apertura,
                      activa = EXCLUDED.activa
                """), {
                    "cod": str(fila["cod_tienda"]),
                    "nombre": valor_seguro(fila.get("nombre_tienda")),
                    "ciudad": valor_seguro(fila.get("ciudad")),
                    "departamento": valor_seguro(fila.get("departamento")),
                    "administrador": valor_seguro(fila.get("administrador")),
                    "apertura": valor_seguro(fila.get("fecha_apertura")),
                    "activa": es_verdadero(fila.get("activa")),
                })


            codigos_maestro = set(
                tiendas["cod_tienda"].astype(str).str.upper()
            )
            codigos_ventas = set(
                ventas["cod_tienda"].dropna().astype(str).str.upper()
            )
            faltantes = sorted(codigos_ventas - codigos_maestro)

            for codigo in faltantes:
                con.execute(text("""
                    INSERT INTO dim_tienda
                    (cod_tienda, nombre_tienda, activa, es_inferida)
                    VALUES (:cod, :nombre, TRUE, TRUE)
                    ON CONFLICT (cod_tienda) DO NOTHING
                """), {
                    "cod": codigo,
                    "nombre": f"Tienda inferida {codigo}",
                })

            productos = productos.drop_duplicates(
                subset=["sku"], keep="first"
            )

            for _, fila in productos.iterrows():
                con.execute(text("""
                    INSERT INTO dim_producto
                    (sku, nombre, categoria, marca, precio_lista, activo,
                     proveedor_nit, proveedor_razon_social)
                    VALUES
                    (:sku, :nombre, :categoria, :marca, :precio, :activo,
                     :nit, :proveedor)
                    ON CONFLICT (sku) DO UPDATE SET
                      nombre = EXCLUDED.nombre,
                      categoria = EXCLUDED.categoria,
                      marca = EXCLUDED.marca,
                      precio_lista = EXCLUDED.precio_lista,
                      activo = EXCLUDED.activo,
                      proveedor_nit = EXCLUDED.proveedor_nit,
                      proveedor_razon_social = EXCLUDED.proveedor_razon_social
                """), {
                    "sku": str(fila["sku"]),
                    "nombre": str(fila["nombre"]),
                    "categoria": valor_seguro(fila.get("categoria")),
                    "marca": valor_seguro(fila.get("marca")),
                    "precio": valor_seguro(fila.get("precio_lista")),
                    "activo": es_verdadero(fila.get("activo", True)),
                    "nit": valor_seguro(fila.get("proveedor_nit")),
                    "proveedor": valor_seguro(
                        fila.get("proveedor_razon_social")
                    ),
                })

      
            nombres_meses = [
                "enero", "febrero", "marzo", "abril", "mayo", "junio",
                "julio", "agosto", "septiembre", "octubre", "noviembre",
                "diciembre"
            ]
            nombres_dias = [
                "lunes", "martes", "miércoles", "jueves",
                "viernes", "sábado", "domingo"
            ]

            fechas = (
                pd.to_datetime(ventas["fecha"])
                .dropna()
                .dt.date.unique()
            )

            for fecha in fechas:
                dia_semana = fecha.weekday()
                con.execute(text("""
                    INSERT INTO dim_fecha
                    (fecha_key, fecha, anio, mes, nombre_mes, dia,
                     dia_semana, nombre_dia, semana_iso, quincena,
                     es_fin_semana, es_festivo)
                    VALUES
                    (:key, :fecha, :anio, :mes, :nombre_mes, :dia,
                     :dia_semana, :nombre_dia, :semana, :quincena,
                     :fin_semana, FALSE)
                    ON CONFLICT (fecha_key) DO NOTHING
                """), {
                    "key": fecha.year * 10000 + fecha.month * 100 + fecha.day,
                    "fecha": fecha,
                    "anio": fecha.year,
                    "mes": fecha.month,
                    "nombre_mes": nombres_meses[fecha.month - 1],
                    "dia": fecha.day,
                    "dia_semana": dia_semana,
                    "nombre_dia": nombres_dias[dia_semana],
                    "semana": fecha.isocalendar().week,
                    "quincena": 1 if fecha.day <= 15 else 2,
                    "fin_semana": dia_semana >= 5,
                })

  
            medios = sorted(
                ventas["medio_pago"].dropna().unique().tolist()
            )

            for medio in medios:
                con.execute(text("""
                    INSERT INTO dim_medio_pago (medio_pago)
                    VALUES (:medio)
                    ON CONFLICT (medio_pago) DO NOTHING
                """), {"medio": medio})


            tiendas_key = {
                fila.cod_tienda: fila.tienda_key
                for fila in con.execute(text(
                    "SELECT cod_tienda, tienda_key FROM dim_tienda"
                )).fetchall()
            }
            productos_key = {
                fila.sku: fila.producto_key
                for fila in con.execute(text(
                    "SELECT sku, producto_key FROM dim_producto"
                )).fetchall()
            }
            medios_key = {
                fila.medio_pago: fila.medio_pago_key
                for fila in con.execute(text(
                    "SELECT medio_pago, medio_pago_key FROM dim_medio_pago"
                )).fetchall()
            }

           
            con.execute(text("""
                CREATE TEMP TABLE stg_fact_ventas
                (LIKE fact_ventas INCLUDING DEFAULTS)
                ON COMMIT DROP
            """))

            hechos = []

            for _, fila in ventas.iterrows():
                codigo_tienda = str(fila["cod_tienda"]).upper()
                sku = str(fila["sku"]).upper()
                medio = str(fila["medio_pago"])

                if codigo_tienda not in tiendas_key:
                    raise ValueError(
                        f"No se encontró la tienda {codigo_tienda}"
                    )
                if sku not in productos_key:
                    raise ValueError(f"No se encontró el producto {sku}")
                if medio not in medios_key:
                    raise ValueError(
                        f"No se encontró el medio de pago {medio}"
                    )

                hechos.append({
                    "id_transaccion": str(fila["id_transaccion"]),
                    "fecha_key": int(fila["fecha_key"]),
                    "tienda_key": tiendas_key[codigo_tienda],
                    "producto_key": productos_key[sku],
                    "medio_pago_key": medios_key[medio],
                    "cantidad": int(fila["cantidad"]),
                    "precio_unitario": float(fila["precio_unitario"]),
                    "valor_bruto": float(fila["valor_bruto"]),
                    "valor_descuento": float(fila["valor_descuento"]),
                    "valor_neto": float(fila["valor_neto"]),
                    "es_devolucion": bool(fila["es_devolucion"]),
                    "precio_imputado": bool(fila["precio_imputado"]),
                    "lote_origen": str(fila["lote_origen"]),
                    "vendedor": valor_seguro(fila.get("vendedor")),
                })

       
            unicos = {}
            for hecho in hechos:
                unicos[hecho["id_transaccion"]] = hecho
            hechos = list(unicos.values())

            columnas_hecho = [
                "id_transaccion", "fecha_key", "tienda_key",
                "producto_key", "medio_pago_key", "cantidad",
                "precio_unitario", "valor_bruto", "valor_descuento",
                "valor_neto", "es_devolucion", "precio_imputado",
                "lote_origen", "vendedor"
            ]

            if hechos:
                nombres = ", ".join(columnas_hecho)
                parametros = ", ".join(
                    f":{columna}" for columna in columnas_hecho
                )
                con.execute(text(
                    f"INSERT INTO stg_fact_ventas ({nombres}) "
                    f"VALUES ({parametros})"
                ), hechos)

            conteos = con.execute(text("""
                SELECT
                  COUNT(*) FILTER (
                    WHERE f.id_transaccion IS NULL
                  ) AS nuevas,

                  COUNT(*) FILTER (
                    WHERE f.id_transaccion IS NOT NULL
                    AND ROW(
                      f.fecha_key, f.tienda_key, f.producto_key,
                      f.medio_pago_key, f.cantidad, f.precio_unitario,
                      f.valor_bruto, f.valor_descuento, f.valor_neto,
                      f.es_devolucion, f.precio_imputado, f.vendedor
                    ) IS DISTINCT FROM ROW(
                      s.fecha_key, s.tienda_key, s.producto_key,
                      s.medio_pago_key, s.cantidad, s.precio_unitario,
                      s.valor_bruto, s.valor_descuento, s.valor_neto,
                      s.es_devolucion, s.precio_imputado, s.vendedor
                    )
                  ) AS actualizadas,

                  COUNT(*) FILTER (
                    WHERE f.id_transaccion IS NOT NULL
                    AND ROW(
                      f.fecha_key, f.tienda_key, f.producto_key,
                      f.medio_pago_key, f.cantidad, f.precio_unitario,
                      f.valor_bruto, f.valor_descuento, f.valor_neto,
                      f.es_devolucion, f.precio_imputado, f.vendedor
                    ) IS NOT DISTINCT FROM ROW(
                      s.fecha_key, s.tienda_key, s.producto_key,
                      s.medio_pago_key, s.cantidad, s.precio_unitario,
                      s.valor_bruto, s.valor_descuento, s.valor_neto,
                      s.es_devolucion, s.precio_imputado, s.vendedor
                    )
                  ) AS sin_cambio

                FROM stg_fact_ventas s
                LEFT JOIN fact_ventas f
                  ON f.id_transaccion = s.id_transaccion
            """)).mappings().one()


            con.execute(text("""
                INSERT INTO fact_ventas AS f
                (id_transaccion, fecha_key, tienda_key, producto_key,
                 medio_pago_key, cantidad, precio_unitario, valor_bruto,
                 valor_descuento, valor_neto, es_devolucion,
                 precio_imputado, lote_origen, vendedor)
                SELECT
                 id_transaccion, fecha_key, tienda_key, producto_key,
                 medio_pago_key, cantidad, precio_unitario, valor_bruto,
                 valor_descuento, valor_neto, es_devolucion,
                 precio_imputado, lote_origen, vendedor
                FROM stg_fact_ventas
                ON CONFLICT (id_transaccion) DO UPDATE SET
                  fecha_key = EXCLUDED.fecha_key,
                  tienda_key = EXCLUDED.tienda_key,
                  producto_key = EXCLUDED.producto_key,
                  medio_pago_key = EXCLUDED.medio_pago_key,
                  cantidad = EXCLUDED.cantidad,
                  precio_unitario = EXCLUDED.precio_unitario,
                  valor_bruto = EXCLUDED.valor_bruto,
                  valor_descuento = EXCLUDED.valor_descuento,
                  valor_neto = EXCLUDED.valor_neto,
                  es_devolucion = EXCLUDED.es_devolucion,
                  precio_imputado = EXCLUDED.precio_imputado,
                  lote_origen = EXCLUDED.lote_origen,
                  vendedor = EXCLUDED.vendedor
                WHERE ROW(
                  f.fecha_key, f.tienda_key, f.producto_key,
                  f.medio_pago_key, f.cantidad, f.precio_unitario,
                  f.valor_bruto, f.valor_descuento, f.valor_neto,
                  f.es_devolucion, f.precio_imputado, f.vendedor
                ) IS DISTINCT FROM ROW(
                  EXCLUDED.fecha_key, EXCLUDED.tienda_key,
                  EXCLUDED.producto_key, EXCLUDED.medio_pago_key,
                  EXCLUDED.cantidad, EXCLUDED.precio_unitario,
                  EXCLUDED.valor_bruto, EXCLUDED.valor_descuento,
                  EXCLUDED.valor_neto, EXCLUDED.es_devolucion,
                  EXCLUDED.precio_imputado, EXCLUDED.vendedor
                )
            """))

            for _, fila in rechazos.iterrows():
                motivo = str(fila.get("motivo", "sin_motivo"))
                registro = {
                    str(k): valor_seguro(v)
                    for k, v in fila.drop(
                        labels=["motivo"], errors="ignore"
                    ).items()
                }

                con.execute(text("""
                    INSERT INTO etl_rechazos
                    (lote, id_transaccion, motivo, registro)
                    VALUES (:lote, :id, :motivo, CAST(:registro AS JSONB))
                """), {
                    "lote": lote,
                    "id": valor_seguro(fila.get("id_transaccion")),
                    "motivo": motivo,
                    "registro": json.dumps(registro, ensure_ascii=False),
                })


       
            fin = datetime.now()

            con.execute(text("""
                INSERT INTO etl_log
                (lote, inicio, fin, filas_leidas, filas_rechazadas,
                 filas_insertadas, filas_actualizadas, filas_sin_cambio)
                VALUES
                (:lote, :inicio, :fin, :leidas, :rechazadas,
                 :insertadas, :actualizadas, :sin_cambio)
            """), {
                "lote": lote,
                "inicio": inicio,
                "fin": fin,
                "leidas": int(filas_leidas),
                "rechazadas": int(len(rechazos)),
                "insertadas": int(conteos["nuevas"]),
                "actualizadas": int(conteos["actualizadas"]),
                "sin_cambio": int(conteos["sin_cambio"]),
            })

    finally:
        engine.dispose()

# ============================================================== MAIN
def main(ruta):
    inicio = datetime.now()
    ruta = Path(ruta)
    lote = ruta.stem

    tiendas = transformar_tiendas(extraer_tiendas())
    productos = transformar_productos(extraer_productos())
    crudo = extraer_ventas(ruta)
    perfilar(crudo, lote)

    ventas, rechazos = transformar_ventas(crudo.copy(), productos, lote)

    # Control de cuadre: ninguna fila se pierde sin explicación
    assert len(ventas) + len(rechazos) == len(crudo), "¡Hay filas que desaparecieron sin quedar en rechazos!"

    cargar(ventas, rechazos, tiendas, productos, lote, len(crudo), inicio)
    print(f"{lote}: leídas {len(crudo)}, cargadas {len(ventas)}, rechazadas {len(rechazos)}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DATOS / "ventas_2026-03_lote1.csv")
