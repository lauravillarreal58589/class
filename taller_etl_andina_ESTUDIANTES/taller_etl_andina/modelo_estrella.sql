-- =====================================================================
-- Bodega de datos Distribuidora Andina - Modelo estrella
-- Plantilla para estudiantes: completen los TODO
-- =====================================================================

-- Dimensión de fecha (entregada completa como referencia de estilo)
CREATE TABLE IF NOT EXISTS dim_fecha (
    fecha_key        INTEGER PRIMARY KEY,          -- AAAAMMDD, p. ej. 20260323
    fecha            DATE NOT NULL UNIQUE,
    anio             SMALLINT NOT NULL,
    mes              SMALLINT NOT NULL,
    nombre_mes       VARCHAR(12) NOT NULL,
    dia              SMALLINT NOT NULL,
    dia_semana       SMALLINT NOT NULL,            -- 1 = lunes ... 7 = domingo
    nombre_dia       VARCHAR(10) NOT NULL,
    semana_iso       SMALLINT NOT NULL,
    quincena         SMALLINT NOT NULL,
    es_fin_semana    BOOLEAN NOT NULL,
    es_festivo       BOOLEAN NOT NULL DEFAULT FALSE -- en marzo de 2026: el lunes 23 (San José)
);


CREATE TABLE IF NOT EXISTS dim_tienda (
    tienda_key SERIAL PRIMARY KEY,
    cod_tienda VARCHAR(10) NOT NULL UNIQUE,
    nombre_tienda VARCHAR(100),
    ciudad VARCHAR(100),
    departamento VARCHAR(100),
    administrador VARCHAR(100),
    fecha_apertura DATE,
    activa BOOLEAN NOT NULL DEFAULT TRUE,
    es_inferida BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_producto (
    producto_key SERIAL PRIMARY KEY,
    sku VARCHAR(30) NOT NULL UNIQUE,
    nombre VARCHAR(150) NOT NULL,
    categoria VARCHAR(100),
    marca VARCHAR(100),
    precio_lista NUMERIC(12,2),
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    proveedor_nit VARCHAR(20),
    proveedor_razon_social VARCHAR(150)
);

CREATE TABLE IF NOT EXISTS dim_medio_pago (
    medio_pago_key SERIAL PRIMARY KEY,
    medio_pago VARCHAR(50) NOT NULL UNIQUE
);


-- M4: Tabla de hechos de ventas
-- Grano: una fila por id_transaccion.
CREATE TABLE IF NOT EXISTS fact_ventas (
    id_transaccion VARCHAR(20) PRIMARY KEY,

    fecha_key INTEGER NOT NULL
        REFERENCES dim_fecha(fecha_key),

    tienda_key INTEGER NOT NULL
        REFERENCES dim_tienda(tienda_key),

    producto_key INTEGER NOT NULL
        REFERENCES dim_producto(producto_key),

    medio_pago_key INTEGER NOT NULL
        REFERENCES dim_medio_pago(medio_pago_key),

    cantidad INTEGER NOT NULL,
    precio_unitario NUMERIC(12,2) NOT NULL,
    valor_bruto NUMERIC(14,2) NOT NULL,
    valor_descuento NUMERIC(14,2) NOT NULL,
    valor_neto NUMERIC(14,2) NOT NULL,

    es_devolucion BOOLEAN NOT NULL DEFAULT FALSE,
    precio_imputado BOOLEAN NOT NULL DEFAULT FALSE,

    lote_origen VARCHAR(40) NOT NULL,
    vendedor VARCHAR(30)
);
-- Tablas de control del ETL (entregadas completas)
CREATE TABLE IF NOT EXISTS etl_rechazos (
    id               SERIAL PRIMARY KEY,
    lote             VARCHAR(40) NOT NULL,
    id_transaccion   VARCHAR(20),
    motivo           VARCHAR(80) NOT NULL,
    registro         JSONB NOT NULL,              -- la fila original tal como llegó
    registrado_en    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS etl_log (
    id                 SERIAL PRIMARY KEY,
    lote               VARCHAR(40) NOT NULL,
    inicio             TIMESTAMP NOT NULL,
    fin                TIMESTAMP NOT NULL,
    filas_leidas       INTEGER NOT NULL,
    filas_rechazadas   INTEGER NOT NULL,
    filas_insertadas   INTEGER NOT NULL,
    filas_actualizadas INTEGER NOT NULL,
    filas_sin_cambio   INTEGER NOT NULL
);
