# %% [markdown]
# # Procesamiento batch de Online Retail con Apache Spark
# 
# Notebook organizado por etapas para cargar, limpiar y analizar el conjunto de datos Online Retail.

# %%
import os
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Configurar Java si está instalado en esta ruta
java_home = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
if Path(java_home).exists():
    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = str(Path(java_home) / "bin") + os.pathsep + os.environ.get("PATH", "")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
print("Entorno preparado.")
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# %%
# Buscar Online Retail.xlsx en la carpeta data del proyecto
rutas_excel = [
    Path.cwd() / "data" / "Online Retail.xlsx",
    Path.cwd().parent / "data" / "Online Retail.xlsx"
]
ruta_excel = next((ruta for ruta in rutas_excel if ruta.exists()), None)

if ruta_excel is None:
    raise FileNotFoundError("No se encontró Online Retail.xlsx. Comprueba que esté en la carpeta data del proyecto.")

ventas_pd = pd.read_excel(ruta_excel, engine="openpyxl")
print("Archivo cargado:", ruta_excel)
print("Dimensiones iniciales:", ventas_pd.shape)
display(ventas_pd.head())

# %% [markdown]
# ## 1. Inspección y limpieza de los datos

# %%
print("Valores faltantes por columna:")
display(ventas_pd.isnull().sum().to_frame("Valores faltantes"))
print("Registros duplicados:", ventas_pd.duplicated().sum())

ventas_pd["InvoiceNo"] = ventas_pd["InvoiceNo"].astype("string").str.strip()
ventas_pd["StockCode"] = ventas_pd["StockCode"].astype("string").str.strip()
ventas_pd["Country"] = ventas_pd["Country"].astype("string").str.strip()
ventas_pd["Description"] = ventas_pd["Description"].fillna("Sin descripción").astype(str)
ventas_pd["Quantity"] = pd.to_numeric(ventas_pd["Quantity"], errors="coerce")
ventas_pd["UnitPrice"] = pd.to_numeric(ventas_pd["UnitPrice"], errors="coerce")
ventas_pd["InvoiceDate"] = pd.to_datetime(ventas_pd["InvoiceDate"], errors="coerce")

ventas_pd = ventas_pd.dropna(subset=["InvoiceNo", "StockCode", "InvoiceDate", "Quantity", "UnitPrice", "Country"]).copy()
ventas_pd = ventas_pd[(ventas_pd["Quantity"] > 0) & (ventas_pd["UnitPrice"] > 0)].copy()
ventas_pd["Quantity"] = ventas_pd["Quantity"].astype("int32")
ventas_pd["UnitPrice"] = ventas_pd["UnitPrice"].astype("float64")
ventas_pd["Revenue"] = ventas_pd["Quantity"] * ventas_pd["UnitPrice"]
print("Dimensiones después de limpiar:", ventas_pd.shape)
display(ventas_pd.head())

# %% [markdown]
# ## 2. Crear la sesión de Apache Spark y el DataFrame

# %%
spark = (
    SparkSession.builder
    .appName("Analisis de ventas Online Retail")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Versión de Spark:", spark.version)

# %%
# ============================================================
# CREAR DATAFRAME SPARK - ONLINE RETAIL - UNAD
# Compatible con el esquema definido para Spark 3.5.1
# ============================================================

import pandas as pd
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, TimestampType
)

# 1. Seleccionar y copiar las columnas
columnas = [
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country", "Revenue"
]

datos = ventas_pd[columnas].copy()

# 2. Limpiar y convertir los tipos de datos
for columna in ["InvoiceNo", "StockCode", "Description", "CustomerID", "Country"]:
    datos[columna] = datos[columna].map(
        lambda valor: None if pd.isna(valor) else str(valor)
    )

datos["InvoiceDate"] = pd.to_datetime(
    datos["InvoiceDate"], errors="coerce"
)
datos["Quantity"] = pd.to_numeric(datos["Quantity"], errors="coerce")
datos["UnitPrice"] = pd.to_numeric(datos["UnitPrice"], errors="coerce")
datos["Revenue"] = pd.to_numeric(datos["Revenue"], errors="coerce")

# 3. Eliminar registros con campos esenciales inválidos
datos = datos.dropna(
    subset=["InvoiceDate", "Quantity", "UnitPrice", "Revenue"]
).copy()

# 4. Definir el esquema de Spark
esquema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Revenue", DoubleType(), True)
])

# 5. Construir filas con tipos nativos de Python
filas = []

for fila in datos.itertuples(index=False, name=None):
    fecha_original = fila[4]

    # Convertir explícitamente la fecha a datetime.datetime de Python
    if isinstance(fecha_original, pd.Timestamp):
        fecha = fecha_original.to_pydatetime()
    else:
        fecha = pd.Timestamp(fecha_original).to_pydatetime()

    fecha = datetime(
        fecha.year, fecha.month, fecha.day,
        fecha.hour, fecha.minute, fecha.second,
        fecha.microsecond
    )

    filas.append((
        fila[0],
        fila[1],
        fila[2],
        int(fila[3]),
        fecha,
        float(fila[5]),
        fila[6],
        fila[7],
        float(fila[8])
    ))

# 6. Crear el DataFrame de Spark
ventas_spark = spark.createDataFrame(filas, schema=esquema)

# 7. Verificar el resultado
print("DataFrame de Spark creado correctamente.")
print("Registros en Spark:", ventas_spark.count())
print("\nEsquema:")
ventas_spark.printSchema()
print("\nPrimeros cinco registros:")
ventas_spark.show(5, truncate=False)

# %% [markdown]
# ## 3. Resumen general de las ventas

# %%
ventas_spark.select("Quantity", "UnitPrice", "Revenue").summary().show()
print("Facturas diferentes:", ventas_spark.select("InvoiceNo").distinct().count())
print("Países diferentes:", ventas_spark.select("Country").distinct().count())
ventas_spark.agg(F.sum("Quantity").alias("Total_unidades")).show()
ventas_spark.agg(F.round(F.sum("Revenue"), 2).alias("Ingresos_totales")).show()

# %% [markdown]
# ## 4. Ingresos por país

# %%
ingresos_pais = (ventas_spark.groupBy("Country")
    .agg(F.round(F.sum("Revenue"), 2).alias("Ingresos"))
    .orderBy(F.desc("Ingresos")))
ingresos_pais.show(15, truncate=False)

# %% [markdown]
# ## 5. Productos más vendidos por unidades

# %%
productos_vendidos = (ventas_spark.groupBy("StockCode", "Description")
    .agg(F.sum("Quantity").alias("Unidades_vendidas"))
    .orderBy(F.desc("Unidades_vendidas")))
productos_vendidos.show(10, truncate=False)

# %% [markdown]
# ## 6. RDD y reduceByKey

# %%
rdd_ingresos = ventas_spark.select("Country", "Revenue").rdd.map(lambda fila: (fila["Country"], fila["Revenue"]))
ingresos_rdd = rdd_ingresos.reduceByKey(lambda a, b: a + b)
resultado_rdd = ingresos_rdd.sortBy(lambda elemento: elemento[1], ascending=False).take(10)
for pais, ingresos in resultado_rdd:
    print(f"{pais}: {ingresos:,.2f}")

# %% [markdown]
# ## 7. Gráficas

# %%
datos_grafica = ingresos_pais.limit(10).toPandas().sort_values("Ingresos", ascending=True)
plt.figure(figsize=(10, 6))
plt.barh(datos_grafica["Country"], datos_grafica["Ingresos"])
plt.title("Top 10 de países por ingresos")
plt.xlabel("Ingresos")
plt.ylabel("País")
plt.tight_layout()
plt.show()

# %%
datos_productos = productos_vendidos.limit(10).toPandas().sort_values("Unidades_vendidas", ascending=True)
etiquetas = datos_productos["Description"].str.slice(0, 35)
plt.figure(figsize=(10, 6))
plt.barh(etiquetas, datos_productos["Unidades_vendidas"])
plt.title("Top 10 de productos por unidades vendidas")
plt.xlabel("Unidades vendidas")
plt.ylabel("Producto")
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 8. Cerrar Spark
# 
# Ejecuta esta celda solamente después de terminar todos los análisis.

# %%
spark.stop()
print("Sesión de Spark cerrada correctamente.")

# %%
%pip install matplotlib

# %%
import sys
import os
import subprocess

print("Python:", sys.executable)
print("JAVA_HOME:", os.environ.get("JAVA_HOME"))

resultado = subprocess.run(
    ["java", "-version"],
    capture_output=True,
    text=True
)

print(resultado.stderr or resultado.stdout)


