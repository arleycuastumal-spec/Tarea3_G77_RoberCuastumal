Tarea 3 – Procesamiento de datos con Apache Spark

1. Descripción del proyecto

Este proyecto desarrolla un proceso de análisis de datos por lotes (batch processing) utilizando Python, Pandas y Apache Spark, como parte de la actividad Tarea 3 de Big Data de la UNAD.

Se utiliza el conjunto de datos público Online Retail, que contiene información de transacciones comerciales. El propósito es preparar, limpiar y analizar los datos para obtener información útil sobre las ventas.

2. Herramientas utilizadas

Python 3.11

Apache Spark / PySpark 3.5.1

Pandas

Matplotlib

OpenPyXL

Jupyter Notebook

Visual Studio Code

3. Procesamiento realizado

El desarrollo incluye las siguientes actividades:

Carga del conjunto de datos Online Retail desde un archivo Excel.

Limpieza y preparación de los registros.

Conversión de los tipos de datos.

Cálculo de la columna Revenue para representar el valor de cada línea de venta.

Creación de un DataFrame de Spark.

Verificación de la estructura y cantidad de registros.

Análisis exploratorio y visualización de resultados.

Después de la limpieza, el DataFrame de Spark contiene 530.104 registros y nueve columnas.

4. Archivos del proyecto

procesamiento_batch_online_retail_jupyter_corregido.ipynb: notebook con el código y los resultados del procesamiento batch.

README.md: descripción del proyecto e instrucciones de ejecución.

data/Online Retail.xlsx: conjunto de datos utilizado, si se incluye en la estructura local del proyecto.

5. Requisitos

Se requiere tener instalado Python 3.11, Java 17 y las librerías necesarias para ejecutar PySpark y el notebook.

Las dependencias principales se pueden instalar desde una terminal con:

py -3.11 -m pip install pyspark==3.5.1 pandas openpyxl matplotlib ipykernel

6. Instrucciones de ejecución

Descargar o clonar el repositorio.

Ubicar el archivo Online Retail.xlsx dentro de la carpeta data.

Abrir el notebook en Visual Studio Code.

Seleccionar el intérprete de Python 3.11 que tenga instaladas las dependencias.

Ejecutar las celdas en orden, desde la carga de datos hasta los análisis y las visualizaciones.

Comprobar los resultados del procesamiento batch.

La ruta del archivo de datos debe coincidir con la utilizada en el código.

7. Resultado esperado

El proceso permite preparar los datos de ventas y analizarlos mediante Apache Spark. La ejecución realizada produjo un DataFrame con 530.104 registros y nueve columnas.

8. Autoría

Estudiante: Rober Arley cuastumal Quenan

Programa: Ingeniería Industrial

Institución: Universidad Nacional Abierta y a Distancia (UNAD)

Actividad: Tarea 3 – Procesamiento de datos con Apache Spark