# SDPD2 — Proyecto de Programación

Autor: Francisco Javier Jareño Ramírez
Asignatura: Sistemas Distribuidos de Procesamiento de Datos II
Universidad Rey Juan Carlos — Curso 2025/2026

## Estructura del repositorio

El proyecto está dividido en dos carpetas, una por cada fase:

- fase1: Pipeline ETL con Apache Airflow y Kafka
- fase2: Spark Structured Streaming con Kafka

## Fase 1 — Pipeline ETL con Apache Airflow

Pipeline ETL sobre el dataset de Netflix implementado con Apache Airflow usando decoradores TaskFlow. El pipeline tiene 5 tareas que se ejecutan en secuencia: extracción, limpieza, validación, transformación y carga en Kafka.

Los ficheros principales son:

- dags/data_pipeline_dag.py: DAG principal con las 5 tareas
- tasks/: módulos Python de cada tarea (extract, clean, validate, transform, kafka_loader)
- config.toml: variables de configuración
- docker-compose.yml: orquestación de contenedores (Airflow, Kafka, Zookeeper, PostgreSQL)

Para ejecutarlo hay que arrancar Docker Compose y acceder a la interfaz web en http://localhost:8080 con usuario admin y contraseña admin. Desde ahí se activa el DAG netflix_etl_kafka_pipeline y se lanza con el botón Trigger DAG.

## Fase 2 — Spark Structured Streaming

Script de PySpark 4.1.1 que lee el topic purchases de Kafka usando Structured Streaming y ejecuta dos consultas con modos de salida diferentes (append y complete).

Los ficheros principales son:

- kafka-producer-confluent.py: producer que envía eventos al topic purchases
- kafka-consumer-confluent.py: consumer para verificar los mensajes
- struct_kafka_consumer_local.py: script principal de Spark con las dos consultas
- salida1.txt: resultado de la consulta 1 (outputMode append)
- salida2.txt: resultado de la consulta 2 (outputMode complete)

Para ejecutarlo hay que tener Kafka corriendo (arrancando Docker Compose desde fase1), Java 17 instalado y PySpark 4.1.1. Primero se ejecuta el producer, luego opcionalmente el consumer para verificar, y por último el script de Spark.
