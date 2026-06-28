import json

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Tamaño de lote para el envío a Kafka.
# Se evita enviar todos los mensajes en una sola llamada para no saturar
# el buffer del producer en datasets grandes.
BATCH_SIZE = 500


def load_to_kafka(final_path: str, bootstrap_servers: str, topic: str) -> str:
    """
    Envía todos los registros procesados a un topic de Apache Kafka.

    Los mensajes se envían en lotes para evitar saturar el buffer interno
    del producer y garantizar la confirmación de entrega de cada lote.
    """

    dataframe = pd.read_parquet(final_path)
    records = dataframe.to_dict(orient="records")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(
            value,
            default=str
        ).encode("utf-8"),
        acks="all",
        retries=3,
    )

    total_sent = 0

    try:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            futures = [producer.send(topic, value=record) for record in batch]

            for future in futures:
                future.get(timeout=10)
                total_sent += 1

            # Flush por lote para liberar el buffer del producer.
            producer.flush()

    except KafkaError as error:
        raise RuntimeError(f"Error enviando datos a Kafka: {error}") from error

    finally:
        producer.close()

    print(f"Se han enviado {total_sent} registros al topic de Kafka: {topic}")

    return f"Mensajes enviados correctamente al topic {topic}: {total_sent} registros"