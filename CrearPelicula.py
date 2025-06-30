import boto3
import uuid
import os
import json

def load_body(event):
    if 'body' not in event:
        return event
    if isinstance(event["body"], dict):
        return event['body']
    else:
        return json.loads(event['body'])

def log(tipo, data):
    data = json.dumps({
        "type": tipo,
        "message": data
    })
    print(data)


def info(data):
    log("INFO", data)

def error(data):
    log("ERROR", data)



def lambda_handler(event, context):
    try:
        # Entrada
        info({"event": event})

        body = event.get("body")
        if isinstance(body, str):
            body = json.loads(body)

        tenant_id = body["tenant_id"]
        pelicula_datos = body["pelicula_datos"]
        nombre_tabla = os.environ["TABLE_NAME"]

        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # Log exitoso
        info({"pelicula_guardada": pelicula})

        return {
            'statusCode': 200,
            'body': json.dumps({
                'mensaje': 'Película creada correctamente',
                'pelicula': pelicula
            })
        }

    except Exception as e:
        # Log de error
        error({
            "mensaje": "Error al crear película",
            "error": str(e)
        })

        return {
            'statusCode': 500,
            'body': json.dumps({
                'mensaje': 'Error interno al crear película',
                'error': str(e)
            })
        }