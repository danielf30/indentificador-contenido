import json
import boto3
import datetime
from botocore.exceptions import ClientError

# Inicializar los clientes de DynamoDB y Step Functions
dynamodb = boto3.resource('dynamodb')
step_functions = boto3.client('stepfunctions')

# Nombre de la tabla de DynamoDB y ARN de Step Function
TABLE_NAME = 'links'
STATE_MACHINE_ARN = 'arn:aws:states:us-east-1:058264293944:stateMachine:MyStateMachine-0admxb4mz'  # Cambia este ARN

def lambda_handler(event, context):
    # Referencia a la tabla
    table = dynamodb.Table(TABLE_NAME)

    try:
        # Realizar el scan con el nuevo nombre de campo 'phase'
        response = table.scan(
            FilterExpression=(
                'Phase = :phase_val AND contains(DateScheduled, :date_val)'
            ),
            ExpressionAttributeValues={
                ':phase_val': 'sin_publicar',
                ':date_val': datetime.datetime.now().strftime('%Y%m%d')
            }
        )
        items = response.get('Items', [])

        if items:
            print(f"Se encontraron {len(items)} registros sin publicar.")
            input_data = json.dumps(items)
            
            # Iniciar Step Function
            step_functions.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                input=input_data  # Ajusta el input según sea necesario
            )
            print("Step Function iniciada con éxito.")
            return items
        else:
            print("No se encontraron registros sin publicar para la fecha actual.")
            return "No matching records found."

    except ClientError as e:
        print(e.response['Error']['Message'])
        raise e


# Simular la ejecución local con un evento de prueba
if __name__ == "__main__":
    # Crear un evento simulado
    event = {}

    # Ejecutar la función Lambda con el evento simulado
    result = lambda_handler(event, context=None)
    
    # Imprimir el resultado de la ejecución
    print(result)