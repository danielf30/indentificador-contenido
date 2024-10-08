import boto3
from botocore.exceptions import ClientError
import datetime

# Inicializar los clientes de DynamoDB y Step Functions
dynamodb = boto3.resource('dynamodb')
step_functions = boto3.client('stepfunctions')

# Nombre de la tabla de DynamoDB y ARN de Step Function
TABLE_NAME = 'links'
STATE_MACHINE_ARN = 'arn:aws:states:us-east-1:058264293944:stateMachine:MyStateMachine-0admxb4mz' # Cambia este ARN

def lambda_handler(event, context):
    # Referencia a la tabla
    table = dynamodb.Table(TABLE_NAME)

    try:
        # Realizar el scan con filtro
        response = table.scan(
            FilterExpression=(
                'Phase = :status_val AND contains(DateScheduled, :date_val)'
            ),
            ExpressionAttributeValues={
                ':status_val': 'sin_publicar',
                ':date_val': datetime.datetime.now().strftime('%Y%m%d')
            }
        )
        
        # Verificar si hay elementos que cumplan con la condición
        items = response.get('Items', [])
        if items:
            print(f"Se encontraron {len(items)} registros sin publicar.")
            
            # Iniciar Step Function
#            step_functions.start_execution(
#                stateMachineArn=STATE_MACHINE_ARN,
#                input=str(items)  # Puedes ajustar el input según lo que necesites
#            )
            print("Step Function iniciada con éxito.")
        else:
            print("No se encontraron registros sin publicar para la fecha actual.")

    except ClientError as e:
        print(e.response['Error']['Message'])
        raise e

