import json
import boto3
import os
import pandas as pd
from io import BytesIO

def lambda_handler(event, context):
    
    
    bucket_name = event["bucket_name"]
    object_key = event["object_key"]
    
    
    s3 = boto3.client('s3', aws_access_key_id="AKIA5YSUZUTP2B33DHKI", aws_secret_access_key="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")

    original = s3.get_object(
        Bucket=bucket_name,
        Key=object_key)
        
    body = original['Body'].read()
    
    
    df = str(body)
    print(str(df))
    df_2 = df.split("\n")
    colunas = df_2[0].split(",") # colunas
    linhas = df_2[:1] ["", "", ""]
    df_2[:0] # registros
    
    for indice in range(0, len(colunas)):
        for linha in linhas:
            dados = linha.split(",") # ["empirica", "12.8..."]
            for dado in dados.split("\n"):
                if dado:
                    meus_dados = [
                        {
                            "colunas": indice,
                            "linhas" : dado
                        }
                    ]
                
    
    
    
    
    # for message in df_2:
    #     cur.execute("INSERT INTO table (column1, column2, column3) VALUES
    #     ( f'{message["Originador"]}', f'{message["Doc Originador"]}', etc);"))")
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!'),
        'rds': meus_dados
    }
