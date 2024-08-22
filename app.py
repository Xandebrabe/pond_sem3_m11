from flask import Flask, request, jsonify
from datetime import datetime
from deco.minio_client import create_bucket_if_not_exists, upload_file, download_file
from deco.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from deco.data_processing import process_data, prepare_dataframe_for_insert
import logging
import pandas as pd

app = Flask(__name__)

# Criar bucket se não existir
create_bucket_if_not_exists("raw-data")

# Executar o script SQL para criar a tabela
execute_sql_script('sql/create_table.sql')
logging.basicConfig(level=logging.INFO)

@app.route('/data', methods=['POST'])
def receive_data():
    data = pd.read_csv('./store_final.csv')
    logging.info(f"CSV data:\n{data.head()}")
    # if not data or 'date' not in data or 'dados' not in data:
    #     return jsonify({"error": "Formato de dados inválido"}), 400

    # try:
    #     datetime.fromtimestamp(data['date'])
    #     int(data['dados'])
    # except (ValueError, TypeError):
    #     return jsonify({"error": "Tipo de dados inválido"}), 400

    # Processar e salvar dados
    filename = process_data(data)
    upload_file("raw-data", filename)

    # Ler arquivo Parquet do MinIO
    download_file("raw-data", filename, f"downloaded_{filename}")
    df_parquet = pd.read_parquet(f"downloaded_{filename}")

    # Preparar e inserir dados no ClickHouse    
    df_prepared = prepare_dataframe_for_insert(df_parquet)
    client = get_client()  # Obter o cliente ClickHouse
    insert_dataframe(client, 'working_data', df_prepared)

    return jsonify({"message": "Dados recebidos, armazenados e processados com sucesso"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)