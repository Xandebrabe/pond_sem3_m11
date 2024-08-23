Comandos para rodar:

`docker-compose up --build`

`poetry run python .\app.py`

para mandar a requisição e acontecer o processamento de dados:

curl

curl --location 'http://localhost:5000/data' \
--header 'Content-Type: application/json' \
--data '{
    "date": 1692345600,
    "dados": 12345
}'

e por fim o código para rodar a view:

CREATE VIEW IF NOT EXISTS data_view AS
SELECT
    data_ingestao,
    JSONExtractInt(dado_linha, 'date') AS date_unix,
    JSONExtract(dado_linha, 'regiao', 'String') AS dados,
    toDateTime(JSONExtractInt(dado_linha, 'data_ingestao') / 1000) AS data_ingestao_datetime
FROM working_data;
