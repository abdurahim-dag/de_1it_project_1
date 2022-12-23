0-start:
	docker compose --env-file ./.env up -d
add-admin-airflow:
	docker exec -it airflow-webserver airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin@a.a --password admin
add-conn-pg_db:
	docker exec airflow-webserver bash -c "airflow connections add \"postgres-db\" --conn-json '{\
    \"conn_type\": \"postgres\",\
    \"login\": \"etl\",\
    \"password\": \"123qwe\",\
    \"host\": \"db-etl\",\
    \"port\": \"5432\",\
    \"schema\": \"postgres\"\
}'"
add-conn-api-5m-airflow:
	docker exec airflow-webserver bash -c "airflow connections add \"api_alphavantage\" --conn-json '{\
    \"conn_type\": \"http\",\
    \"host\": \"www.alphavantage.co\",\
    \"schema\": \"https\"\
}'"
add-variable-api-key-airflow:
	docker exec airflow-webserver airflow variables set api_key AW0MSMJE8BC3Y4FM

start: 0-start
add-connections-airflow: add-conn-pg_db add-conn-api-5m-airflow
add-user-airflow: add-admin-airflow