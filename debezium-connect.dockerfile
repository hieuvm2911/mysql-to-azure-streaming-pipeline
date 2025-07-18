FROM alpine:latest
RUN apk add --no-cache bash curl

COPY ./kafka /kafka

CMD bash -c '\
  curl --fail -i -X POST http://debezium:8083/connectors \
    -H "Accept:application/json" \
    -H "Content-Type:application/json" \
    -d @/kafka/debezium_mysql_connector.json'