FROM debezium/connect:2.6

USER root

RUN curl -fsSL \
  "https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-elasticsearch/versions/14.0.12/confluentinc-kafka-connect-elasticsearch-14.0.12.zip" \
  -o /tmp/es-connector.zip \
  && unzip /tmp/es-connector.zip -d /kafka/connect/ \
  && rm /tmp/es-connector.zip

USER 1001
