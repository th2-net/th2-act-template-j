FROM openjdk:12-alpine
ENV GRPC_PORT=8080 \
    RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_USER="" \
    RABBITMQ_PASS="" \
    RABBITMQ_VHOST=th2 \
    RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY="" \
    TH2_VERIFIER_GRPC_HOST="" \
    TH2_VERIFIER_GRPC_PORT="" \
    TH2_EVENT_STORAGE_GRPC_HOST="" \
    TH2_EVENT_STORAGE_GRPC_PORT="" \
#FIXME: Act should resolve queue information from session info which passed by caller (script)
    TH2_FIX_CONNECTIVITY_SEND_MQ="" \
#FIXME: Act should resolve queue information from session info which passed by caller (script)
    TH2_FIX_CONNECTIVITY_IN_MQ=""
WORKDIR /home
COPY ./ .
ENTRYPOINT ["/home/act-service/bin/act-service", "/home/act-service/etc/config.yml"]