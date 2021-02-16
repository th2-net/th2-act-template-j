FROM gradle:6.6-jdk11 AS build
ARG release_version
ARG bintray_user
ARG bintray_key
ARG nexus_url
ARG nexus_user
ARG nexus_password
ARG vcs_url

COPY ./ .
RUN gradle --no-daemon clean build dockerPrepare \
    -Pbintray_user=${bintray_user} \
    -Pbintray_key=${bintray_key} \
    -Pnexus_url=${Pnexus_url} \
    -Pnexus_user=${nexus_user} \
    -Pnexus_password=${Pnexus_password} \
    -Pvcs_url=${vcs_url}

FROM adoptopenjdk/openjdk11:alpine
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
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "/home/service/etc/config.yml"]