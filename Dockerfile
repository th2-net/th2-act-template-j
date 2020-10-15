FROM gradle:6.6-jdk11 AS build
ARG release_version
ARG artifactory_user
ARG artifactory_password
ARG artifactory_deploy_repo_key
ARG artifactory_url

COPY ./ .
RUN gradle --no-daemon clean build dockerPrepare artifactoryPublish \
     -Prelease_version=${release_version} \
     -Partifactory_user=${artifactory_user} \
     -Partifactory_password=${artifactory_password} \
     -Partifactory_deploy_repo_key=${artifactory_deploy_repo_key} \
     -Partifactory_url=${artifactory_url}

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
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "/home/service/etc/config.yml"]