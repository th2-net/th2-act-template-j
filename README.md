# th2 act template (5.0.0)

## Overview

Act is a passive th2 component with parameterized functions which is implemented as part of the test logic. Script or
other components can call these functions via gRPC. Act can interact with codec , hands, check1s, other acts to execute
its tasks. Information about the progress of the task is published to the estore th2 component via MQ pin. This th2
component type allows frequently used script logic into it and then share it between all th2 components.

This project is implemented gRPC API described in
the [th2-grpc-act-template](https://github.com/th2-net/th2-grpc-act-template/blob/master/src/main/proto/th2_grpc_act_template/act_template.proto "act_template.proto")

Most of them consists of the next steps:

1. Gets a gRPC request with parameters.
2. Requests checkpoint from check1 via gRPC pin
3. Sends the passed business message to Codec via mq pin
4. Waits the specific business message from Codec during specified timeout
5. Returns responded business message with checkpoint

![picture](scheme.png)

## Custom resources for infra-mgr

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: act
spec:
  type: th2-act
  pins:
    - name: grpc_server
      connection-type: grpc
    - name: grpc_client_to_check1
      connection-type: grpc
    - name: from_codec_transport
      connection-type: mq
      attributes:
        - subscribe
        - transport-group
        - oe
    - name: to_codec1_transport
      connection-type: mq
      attributes:
        - publish
        - transport-group
      filters:
        - metadata:
            - field-name: session_alias
              expected-value: conn1_session_alias
              operation: EQUAL
    - name: to_codec2_transport
      connection-type: mq
      attributes:
        - publish
        - transport-group
      filters:
        - metadata:
            - field-name: session_alias
              expected-value: conn2_session_alias
              operation: EQUAL
```

## Descriptor gradle plugin

Also we recommend to
apply [th2-box-descriptor-generator plugin](https://github.com/th2-net/th2-box-descriptor-generator). It allows
generating a th2 descriptor. CI should publish the project's docker image with the descriptor content as the value of
the `protobuf-description-base64` label. Such descriptors can be used to interact with a box-raised gRPC server.

## Release Notes

### 5.0.0

+ Migrated to th2 transport protocol
+ Updated bom to `4.5.0`
+ Updated kotlin to `1.8.22`
+ Updated common to `5.4.0`
+ Updated grpc-act-template to `4.1.0`
+ Updated grpc-check1 to `4.3.0`
+ Added common-utils `2.2.0`

### 4.0.1

+ 3.9.0 merged

### 4.0.0

+ Update `kotlin.jvm` version from `1.3.72` to `1.5.30`
+ Migration to books/pages cradle 4.0.0
    + Update `th2-common` version from `3.26.4` to `4.0.0`
    + Update `th2-grpc-act-template` version from `3.4.0` to `4.0.0`
    + Update `th2-grpc-check1` version from `3.4.2` to `4.0.0`

### 3.9.0

+ th2-common to `3.44.1`
+ th2-bom to `4.2.0`
+ th2-grpc-check to `3.8.0`
+ grpc-check1 updated to `3.8.0`
+ updated gradle to 7.6

### 3.8.0

+ Update `th2-grpc-act-template` version from `3.9.0` to `3.10.0`
+ Implement `placeOrderCancelRequest` and `placeOrderCancelReplaceRequest` methods

### 3.7.0

+ Update `th2-common` version from `3.26.4` to `3.44.0`
+ Update `th2-grpc-act-template` version from `3.4.0` to `3.9.0`
+ Update `th2-grpc-check1` version from `3.4.2` to `3.6.0`
+ Update `kotlin` version from `1.3.72` to `1.6.21`

### 3.5.0

+ Update `th2-common` version from `3.16.5` to `3.26.4`
+ Update `th2-grpc-act-template` version from `3.2.0` to `3.4.0`
+ Update `th2-grpc-check1` version from `3.2.0` to `3.4.2`

### 3.4.1

+ Make gRPC response objects containing logs about the errors that happened in gRPC methods
+ Fixed configuration for gRPC server.
    + Added the property workers, which changes the count of gRPC server's threads

### 3.3.1

+ add creation of error event in case the `sendMessage` call did not success

### 3.3.0

+ migration to Sonatype

### 3.2.1

+ removed gRPC event loop handling
+ fixed dictionary reading

### 3.2.0

+ reads dictionaries from the /var/th2/config/dictionary folder.
+ uses mq_router, grpc_router, cradle_manager optional JSON configs from the /var/th2/config folder
+ tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd,
  default configuration
+ update Cradle version. Introduce async API for storing events
