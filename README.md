# th2 act template (3.4.1)

## Overview

Act is a passive th2 component with parameterized functions which is implemented as part of the test logic. Script or other components can call these functions via gRPC.
Act can interact with conn (Connects), hands, check1s, other acts to execute its tasks. Information about the progress of the task is published to the estore th2 component via MQ pin. This th2 component type allows for often used logic from script into it and then share it between all th2 components.

This project is implemented gRPC API described in the [th2-grpc-act-template](https://github.com/th2-net/th2-grpc-act-template/blob/master/src/main/proto/th2_grpc_act_template/act_template.proto "act_template.proto")

Most of them consist of the next steps:
1. Gets a gRPC request with parameters.
1. Requests checkpoint from check1 via gRPC pin
1. Sends the passed business message to Connect via mq pin 
1. Waits the specific business message from Connect during specified timeout 
1. Returns responded business message with checkpoint

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
    - name: from_codec
      connection-type: mq
      attributes: [ "subscribe", "parsed", "oe" ]
    - name: to_conn1
      connection-type: mq
      attributes:
        - publish
        - parsed
      filters:
        - metadata:
            - field-name: session_alias
              expected-value: conn1_session_alias
              operation: EQUAL
    - name: to_conn2
      connection-type: mq
      attributes:
        - publish
        - parsed
      filters:
        - metadata:
            - field-name: session_alias
              expected-value: conn2_session_alias
              operation: EQUAL
```
## Descriptor gradle plugin

Also recommened to apply [th2-box-descriptor-generator plugin](https://github.com/th2-net/th2-box-descriptor-generator). It allows to generate gRPC desciptors and attach them to the docker image. They can be used in projects that implement gRPC interface or dynamically use it (like act-ui).


## Release Notes

### 3.4.1

+ Make gRPC response objects contain logs about the errors happened in gRPC methods
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
+ tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd, default configuration
+ update Cradle version. Introduce async API for storing events
