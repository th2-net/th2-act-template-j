# Overview

Act is a passive th2 component with parameterised functions which is implemented as parts of the logic test. Script or other components can call these functions via gRPC.
Act can interact with conn (Connects), hands, check1s, other acts to execute its tasks. Information about the progress of the task is published to the estore th2 component via mq pin. This th2 component type allows for often used logic from script into it and then share it between all th2 components.

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
kind: Th2GenericBox
spec:
  type: th2-act
  pins:
    - name: grpc_server
      connection-type: grpc
    - name: grpc_client_to_check1
      connection-type: grpc
    - name: from_codec
      connection-type: mq
      attributes: [ "subscribe", "parsed", "first", "oe" ]
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
