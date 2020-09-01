# Overview
Act component is an active component that sends a message to Connect by MQ, waits for a response message for a specified timeout, and returns the received message to the caller. If no response message is received, Act returns a gRPC error response to the script. Communication with Connect  takes place via a message queue, communication with the script - via gRPC. 

Act sends events about the status of the internal steps to the "Event store" component during request processing. These events are written to the root event associated with this request to Act and, as a result, get into the report on the script run.

![picture](scheme.png)

# Configuration

## Environment variables
- RABBITMQ_PASS=some_pass (***required***)
- RABBITMQ_HOST=some-host-name-or-ip (***required***)
- RABBITMQ_PORT=7777 (***required***)
- RABBITMQ_VHOST=someVhost (***required***)
- RABBITMQ_USER=some_user (***required***)
- GRPC_PORT=7878 (***required***)
- TH2_CONNECTIVITY_QUEUE_NAMES={"fix_client": {"exchangeName":"demo_exchange", "toSendQueueName":"client_to_send", "toSendRawQueueName":"client_to_send_raw", "inQueueName": "fix_codec_out_client", "inRawQueueName": "client_in_raw", "outQueueName": "client_out" , "outRawQueueName": "client_out_raw"  }, "fix_server": {"exchangeName":"demo_exchange", "toSendQueueName":"server_to_send", "toSendRawQueueName":"server_to_send_raw", "inQueueName": "fix_codec_out_server", "inRawQueueName": "server_in_raw", "outQueueName": "server_out" , "outRawQueueName": "server_out_raw"  }} (***required***)
- TH2_VERIFIER_GRPC_HOST=verify-host-name-or-ip; (***required***)
- TH2_VERIFIER_GRPC_PORT=9999; (***required***)
- TH2_EVENT_STORAGE_GRPC_HOST=event-store-host-name-or-ip; (***required***)
- TH2_EVENT_STORAGE_GRPC_PORT=9999; (***required***)