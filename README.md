# broker

<p align="center">
  <a href="https://skillicons.dev">
    <img src="https://skillicons.dev/icons?i=go,postgres,kubernetes,docker,prometheus,cassandra" />
  </a>
</p>


The project is a distributed message broker system built using Golang, designed to provide reliable and consistent message delivery within a clustered environment. 
It incorporates various technologies and architectural components to ensure high availability, scalability, and robustness. 


## Project Description:

The message broker system is a critical infrastructure component for facilitating communication and coordination among distributed services and applications. 
It employs cutting-edge technologies and architectural choices to ensure reliability, scalability, and ease of management.


<img width="1228" alt="Screenshot 2023-10-03 at 5 20 25 PM" src="https://github.com/ajhexer/broker/assets/72503020/9bbee4a1-a58d-4a6e-ac70-60d63899011b">


## Key Features

- Consensus Algorithm with Raft:
The core of this message broker relies on the Raft consensus algorithm, ensuring that the index of logs remains consistent across all nodes in the cluster. This guarantees data integrity and high availability even in the face of network failures or node crashes.

- GRPC and Protocol Buffers (Protobuf):
The system utilizes GRPC and Protobuf for efficient and language-agnostic message transferring. This ensures fast and reliable communication between different components of the system, enhancing interoperability and performance.

- Monitoring with Prometheus:
For comprehensive monitoring and observability, Prometheus is integrated into the architecture. It enables real-time tracking of system metrics, facilitating proactive issue detection and resolution.

- Envoy for Rate Limiting:
Envoy is employed to implement rate limiting, ensuring fair usage of the message broker's resources and preventing abuse or overloading of the system.

- Kubernetes and Docker Deployment:
The message broker is containerized using Docker and orchestrated using Kubernetes. This deployment approach ensures easy scaling, load balancing, and management of the broker in a containerized environment.

- Persistent Storage with Cassandra and PostgreSQL:
To handle data persistence, the system uses both Cassandra and PostgreSQL databases. Cassandra offers high scalability and availability for large-scale data storage, while PostgreSQL is chosen for structured data needs, providing ACID compliance.

- Service Discovery and Membership Monitoring:
Service discovery and membership monitoring are facilitated through a combination of Serf and the Kubernetes API server. Serf, using the gossip protocol, discovers and tracks new nodes in the cluster, maintaining an up-to-date view of cluster membership.

## RPCs Description
- Publish Requst
```protobuf
message PublishRequest {
  string subject = 1;
  bytes body = 2;
  int32 expirationSeconds = 3;
}
```
- Fetch Request
```protobuf
message FetchRequest {
  string subject = 1;
  int32 id = 2;
}
```
- Subscribe Request
```protobuf
message SubscribeRequest {
  string subject = 1;
}
```
- RPC Service
```protobuf
service Broker {
  rpc Publish (PublishRequest) returns (PublishResponse);
  rpc Subscribe(SubscribeRequest) returns (stream MessageResponse);
  rpc Fetch(FetchRequest) returns (MessageResponse);
  rpc Broadcast(PublishRequest) returns (BroadcastResponse);
  rpc IncIdx(IncIdxRequest) returns (IncIdxResponse);
}
```
