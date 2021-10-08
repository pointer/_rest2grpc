# rest2grpc

Experiment to migrate REST system to gRPC :

- Download the database/tables schema/structure from DBMS (SQL/NoSQL)
- Use of Python and Pandas to Generate Protobuf files,
- Use of Python and protoc to compile Proto files to C++,
- Generate GRPC Server and GRPC client,
- Use of gRPC Ecosystem gRPC-Gateway, OpenAPI and protoc-gen-swagger
- Use C++ Boost Beast to link GRPC Server to REST Server, and GRPC Client to REST Client.
- REST Client will connect to Boost Beast API in the GRPC client locally on client pc
- Upon client request the Boost Beast API call the GRPC Client corresponding function
- GRPC Client will connect/send the request to the GRPC server remotly
- GRPC server will connect/route the request to the REST server locally on server pc
