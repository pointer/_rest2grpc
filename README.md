# rest2grpc

Attempt to migrate REST system to gRPC :
o Download the database/tables schema/structure from DBMS (SQL/NoSQL)
o Use Python and Pandas to Generate Protobuf files,
o Use Python and protoc to compile Proto files to C++,
o Generate GRPC Server and GRPC client,
o Use C++ Boost Beast to link GRPC Server to REST Server, and GRPC Client to REST Client.
