# rest2grpc

Attempt to migrate REST system to gRPC :
o Download the database/tables schema/structure from DBMS (SQL/NoSQL)
o Use Python and Pandas to Generate Protobuf files,
o Use Python and protoc to compile Proto files to C++,
o Generate GRPC Server and GRPC client,
o Use C++ Boost Beast to link GRPC Server to REST Server, and GRPC Client to REST Client.
o REST Client will connect to Boost Beast API in the GRPC client locally on client pc
o Upon client request the Boost Beast API call the GRPC Client corresponding function  
o GRPC Client will connect/send the request to the GRPC server remotly
o GRPC server will connect/route the request to the REST server locally on server pc
o and vice versa........
