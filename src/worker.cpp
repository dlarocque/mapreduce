#include <iostream>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "coordinator.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using coordinator::Coordinator;
using coordinator::AssignRequest;
using coordinator::AssignReply;

class CoordinatorClient {
    public:
    CoordinatorClient(std::shared_ptr<Channel> channel) : stub_(Coordinator::NewStub(channel)) {
        // Empty
    }
    
    AssignReply Assign() {
        AssignRequest request;
        AssignReply reply;
        ClientContext context;
        
        request.set_worker_id("1");
        
        Status status = stub_->Assign(&context, request, &reply);
        if (status.ok()) {
            return reply;
        } else {
            std::cout << status.error_code() << ": " << status.error_message() << std::endl;
            return {}; // FIXME: Should we throw an exception here?
        }
    }
    
    private:
    std::unique_ptr<Coordinator::Stub> stub_;
};
  
int main() {
    // Start the RPC server
    // Listen for incoming requests
    
    CoordinatorClient client(grpc::CreateChannel("0.0.0.0:8995", grpc::InsecureChannelCredentials()));
    AssignReply reply = client.Assign();
    // Check if the reply is non-empty
    if (reply.taskname() != "" && reply.input_filename() != "" && reply.output_filename() != "") {
        std::cout << "Received task: " << reply.taskname() << std::endl;
        std::cout << "Input filename: " << reply.input_filename() << std::endl;
        std::cout << "Output filename: " << reply.output_filename() << std::endl;
    } else {
        std::cout << "RPC error" << std::endl;
    }

    return 0;
}
