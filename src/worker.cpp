#include <iostream>
#include <memory>
#include <dlfcn.h>
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
    
    AssignReply Assign(std::string worker_id) {
        AssignRequest request;
        AssignReply reply;
        ClientContext context;
        
        request.set_worker_id(worker_id);
        
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

// Map and reduce functions
typedef void (*map_func_t)(const char*, void(*)(const char*, const char*));
typedef void (*reduce_func_t)(const char*, const char* const*, int, void(*)(const char*, const char*));
  
int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <worker_id> <map_reduce_functions.so>" << std::endl;
        return 1;
    }

    std::string so_filename = argv[2];
    std::cout << "Loading shared object: " << so_filename << std::endl;
    
    void* handle = dlopen(so_filename.c_str(), RTLD_LAZY);
    if (!handle) {
        std::cerr << "dlopen() failed: " << dlerror() << std::endl;
        return 1;
    }

    map_func_t map_func = (map_func_t)dlsym(handle, "map");
    if (!map_func) {
        std::cerr << "dlsym() failed for map: " << dlerror() << std::endl;
        return 1;
    }

    reduce_func_t reduce_func = (reduce_func_t)dlsym(handle, "reduce");
    if (!reduce_func) {
        std::cerr << "dlsym() failed for reduce: " << dlerror() << std::endl;
        return 1;
    }

    std::cout << "Loaded map and reduce functions" << std::endl;

    std::string worker_id = argv[1];

    CoordinatorClient client(grpc::CreateChannel("0.0.0.0:8995", grpc::InsecureChannelCredentials()));
    AssignReply reply = client.Assign(worker_id);
    if (reply.taskname() != "" && reply.input_filename() != "" && reply.output_filename() != "") {
        std::cout << "Received task: " << reply.taskname() << std::endl;
        std::cout << "Input filename: " << reply.input_filename() << std::endl;
        std::cout << "Output filename: " << reply.output_filename() << std::endl;
    } else {
        std::cerr << "RPC error" << std::endl;
        return 1;
    }
    

    return 0;
}
