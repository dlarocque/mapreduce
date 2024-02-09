#include <iostream>
#include <fstream>
#include <memory>
#include <algorithm>
#include <unordered_map>
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

// Emit function
// I don't like this global variable, but it's the only way to pass the emit function to the map function.
// The map_func and reduce_func can't take a std::function as an argument because they are called from dlsym, 
// and that seems to cause segmentation faults, probably something to do with the name mangling.
std::vector<std::pair<std::string, std::string>> intermediate;
void emit(const char* key, const char* value) {
    intermediate.push_back({key, value});
}

// Map and reduce functions
typedef void (*map_func_t)(const char* input, void (*emit) (const char*, const char*));
typedef void (*reduce_func_t)(const char* key, const char* const* values, int values_len, std::function<void(const char*, const char*)>);
  
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
    
    // Open the input file
    std::ifstream input_file(reply.input_filename());
    if (!input_file.is_open()) {
        std::cerr << "Failed to open input file: " << reply.input_filename() << std::endl;
        return 1;
    }
    
    // Read the input file into a buffer
    std::stringstream buffer;
    buffer << input_file.rdbuf();
    std::string input = buffer.str();
    input_file.close();
    
    // Call the map or reduce function
    if (reply.taskname() == "map") {
        // Intermediate key-value store
        map_func(input.c_str(), emit);
        
        // Write the intermediate key-value pairs to the output file
        std::ofstream output_file(reply.output_filename());
        if (!output_file.is_open()) {
            std::cerr << "Failed to open output file: " << reply.output_filename() << std::endl;
            return 1;
        }

        for (const auto& kv : intermediate) {
            output_file << kv.first << "\t" << kv.second << std::endl;
        }

        output_file.close();
    } else if (reply.taskname() == "reduce") {
        // TODO: Sort the intermediate key-value pairs

        // TODO: Aggregate values and send them to the reducer function
        reduce_func(input.c_str(), nullptr, 0, [](const char* key, const char* value) {
                // Write the final key-value pair to the output file
        });
    } else {
        std::cerr << "Unknown task: " << reply.taskname() << std::endl;
        return 1;
    }

    return 0;
}
