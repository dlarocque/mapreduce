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
using coordinator::CompleteRequest;
using coordinator::CompleteReply;

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
    
    CompleteReply Complete(std::string worker_id, std::string taskname, std::string output_filename) {
        CompleteRequest request;
        CompleteReply reply;
        ClientContext context;
        
        request.set_worker_id(worker_id);
        request.set_taskname(taskname);
        request.set_output_filename(output_filename);
        
        Status status = stub_->Complete(&context, request, &reply);
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
void emit_intermediate(const char* key, const char* value) {
    intermediate.push_back({key, value});
}

std::vector<std::pair<std::string, std::string>> intermediate_final;
void emit_final(const char* key, const char* value) {
    intermediate_final.push_back({key, value});
}

// Map and reduce functions
typedef void (*map_func_t)(const char* input, void (*emit) (const char*, const char*));
typedef void (*reduce_func_t)(const char* key, const char* const* values, int values_len, void (*emit) (const char*, const char*));
  
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
    
    // TODO: Check if the reply is empty
    std::string input;
    for (const auto& filename : reply.input_filename()) {
        std::stringstream buffer;
        std::ifstream input_file(filename);
        if (!input_file.is_open()) {
            std::cerr << "Failed to open input file: " << filename << std::endl;
            return 1;
        }
        buffer << input_file.rdbuf();
        input.append(buffer.str());
        input_file.close();
    }
    
    // Call the map or reduce function
    std::string taskname = reply.taskname();
    if (taskname == "map") {
        // Sort the input by key
        // 

        // Intermediate key-value store
        map_func(input.c_str(), emit_intermediate);
        
        // Write the intermediate key-value pairs to the output file
        std::ofstream output_file(reply.output_filename());
        if (!output_file.is_open()) {
            std::cerr << "Failed to open output file: " << reply.output_filename() << std::endl;
            return 1;
        }

        for (const auto& kv : intermediate) {
            output_file << kv.first << "\t" << kv.second << std::endl;
        }
        
        std::cout << "Wrote " << intermediate.size() << " key-value pairs to " << reply.output_filename() << std::endl;

        output_file.close();
        
        // Send the intermediate file name to the coordinator to notify them that we're done
        // TODO: Implement Complete RPC
    } else if (taskname == "reduce") {
        // Convert the input string to a vector of key-value pairs
        size_t start = 0; 
        while (start < input.size()) {
            size_t end = input.find('\n', start);
            if (end == std::string::npos) {
                end = input.size();
            }
            std::string line = input.substr(start, end - start);
            size_t tab = line.find('\t');
            std::string key = line.substr(0, tab);
            std::string value = line.substr(tab + 1);
            intermediate.push_back({key, value});
            start = end + 1;
        }

        // Sort the intermediate key-value pairs
        std::sort(intermediate.begin(), intermediate.end()); 
        
        
        // Aggregate values and send them to the reducer function
        std::string current_key = intermediate[0].first;
        std::vector<const char*> values; // Must be a vector of const char* because that's what the reduce function expects
        for (const auto& kv : intermediate) {
            if (kv.first != current_key) {
                const char* const* values_data = values.data(); // Implicit conversion from vector<const char*> to const char* const*
                reduce_func(current_key.c_str(), values_data, values.size(), emit_final);
                current_key = kv.first;
                values.clear();
            }
            values.push_back(kv.second.c_str());
        }
        
        // Write the final key-value pairs to the output file
        std::ofstream output_file(reply.output_filename());
        if (!output_file.is_open()) {
            std::cerr << "Failed to open output file: " << reply.output_filename() << std::endl;
            return 1;
        }

        std::cout << "Writing final key-value pairs to " << reply.output_filename() << std::endl;
        for (const auto& kv : intermediate_final) {
            output_file << kv.first << "\t" << kv.second << std::endl;
        }

        output_file.close();
    } else {
        std::cerr << "Unknown task: " << reply.taskname() << std::endl;
        return 1;
    }
    
    // Send Complete RPC to the coordinator
    std::cout << "Sending Complete RPC to the coordinator" << std::endl;
    CompleteReply complete_reply = client.Complete(worker_id, reply.taskname(), reply.output_filename());
    
    std::cout << "Complete RPC returned " << std::endl;
    return 0;
}
