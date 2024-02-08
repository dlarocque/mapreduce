//
// Created by Daniel La Rocque on 2024-01-19.
//

#pragma once

#ifndef MAPREDUCE_MAPREDUCE_HPP
#define MAPREDUCE_MAPREDUCE_HPP

#include <memory>
#include <utility>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <filesystem>
#include <algorithm>
#include <grpcpp/grpcpp.h>
#include "coordinator.grpc.pb.h"

using grpc::Server;
using grpc::Status;
using grpc::ServerContext;
using grpc::ServerBuilder;
using coordinator::Coordinator;
using coordinator::AssignRequest;
using coordinator::AssignReply;

class MapReduceServiceImpl final : public Coordinator::Service {
public: 
    explicit MapReduceServiceImpl(size_t num_mappers, size_t num_reducers, size_t num_segments) : num_mappers(num_mappers), num_reducers(num_reducers), num_assigned_mappers(0), num_assigned_reducers(0), num_segments(num_segments) {
        std::cout << "MapReduceServiceImpl created" << std::endl;
        std::cout << "Number of mappers: " << num_mappers << std::endl;
        std::cout << "Number of reducers: " << num_reducers << std::endl;
    }
    
    Status Assign(ServerContext* context, const AssignRequest* request, AssignReply* reply) override {
        std::cout << "Received AssignRequest from worker: " << request->worker_id() << std::endl;

        if (request->worker_id().empty()) {
            std::cerr << "error: worker ID is empty" << std::endl;
            return Status::CANCELLED;
        }

        // Assign map tasks first
        if (this->num_assigned_mappers < this->num_mappers) {
            std::string taskname = "map";
            // FIXME: We should be using a more robust directory access method
            // in case the worker is running in a different directory or machine
            std::string input_filename = "segments/segment_" + std::to_string(this->num_assigned_mappers);
            std::string output_filename = "mr-int-" + std::to_string(this->num_assigned_mappers);
            reply->set_taskname(taskname);
            reply->set_input_filename(input_filename);
            reply->set_output_filename(output_filename);
            this->num_assigned_mappers++;
            std::cout << "Assigned map task to worker: " << request->worker_id() << std::endl;
            std::cout << "Remaining map tasks: " << this->num_mappers - this->num_assigned_mappers << std::endl;
            return Status::OK;
        } else if (this->num_assigned_reducers < this->num_reducers) {
            std::string taskname = "reduce";
            std::string input_filename = "input";
            std::string output_filename = "output";
            reply->set_taskname(taskname);
            reply->set_input_filename(input_filename);
            reply->set_output_filename(output_filename);
            this->num_assigned_reducers++;
            std::cout << "Assigned reduce task to worker: " << request->worker_id() << std::endl;
            std::cout << "Remaining reduce tasks: " << this->num_reducers - this->num_assigned_reducers << std::endl;
            return Status::OK;
        } else {
            std::cout << "No more tasks to assign to worker: " << request->worker_id() << std::endl;
            return Status::CANCELLED;
        }

        return Status::OK;
    }

    private:
    size_t num_mappers;
    size_t num_reducers;
    size_t num_assigned_mappers = 0;
    size_t num_assigned_reducers = 0;
    size_t num_segments;
};

namespace mapreduce {
    // Forward declarations
    class Mapper;
    class Reducer;

    class MapReduceSpec {
    public:
        std::string input_dir_name;
        std::string output_filename;
        std::ofstream output_file;
        size_t num_mappers;
        size_t num_reducers;
        size_t max_segment_size;
        size_t num_segments;
        size_t num_assigned_mappers;
        size_t num_assigned_reducers;

        Mapper* mapper;
        Reducer* reducer;

        // Synchronization primitives
        std::mutex intermediate_mutex;
        std::mutex intermediate_final_mutex;

        std::vector<std::pair<std::string, std::string>> intermediate;
        std::unordered_map<std::string, std::string> intermediate_final;
        
        void execute() {
            std::cout << "executing mapreduce job" << std::endl;
            std::cout << "input directory: " << this->input_dir_name << std::endl;
            std::cout << "output filename: " << this->output_filename << std::endl;
            std::cout << "number of mappers: " << this->num_mappers << std::endl;
            std::cout << "number of reducers: " << this->num_reducers << std::endl;
            std::cout << "max segment size: " << this->max_segment_size << std::endl;

            // Read input files, and split each of them into segments of at most 16MB
            std::vector<std::string> segments; // Each segment is at most 16MB

            for (const auto& entry : std::filesystem::directory_iterator(this->input_dir_name)) {
                const auto& path = entry.path();
                if (std::filesystem::is_regular_file(path)) {
                    std::ifstream file(path);
                    std::string buffer;
                    std::string line;
                    buffer.reserve(this->max_segment_size);
                    while (std::getline(file, line)) {
                        // FIXME: If a single line exceeds the maximum segment size, then we should split it into multiple segments
                        if (buffer.size() + line.size() > this->max_segment_size) {
                            segments.push_back(buffer);
                            buffer.clear();
                        }

                        if (!line.empty())
                            buffer += line + "\n";
                    }

                    segments.push_back(buffer);
                } else {
                    std::cerr << "error: " << path << " is not a regular file, and will be ignored" << std::endl;
                }
            }

            std::cout << "number of segments: " << segments.size() << std::endl;
            std::cout << "segment sizes (bytes): ";
            for (const auto& segment : segments) {
                std::cout << segment.size() << " ";
            }
            std::cout << std::endl;

            this->num_segments = segments.size();

            // Write all segments to disk in a temporary directory
            std::filesystem::path segments_dir("segments");
            std::filesystem::create_directory(segments_dir);
            for (size_t i = 0; i < segments.size(); i++) {
                std::ofstream segment_file;
                segment_file.open("segments/segment_" + std::to_string(i));
                segment_file << segments[i];
                segment_file.close();
            }

            // Start the RPC server
            std::string server_address = "0.0.0.0:8995";
            MapReduceServiceImpl service(this->num_mappers, this->num_reducers, segments.size());
            ServerBuilder builder;
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
            builder.RegisterService(&service);
            std::cout << "registered service" << std::endl;
            std::unique_ptr<Server> server(builder.BuildAndStart());
            std::cout << "Server listening on " << server_address << std::endl;

            server->Wait();

            // Clean up the temporary directory
            std::filesystem::remove_all(segments_dir);

    /*
     *COORDINATOR
            // Start map tasks
            for (const auto& segment : segments)
                this->mapper->map(mr, segment);

            // Sort the intermediate key-value pairs by key
            // FIXME: If the size of the intermediate data is too large, then we should use an external sort
            std::sort(mr.intermediate.begin(), mr.intermediate.end(), [](const auto& a, const auto& b) {
                return a.first < b.first;
            });

            if (mr.intermediate.empty()) {
                std::cout << "no intermediate data to reduce" << std::endl;
                return;
            }

            // Collect all values for each key and send them to the reducer
            std::string curr_key = mr.intermediate[0].first;
            std::vector<std::string> curr_intermediate_values;
            for (const auto& [key, value] : mr.intermediate) {
                if (key != curr_key) {
                    this->reducer->reduce(mr, curr_key, curr_intermediate_values);
                    curr_intermediate_values.clear();
                    curr_key = key;
                }

                curr_intermediate_values.push_back(value);
            }

            if (!curr_intermediate_values.empty())
                this->reducer->reduce(mr, curr_key, curr_intermediate_values);


            // Write the results to the output file
            std::ofstream output_file;
            output_file.open(this->output_filename);
            output_file.close();
            std::string output;
            for (const auto& [key, value] : mr.intermediate_final) {
                output += key + " " + value + "\n";
            }
            output_file << output;

            std::cout << "mapreduce job complete" << std::endl;
            */
        }
    };

    class Mapper {
    public:
        virtual void map(MapReduceSpec& mr, const std::string&) = 0;
    };

    class Reducer {
    public:
        virtual void reduce(MapReduceSpec& mr, const std::string& key, std::vector<std::string> intermediate_values) = 0;
    };


    void emit_intermediate(MapReduceSpec& mr, const std::string& key, const std::string& value) {
        mr.intermediate.emplace_back(key, value);
    }

    void emit(MapReduceSpec& mr, const std::string& key, const std::string& value) {
        mr.intermediate_final[key] = value;
    }
}

#endif //MAPREDUCE_MAPREDUCE_HPP

