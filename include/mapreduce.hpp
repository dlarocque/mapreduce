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
#include <chrono>
#include <thread>
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
using coordinator::CompleteRequest;
using coordinator::CompleteReply;

enum TaskType {
    MAP,
    REDUCE
};

enum TaskState {
    IDLE,
    IN_PROGRESS,
    COMPLETE
};

struct Task {
    TaskState state;
    std::string worker_id; // For non-idle tasks
    std::string output_filename;
};

struct MapTask : public Task {
    std::string input_filename;
};

struct ReduceTask : public Task {
    std::vector<std::string> input_filenames;
};

void printMapTask(const MapTask& task) {
    std::cout << "MapTask: " << std::endl;
    std::cout << "  state: " << task.state << std::endl;
    std::cout << "  worker_id: " << task.worker_id << std::endl;
    std::cout << "  input_filename: " << task.input_filename << std::endl;
    std::cout << "  output_filename: " << task.output_filename << std::endl;
}

struct JobState {
    size_t num_mappers;
    size_t num_reducers;
    size_t segment_size;
    size_t num_segments;
    size_t num_idle_map_tasks;
    size_t num_idle_reduce_tasks;
    size_t num_in_progress_map_tasks = 0;
    size_t num_in_progress_reduce_tasks = 0;
    size_t num_completed_map_tasks = 0;
    size_t num_completed_reduce_tasks = 0;
    bool finished = false;

    std::vector<MapTask> map_tasks;
    std::vector<ReduceTask> reduce_tasks;
};

class MapReduceServiceImpl final : public Coordinator::Service {
public: 
    explicit MapReduceServiceImpl(std::shared_ptr<JobState> state) : state(std::move(state)) { }
    
    Status Assign(ServerContext* context, const AssignRequest* request, AssignReply* reply) override {
        // TODO: Add more error handling and validation
        std::cout << "Received AssignRequest from worker: " << request->worker_id() << std::endl;
        std::cout << "Number of idle map tasks: " << this->state->num_idle_map_tasks << std::endl;
        std::cout << "Number of idle reduce tasks: " << this->state->num_idle_reduce_tasks << std::endl;
        std::cout << "Number of in progress map tasks: " << this->state->num_in_progress_map_tasks << std::endl;
        std::cout << "Number of in progress reduce tasks: " << this->state->num_in_progress_reduce_tasks << std::endl;
        std::cout << "Number of completed map tasks: " << this->state->num_completed_map_tasks << std::endl;
        std::cout << "Number of completed reduce tasks: " << this->state->num_completed_reduce_tasks << std::endl;

        if (request->worker_id().empty()) {
            std::cerr << "error: worker ID is empty" << std::endl;
            return Status::CANCELLED;
        }
        
        const bool map_phase = this->state->num_completed_map_tasks < this->state->map_tasks.size();

        // Since a worker is sending an Assign RPC, we can assume that it is idle.
        // In this simple implementation, we only assign reduce tasks once all of the reduce
        // tasks have completed.
        if (map_phase && this->state->num_idle_map_tasks > 0) {
            // Search for an idle map task
            std::cout << "TASKS:" << std::endl;
            for (auto& task : this->state->map_tasks) {
                printMapTask(task);
                if (task.state == TaskState::IDLE) {
                    reply->set_taskname("map");
                    reply->add_input_filename(task.input_filename);
                    reply->set_output_filename(task.output_filename);

                    task.state = TaskState::IN_PROGRESS;
                    task.worker_id = request->worker_id();
                    this->state->num_idle_map_tasks--;
                    this->state->num_in_progress_map_tasks++;
                    
                    std::cout << "Assigned map task to worker: " << request->worker_id() << std::endl;
                    printMapTask(task);
                    return Status::OK;
                }
            }


        } else if (!map_phase && this->state->num_idle_reduce_tasks > 0) {
            // Search for an idle reduce task to assign
            ReduceTask idle_reduce_task;
            for (auto& task: this->state->reduce_tasks) {
                if (task.state == TaskState::IDLE) {
                    reply->set_taskname("reduce");
                    for (const auto& input_filename : task.input_filenames) {
                        reply->add_input_filename(input_filename);
                    }
                    reply->set_output_filename(task.output_filename);

                    task.state = TaskState::IN_PROGRESS;
                    task.worker_id = request->worker_id();
                    this->state->num_idle_reduce_tasks--;

                    std::cout << "Assigned reduce task to worker: " << request->worker_id() << std::endl;
                    return Status::OK;
                }
            }
            
        } else {
            // There are no idle tasks, so we can't assign any more to the worker
            // In this case, the worker should wait, and then send another Assign RPC
            std::cout << "No more tasks to assign to worker: " << request->worker_id() << std::endl;
            return Status::CANCELLED;
        }

        return Status::OK;
    }
    
    Status Complete(ServerContext* context, const CompleteRequest* request, CompleteReply* reply) override {
        // TODO: Add more error handling and validation
        if (request->worker_id().empty()) {
            std::cerr << "error: worker ID is empty" << std::endl;
            return Status::CANCELLED;
        }

        std::cout << "Received CompleteRequest from worker: " << request->worker_id() << std::endl;
        
        if (request->taskname() == "map") {
            // Update the reduce task to be complete
            for (auto& task : this->state->map_tasks) {
                if (task.worker_id == request->worker_id()) {
                    task.state = TaskState::COMPLETE;
                    this->state->num_completed_map_tasks++;
                    this->state->num_in_progress_map_tasks--;
                    std::cout << "Map task completed by worker: " << request->worker_id() << std::endl;
                    return Status::OK;
                }
            }
        } else if (request->taskname() == "reduce") {
            // Update the reduce task to be complete
            for (auto& task : this->state->reduce_tasks) {
                if (task.worker_id == request->worker_id()) {
                    task.state = TaskState::COMPLETE;
                    this->state->num_completed_reduce_tasks++;
                    this->state->num_in_progress_reduce_tasks--;
                    std::cout << "Reduce task completed by worker: " << request->worker_id() << std::endl;
                }
            }

            std::cout << "Number of completed reduce tasks: " << this->state->num_completed_reduce_tasks << std::endl;
            std::cout << "Number of reduce tasks: " << this->state->reduce_tasks.size() << std::endl;

            if (this->state->num_completed_reduce_tasks == this->state->reduce_tasks.size()) {
                std::cout << "All reduce tasks have completed" << std::endl;
                std::cout << "MapReduce job has completed" << std::endl;

                this->state->finished = true;

            }
            
            return Status::OK;
        } else {
            std::cerr << "error: unknown task name" << std::endl;
            return Status::CANCELLED;
        }
        
        return Status::OK;
    }

    private:
    std::shared_ptr<JobState> state;
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
        size_t num_assigned_mappers = 0;
        size_t num_assigned_reducers = 0;
        
        std::vector<Task> tasks;

        Mapper* mapper;
        Reducer* reducer;

        void execute() {
            std::cout << "executing mapreduce job" << std::endl;
            std::cout << "input directory: " << this->input_dir_name << std::endl;
            std::cout << "output filename: " << this->output_filename << std::endl;
            std::cout << "number of mappers: " << this->num_mappers << std::endl;
            std::cout << "number of reducers: " << this->num_reducers << std::endl;
            std::cout << "max segment size: " << this->max_segment_size << std::endl;

            std::vector<std::string> segments = std::move(inputSegments());

            std::cout << "number of segments: " << segments.size() << std::endl;
            std::cout << "segment sizes (bytes): ";
            for (const auto& segment : segments) {
                std::cout << segment.size() << " ";
            }
            std::cout << std::endl;

            // Write all segments to disk in a temporary directory
            std::filesystem::path segments_dir("segments");
            std::filesystem::create_directory(segments_dir);
            for (size_t i = 0; i < segments.size(); i++) {
                std::ofstream segment_file;
                segment_file.open("segments/segment_" + std::to_string(i));
                segment_file << segments[i];
                segment_file.close();
            }

            // Initializate job state
            std::shared_ptr<JobState> state = std::make_shared<JobState>();
            state->num_mappers = this->num_mappers;
            state->num_reducers = this->num_reducers;
            state->segment_size = this->max_segment_size;
            state->num_segments = segments.size();
            state->num_idle_map_tasks = segments.size(); // Should we let the user set this?
            state->num_idle_reduce_tasks = this->num_reducers;
            state->finished = false;
            
            // Initialize map tasks
            std::cout << "Initializing map tasks" << std::endl;
            std::vector<std::string> segment_filenames;
            for (size_t i = 0; i < segments.size(); i++) {
                MapTask map_task;
                map_task.state = TaskState::IDLE;
                map_task.input_filename = "segments/segment_" + std::to_string(i);
                map_task.output_filename = "mr-int-" + std::to_string(i);
                state->map_tasks.push_back(map_task);
                segment_filenames.push_back(map_task.output_filename);
                printMapTask(map_task);
            }
            
            size_t segments_per_reducer = (segments.size() + 1) / this->num_reducers;
            std::cout << "segments_per_reducer: " << segments_per_reducer << std::endl;
            size_t j = 0;
            for (size_t i = 0; i < this->num_reducers; i++) {
                std::vector<std::string> input_filenames;
                while (j < segments.size() && input_filenames.size() < segments_per_reducer) {
                    input_filenames.push_back(segment_filenames[j]);
                    j++;
                }
                
                // Print all the input filenames for the reduce tasks
                std::cout << "input_filenames for reduce task " << i << ": ";
                for (const auto& filename : input_filenames) {
                    std::cout << filename << " ";
                }
                std::cout << std::endl;

                ReduceTask reduce_task;
                reduce_task.state = TaskState::IDLE;
                reduce_task.input_filenames = input_filenames;
                reduce_task.output_filename = "mr-out-" + std::to_string(i);
                state->reduce_tasks.push_back(reduce_task);
            }
            
            // Start the RPC server
            std::string server_address = "0.0.0.0:8995"; // FIXME: Don't make this hard-coded
            MapReduceServiceImpl service(state);
            ServerBuilder builder;
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
            builder.RegisterService(&service);
            std::cout << "registered service" << std::endl;
            std::unique_ptr<Server> server(builder.BuildAndStart());
            std::cout << "Server listening on " << server_address << std::endl;

            // Create a seperate thread to monitor the job completion status
            std::thread job_monitor_thread([state, &server] {
                while (true) {
                    if (state->finished) {
                        server->Shutdown();
                        std::cout << "Server shutdown" << std::endl;
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            });

            server->Wait();
            job_monitor_thread.join();

            // Clean up the temporary directory
            std::cout << "Cleaning up temporary directory" << std::endl;
            std::filesystem::remove_all(segments_dir);
        }
        
        std::vector<std::string> inputSegments() {
            // Read input files, and split each of them into segments, and write them to the local disk
            std::vector<std::string> segments;
            for (const auto& entry : std::filesystem::directory_iterator(this->input_dir_name)) {
                const auto& path = entry.path();
                if (std::filesystem::is_regular_file(path)) {
                    std::ifstream file(path);
                    std::string buffer;
                    std::string line;
                    buffer.reserve(this->max_segment_size);
                    while (std::getline(file, line)) {
                        // If a single line is greater than the maximum segment size,
                        // we cut the first 16MB of the line and emit it as a segment
                        // repeatedly until the entire line is emitted
                        if (line.size() > this->max_segment_size) {
                            size_t start = 0;
                            while (start < line.size()) {
                                segments.push_back(line.substr(start, this->max_segment_size));
                                start += this->max_segment_size;
                            }
                        } else if (buffer.size() + line.size() > this->max_segment_size) {
                            // If the buffer is full, then we emit the segment and clear the buffer
                            // to make space for the current line
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
            
            return segments;
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
}

#endif //MAPREDUCE_MAPREDUCE_HPP

