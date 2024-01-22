//
// Created by Daniel La Rocque on 2024-01-19.
//

#pragma once

#ifndef MAPREDUCE_MAPREDUCE_HPP
#define MAPREDUCE_MAPREDUCE_HPP

#include <utility>
#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include <fstream>
#include <unordered_map>

namespace mapreduce {
    // FIXME: We can not be using shared global state in a concurrent program
    static std::unordered_map<std::string, unsigned> intermediate;
    static std::ofstream output_file;

    class Mapper {
    public:
        virtual void map(const std::string&) = 0;
    };

    class Reducer {
    public:
        virtual void reduce(const std::unordered_map<std::string, unsigned>&) = 0;
    };

    struct MapReduceSpecification {
        std::string input_dir_name;
        std::string output_filename;
        std::ofstream output_file;
        size_t num_mappers;
        size_t num_reducers;
        size_t max_segment_size;

        Mapper* mapper;
        Reducer* reducer;
    };

    void execute(const MapReduceSpecification& spec) {
        std::cout << "executing mapreduce job" << std::endl;

        // Read input files, and split each of them into segments of at most 16MB
        std::vector<std::string> segments; // Each segment is at most 16MB

        for (const auto& entry : std::filesystem::directory_iterator(spec.input_dir_name)) {
            const auto& path = entry.path();
            if (std::filesystem::is_regular_file(path)) {
                std::ifstream file(path);
                std::string buffer;
                std::string line;
                buffer.reserve(spec.max_segment_size);
                while (std::getline(file, line)) {
                    // FIXME: If a single line exceeds the maximum segment size, then we should split it into multiple segments
                    if (buffer.size() + line.size() > spec.max_segment_size) {
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

        // Open the output file
        output_file.open(spec.output_filename);

        // Start map tasks
        for (const auto& segment : segments)
            spec.mapper->map(segment);

        // Start reduce tasks
        spec.reducer->reduce(intermediate);

        output_file.close();
        std::cout << "mapreduce job complete" << std::endl;
    }

    void emit_intermediate(const std::string& key, unsigned value) {
        if (intermediate.find(key) != intermediate.end())
            intermediate[key] += value;
        else
            intermediate[key] = value;
    }

    void emit(const std::string& key, unsigned value) {
        // Write the key-value pair to the output file
        output_file << key << " " << value << std::endl;
    }
}


#endif //MAPREDUCE_MAPREDUCE_HPP

