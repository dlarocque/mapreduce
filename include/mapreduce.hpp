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
#include <filesystem>
#include <algorithm>

namespace mapreduce {
    // FIXME: We can not be using shared global state in a concurrent program
    std::vector<std::pair<std::string, std::string>> intermediate;
    static std::ofstream output_file;

    class Mapper {
    public:
        virtual void map(const std::string&) = 0;
    };

    class Reducer {
    public:
        virtual void reduce(const std::string& key, std::vector<std::string> intermediate_values) = 0;
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

        // Sort the intermediate key-value pairs by key
        // FIXME: If the size of the intermediate data is too large, then we should use an external sort
        std::sort(intermediate.begin(), intermediate.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });

        if (intermediate.empty()) {
            std::cout << "no intermediate data to reduce" << std::endl;
            return;
        }

        // Collect all values for each key and send them to the reducer
        std::string curr_key = intermediate[0].first;
        std::vector<std::string> curr_intermediate_values;
        for (const auto& [key, value] : intermediate) {
            if (key != curr_key) {
                spec.reducer->reduce(curr_key, curr_intermediate_values);
                curr_intermediate_values.clear();
                curr_key = key;
            }

            curr_intermediate_values.push_back(value);
        }

        if (!curr_intermediate_values.empty())
            spec.reducer->reduce(curr_key, curr_intermediate_values);

        output_file.close();
        std::cout << "mapreduce job complete" << std::endl;
    }

    void emit_intermediate(const std::string& key, const std::string& value) {
        intermediate.emplace_back(key, value);
    }

    void emit(const std::string& key, const std::string& value) {
        // Write the key-value pair to the output file
        output_file << key << " " << value << std::endl;
    }
}


#endif //MAPREDUCE_MAPREDUCE_HPP

