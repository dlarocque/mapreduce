//
// Created by Daniel La Rocque on 2024-01-19.
//

#include "../include/mapreduce.hpp"

#include <iostream>
#include <vector>
#include <string>

class WordCountMapper : public mapreduce::Mapper {
    void map(mapreduce::MapReduce& mr, const std::string& input) override {
        const auto& n = input.size();
        std::string word;
        for (size_t i = 0; i < n; ++i) {
            // Skip past leading whitespace
            while (i < n && isspace(input[i]))
                ++i;

            size_t start = i;
            while (i < n && !isspace(input[i]))
                i++;

            if (start < i)
                mapreduce::emit_intermediate(mr, input.substr(start, i - start), "1");
        }
    }
};

class WordCountReducer : public mapreduce::Reducer {
    void reduce(mapreduce::MapReduce& mr, const std::string& key, const std::vector<std::string> intermediate_values) override {
        int value = 0;
        for (const auto& intermediate_value : intermediate_values) {
            value += std::stoi(intermediate_value);
        }
        mapreduce::emit(mr, key, std::to_string(value));
    }
};

int main(int argc, char** argv) {
    auto start = std::chrono::high_resolution_clock::now();

    mapreduce::MapReduceSpecification spec;
    spec.input_dir_name = argv[1];
    spec.output_filename = argv[2];
    spec.num_mappers = 1;
    spec.num_reducers = 1;
    spec.max_segment_size = 16 * 1024 * 1024;

    WordCountMapper wc_mapper;
    WordCountReducer wc_reducer;

    spec.mapper = &wc_mapper;
    spec.reducer = &wc_reducer;

    mapreduce::execute(spec);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
    std::cout << "elapsed time: " << ms.count() << "ms" << std::endl;

    return 0;
}