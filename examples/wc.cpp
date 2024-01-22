//
// Created by Daniel La Rocque on 2024-01-19.
//

#include "../include/mapreduce.hpp"

#include <iostream>
#include <vector>
#include <string>

class WordCountMapper : public mapreduce::Mapper {
    // TODO: Implement map function that emits intermediate key-value pairs
    void map(const std::string& input) override {
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
                mapreduce::emit_intermediate(input.substr(start, i - start), 1);
        }
    }
};

class WordCountReducer : public mapreduce::Reducer {
    void reduce(const std::unordered_map<std::string, unsigned>& intermediate) override {
        for (const auto& pair : intermediate) {
            mapreduce::emit(pair.first, pair.second);
        }
    }
};

int main(int argc, char** argv) {
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

    return 0;
}