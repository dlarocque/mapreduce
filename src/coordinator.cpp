#include "../include/mapreduce.hpp"
#include <chrono>

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_file> <server_address> <num_mappers> <num_reducers>" << std::endl;
        return 1;
    }
    
    std::string input_dir = argv[1];
    std::string output_file = argv[2];
    std::string server_address = argv[3];
    int num_mappers = std::stoi(argv[4]);
    int num_reducers = std::stoi(argv[5]);
    int max_segment_size = 16 * 1024 * 1024;

    // Create the Map Reduce specification
    mapreduce::MapReduceSpec spec;
    spec.input_dir_name = input_dir;
    spec.output_filename = output_file;
    spec.num_mappers = num_mappers;
    spec.num_reducers = num_reducers;
    spec.max_segment_size = max_segment_size;
    
    auto start = std::chrono::high_resolution_clock::now();

    // Start the Map Reduce job
    spec.execute();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "MapReduce job completed in " << elapsed.count() << " seconds" << std::endl;

    return 0;
}
