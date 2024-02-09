#include "../include/mapreduce.hpp"

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_file> <num_mappers> <num_reducers>" << std::endl;
        return 1;
    }
    
    std::string input_dir = argv[1];
    std::string output_file = argv[2];
    int num_mappers = std::stoi(argv[3]);
    int num_reducers = std::stoi(argv[4]);

    std::cout << "Input directory: " << input_dir << std::endl;
    std::cout << "Output file: " << output_file << std::endl;
    std::cout << "Number of mappers: " << num_mappers << std::endl;
    std::cout << "Number of reducers: " << num_reducers << std::endl;
    
    // Create the Map Reduce specification
    mapreduce::MapReduceSpec spec;
    spec.input_dir_name = input_dir;
    spec.output_filename = output_file;
    spec.num_mappers = num_mappers;
    spec.num_reducers = num_reducers;
    spec.max_segment_size = 16 * 1024 * 1024;
    
    // Start the Map Reduce job
    spec.execute();

    return 0;
}