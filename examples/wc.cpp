#include <iostream>
#include <string>
#include <functional>

extern "C" void map(const char* c_str_input, void (*emit) (const char*, const char*)) {
    std::string input(c_str_input);
    const size_t& n = input.size();

    std::string word;
    for (size_t i = 0; i < n; ++i) {
        // Skip past leading whitespace
        while (i < n && isspace(input[i]))
            ++i;

        // Start of the word
        size_t start = i;
        while (i < n && !isspace(input[i]))
            i++;

        // Emit the word
        if (start < i)
            emit(input.substr(start, i - start).c_str(), "1");
    }
}

extern "C" void reduce(const char* key, const char* const* values, int values_len, void (*emit)(const char*, const char*)) {
    int count = 0;

    // Sum all the values
    for (int i = 0; i < values_len; ++i) {
        count += std::stoi(values[i]);
    }
    
    // Emit the sum
    emit(key, std::to_string(count).c_str());
}
