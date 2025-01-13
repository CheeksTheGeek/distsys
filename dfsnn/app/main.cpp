import <iostream>;
import <vector>;
import <string>;
import <sstream>; // Required for std::stringstream
import <any>;     // Required for std::any
import <functional>; // Required for std::function
import <variant>;    // Required for std::variant

import argparse;
import ansi;

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("HDFS NameNode");

    parser.add_argument("-p", "--port")
        .help("Port to listen on")
        .default_value(8080) // Directly use int for default
        .scan<'d', int>();   // To convert the value to int

    parser.add_argument("-H", "--host")
        .help("Host to listen on")
        .default_value(std::string("0.0.0.0"));

    parser.add_argument("-d", "--data-dir")
        .help("Data directory")
        .default_value(std::string("/data"));

    parser.add_argument("-f", "--format")
        .help("Format the NameNode")
        .default_value(false)
        .implicit_value(true);

    parser.add_argument("-s", "--start")
        .help("Start the NameNode")
        .default_value(false)
        .implicit_value(true);

    parser.add_argument("-S", "--stop")
        .help("Stop the NameNode")
        .default_value(false)
        .implicit_value(true);

    parser.add_argument("-r", "--restart")
        .help("Restart the NameNode")
        .default_value(false)
        .implicit_value(true);

    parser.add_argument("-c", "--check")
        .help("Check the NameNode")
        .default_value(false)
        .implicit_value(true);

    try {
        parser.parse_args(argc, argv);
    } catch (const std::runtime_error& e) {
        std::cerr << e.what() << std::endl;
        std::cerr << parser;
        return 1;
    }

    // Retrieve integer value for port
    int port = parser.get<int>("port");

    std::cout << "Port: " << port << std::endl;
    std::cout << "Host: " << parser.get<std::string>("host") << std::endl;
    std::cout << "Data directory: " << parser.get<std::string>("data-dir") << std::endl;
    std::cout << "Format: " << parser.get<bool>("format") << std::endl;
    std::cout << "Start: " << parser.get<bool>("start") << std::endl;

    return 0;
}