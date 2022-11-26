#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging

#include <FileSystemDN.h>

int main(int argc, char ** argv){
//    int blksz = std::stoi(argv[1]);
    lrc::FileSystemDN fileSystemDn;
    fileSystemDn.Run();
    return 0;
}