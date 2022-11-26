#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging
#include "FileSystemCN.h"
int main(){

    lrc::FileSystemCN cn;
    cn.Run();
    return 0;
}