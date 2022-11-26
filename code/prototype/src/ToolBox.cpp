#include "ToolBox.h"

void lrc::RandomStripeGenerator(const std::string &srcpath, int blocks, int blocksize) {
    std::vector<char> datav(blocksize, 0);
    std::ofstream ofs(srcpath);
    for (int i = 0; i < blocks; ++i) {
        datav.assign(blocksize, '1' +i);
        ofs.write(datav.data(), blocksize);
        ofs.flush();
    }
}
