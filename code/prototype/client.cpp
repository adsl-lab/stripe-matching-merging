#include "FileSystemClient.h"
#include "ToolBox.h"
#include <iomanip>
#include <bitset>

using namespace std;

int main() {

    {
        int blksz = 64;
        int stpnum = 100;
        int place = -1;
        int k,l,g;
        std::cout << "enter your k-l-g\n";
        std::cin>>k>>l>>g;
        std::cout << "enter your block size[KB]\n";
        std::cin >> blksz;
        std::cout << "enter your stripe number\n";
        std::cin >> stpnum;
        lrc::FileSystemClient fileSystemClient;
        std::cout << "enter your placement policy :1 for random 2 for sparse 3 for compact\n";
        std::cin>>place;
        if(place==1) {
            fileSystemClient.SetPlaceMentPolicy(lrc::FileSystemClient::PLACE::RANDOM);
        }else if(place == 2)
        {
            fileSystemClient.SetPlaceMentPolicy(lrc::FileSystemClient::PLACE::SPARSE);
        }else{
            fileSystemClient.SetPlaceMentPolicy(lrc::FileSystemClient::PLACE::COMPACT);
        }
        for (int i = 0; i < stpnum; ++i) {
            lrc::RandomStripeGenerator("teststripe" + std::to_string(i) + ".txt", k, blksz * 1024);
            fileSystemClient.UploadStripe("teststripe" + std::to_string(i) + ".txt", i, {k, l, g, blksz}, true);
        }
        auto stripelocs = fileSystemClient.ListStripes();
        for (const auto &stripe : stripelocs) {
            std::cout << "stripeid: " << stripe.stripeid << std::endl;
            for (const auto &node : stripe.blklocation) {
                std::cout << node << ("\n" == node ? "" : "\t");
            }
        }
        std::cout << std::endl;
//        fileSystemClient.DownLoadStripe("", "", 0);
        int scaleratio = 1;
        int mode = -1;
        int match=-1;
        std::cout << "enter your transition goal: 1 for g same 2 for g double\n";
        std::cin>>scaleratio;
        std::cout << "enter your coding policy: 1 for basic 2 for partial\n";
        std::cin >> mode;
        std::cout << "enter your matching policy: 1 for seq 2 for perfect\n";
        std::cin >> match;
        auto start = std::chrono::high_resolution_clock::now();
        if(mode == 1) {
            fileSystemClient.TransformRedundancy(coordinator::TransitionUpCMD_MODE_BASIC,scaleratio==2,match==2,2);
        }else if(mode == 2){
            fileSystemClient.TransformRedundancy(coordinator::TransitionUpCMD_MODE_BASIC_PART,scaleratio==2,match==2,2);
        }else{
            fileSystemClient.TransformRedundancy(coordinator::TransitionUpCMD_MODE_DESIGNED,scaleratio==2,match==2,2);
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto diff = end - start;
        std::cout << std::chrono::duration<double,std::milli>(diff).count() << "ms" << std::endl;
        std::cout << std::chrono::duration<double,std::nano>(diff).count() << "ns" << std::endl;

        //list all for check
        std::cout << "after transition: \n";
        stripelocs = fileSystemClient.ListStripes();
        for (const auto &stripe : stripelocs) {
            std::cout << "stripeid: " << stripe.stripeid << std::endl;
            for (const auto &node : stripe.blklocation) {
                std::cout << node << ("\n" == node ? "" : "\t");
            }
        }
        std::cout << std::endl;

//        //get fs view
//        fileSystemClient.ListStripes();
//        stripelocs = fileSystemClient.ListStripes();
//        for (const auto &stripe : stripelocs) {
//            std::cout << "stripeid: " << stripe.stripeid << std::endl;
//            for (const auto &node : stripe.blklocation) {
//                std::cout << node << ("\n" == node ? "" : "\t");
//            }
//        }
//        std::cout << std::endl;
    }
    return 0;
}