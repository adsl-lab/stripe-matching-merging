#ifndef LRC_METAINFO_H
#define LRC_METAINFO_H
#include "devcommon.h"

namespace lrc{
    /*
     * this file provides general filesystem meta infomation such as StripeInfo , ClusterInfo etc.
    */

    struct ECSchema{
        ECSchema() =default;

        ECSchema(int datablk, int localparityblk, int globalparityblk, int blksize) : datablk(datablk),
        localparityblk{localparityblk},globalparityblk{globalparityblk},blksize{blksize}{}
        int datablk;
        int localparityblk;
        int globalparityblk;
        int blksize; // count Bytes
    };

    struct ECSchemaHash{
        std::size_t operator()(const ECSchema & ecSchema)const {
            return std::hash<size_t>()(std::hash<int>()(ecSchema.datablk)+std::hash<int>()(ecSchema.localparityblk)+std::hash<int>()(ecSchema.globalparityblk));
        }
    };

    struct ECSchemaCMP{
        bool operator()(const ECSchema & left,const ECSchema & right) const
        {
            return left.datablk==right.datablk&&
            left.localparityblk==right.localparityblk&&
            left.globalparityblk==right.globalparityblk;
        }
    };


    enum ErasureCodingPolicy{
        LRC = 0 // default
    };
    struct StripeInfo{
        /*
         * describe a stripe[same as a file in this project] , consists of stripe width , local parity
         * and global parity blocks , currently our own LRC ECSchema
        */

        //location : an uri vector
        //ie : [{datablk :}ip:port1,ip:port2,...|{local_parity_blk :}ip:port k+1,...|{global_parityblk : }... ]
        std::vector<std::string> blklocation;

        //ie : [cluster1 , cluster1 ,cluster2,] means blklocation[i] coming from clusterdistribution[i]
        std::vector<int> clusterdistribution;

        std::string dir;
        int stripeid;
        ECSchema ecschema;
        ErasureCodingPolicy ecpolicy;


        bool operator<(const StripeInfo &rhs) const {
            return stripeid < rhs.stripeid;
        }

    };

    struct DataNodeInfo{
        std::unordered_set<int> stored_stripeid;//{stripeid,block_type(0:data 1:localp 2:globalp)}
        std::string crosscluster_routeruri;
        int clusterid;
        int consumed_space;//count in MB because blk size count in MB too
        int port;
    };

    struct ClusterInfo{
        std::vector<std::string> datanodesuri;
        std::string gatewayuri;
        int clusterid;
        int stripeload;//stripes number
        ClusterInfo(const std::vector<std::string> &datanodesuri, const std::string &gatewayuri, int clusterid,
                    int stripeload) : datanodesuri(datanodesuri), gatewayuri(gatewayuri), clusterid(clusterid),
                                      stripeload(stripeload) {}
        ClusterInfo()=default;
    };

    /*
     * decribe a filesystem namespace
     */
    /*
    struct FileSystemDirectoryTree{
        struct FileSystemDirectoryNode{
            FileSystemObject
            std::vector<FileSystemDirectoryNode *> childnodes;
        };
    };
    */
}

#endif //LRC_METAINFO_H
