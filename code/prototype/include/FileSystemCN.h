#ifndef LRC_FILESYSTEMCN_H
#define LRC_FILESYSTEMCN_H

#include "coordinator.grpc.pb.h"
#include "datanode.grpc.pb.h"
#include "MetaInfo.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <spdlog/logger.h>
#include "devcommon.h"

namespace lrc {



    class FileSystemCN {
    public:



        FileSystemCN(const std::string &mConfPath = "./conf/configuration.xml",
                     const std::string &mMetaPath = "./meta/fsimage.xml",
                     const std::string &mLogPath = "./log/logFile.txt",
                     const std::string &mClusterPath = "./conf/cluster.xml");

        class FileSystemImpl final :public coordinator::FileSystem::Service{

        public:
            bool m_initialized{false};
            //logger itself is thread safe
            enum TYPE{
                DATA,LP,GP
            };

            enum PLACE{
                COMPACT_PLACE,
                RANDOM_PLACE,
                SPARSE_PLACE
            };

            enum MATCH{
                SEQ,RANDOM,PERFECT
            };
            typedef std::pair<int, std::vector<std::string>> StripeImage;//{id,{dn1,dn2,dn3,d,lp1,lp2,l...}}
            typedef std::unordered_map<std::string, std::tuple<int,FileSystemCN::FileSystemImpl::TYPE, int,bool>> StripeLayoutGroup;
            typedef std::tuple<StripeLayoutGroup,StripeLayoutGroup,StripeLayoutGroup>  SingleStripeLayout;
            typedef std::tuple<std::vector<int>,std::vector<int>, std::vector<int>> SingleStripeLayout_bycluster;
            typedef std::tuple<std::vector<int>,std::vector<int>,std::vector<std::string>,std::string> Partial_Coding_Plan;
            typedef std::tuple<std::vector<int>,std::vector<std::string>,std::vector<std::string>> Migration_Plan;
            typedef std::tuple<std::vector<int>,std::vector<int>,std::vector<std::string>,std::string> Global_Coding_Plan;

            typedef std::tuple<std::vector<Partial_Coding_Plan>,std::vector<Global_Coding_Plan>,Migration_Plan,std::vector<Partial_Coding_Plan>> Transition_Plan;

            std::shared_ptr<spdlog::logger> m_cn_logger;

            std::string m_conf_path;
            std::string m_meta_path;
            std::string m_log_path;
            std::string m_cluster_path;
            std::string m_fs_uri;

            ECSchema m_fs_defaultecschema;
            //performance issue to refactor as a rw mutex
            std::mutex m_fsimage_mtx;

//            std::unordered_map<ECSchema,std::set<int>,ECSchemaHash,ECSchemaCMP> m_fs_stripeschema;//fsimage.xml

            std::unordered_map<int, std::vector<std::string>> m_fs_image;//read from meta[fsimage.xml] in initialize stage
            std::unordered_map<std::string, DataNodeInfo> m_dn_info;//cluster.xml
            std::unordered_map<int,ClusterInfo> m_cluster_info;
            std::atomic<int> m_fs_nextstripeid{0};

            std::mutex m_stripeupdatingcount_mtx;
            std::condition_variable m_updatingcond;
            std::unordered_map<int,int> stripe_in_updatingcounter;
            std::unordered_map<int,StripeLayoutGroup> stripe_in_updating;// stripeid , unreceivedDNList
            std::unordered_set<int> stripeid_in_updating_sparse;
            std::unordered_set<int> stripeid_in_updating_random;
            std::unordered_set<int> stripeid_in_updating_compact;


            //stub
            std::map<std::string, std::unique_ptr<datanode::FromCoodinator::Stub>> m_dn_ptrs;

            //policy
            PLACE m_placementpolicy{PLACE::SPARSE_PLACE};
            std::unordered_map<int,lrc::ECSchema> m_sparsestripeids;
            std::unordered_map<int,lrc::ECSchema> m_randomstripeids;
            std::unordered_map<int,lrc::ECSchema> m_compactstripeids;
            //use a pre computed layout
            //there are 3 types placement
            std::unordered_map<lrc::ECSchema,std::vector<SingleStripeLayout>,ECSchemaHash,ECSchemaCMP> random_placement_layout;
            std::unordered_map<lrc::ECSchema,int,ECSchemaHash,ECSchemaCMP> random_placement_layout_cursor;

            std::unordered_map<lrc::ECSchema,std::vector<SingleStripeLayout>,ECSchemaHash,ECSchemaCMP> compact_placement_layout;
            std::unordered_map<lrc::ECSchema,int,ECSchemaHash,ECSchemaCMP> compact_placement_layout_cursor;

            std::unordered_map<lrc::ECSchema,std::vector<SingleStripeLayout>,ECSchemaHash,ECSchemaCMP> sparse_placement_layout;
            std::unordered_map<lrc::ECSchema,int,ECSchemaHash,ECSchemaCMP> sparse_placement_layout_cursor;





        public:
            grpc::Status deleteStripe(::grpc::ServerContext *context, const::coordinator::StripeId *request,
                                      ::coordinator::RequestResult *response) override;


            bool askDNhandling(const std::string & dnuri,int stripeid,bool isupload=true,bool ispart=false);


            grpc::Status uploadCheck(::grpc::ServerContext *context, const::coordinator::StripeInfo *request,
                                     ::coordinator::RequestResult *response) override;

            FileSystemImpl()=default;


            grpc::Status listAllStripes(::grpc::ServerContext *context, const::coordinator::ListAllStripeCMD *request,
                                        ::grpc::ServerWriter<::coordinator::StripeLocation> *writer) override;

            void updatestripeupdatingcounter(int stripeid, std::string fromdatanode_uri);

            bool isMInitialized() const;

            const std::shared_ptr<spdlog::logger> &getMCnLogger() const;

            const std::string &getMFsUri() const;

            const ECSchema &getMFsDefaultecschema() const;


            const std::unordered_map<int, std::vector<std::string>> &getMFsImage() const;

            const std::unordered_map<std::string, DataNodeInfo> &getMDnInfo() const;

            FileSystemImpl(const std::string &mConfPath = "./conf/configuration.xml",
                           const std::string &mMetaPath = "./meta/fsimage.xml",
                           const std::string &mLogPath = "./log/logFile.txt",
                           const std::string &mClusterPath = "./conf/cluster.xml");


            grpc::Status createDir(::grpc::ServerContext *context, const ::coordinator::Path *request,
                                   ::coordinator::RequestResult *response) override;


            grpc::Status uploadStripe(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                      ::coordinator::StripeDetail *response) override;


            bool initialize();

            bool initcluster() ;

            bool clearexistedstripes();

            void loadhistory();

            grpc::Status transitionup(::grpc::ServerContext *context, const::coordinator::TransitionUpCMD *request,
                                      ::coordinator::RequestResult *response) override;

            grpc::Status reportblockupload(::grpc::ServerContext *context,const coordinator::StripeId *request,
                                             ::coordinator::RequestResult *response) override;

            SingleStripeLayout
            placement_resolve(ECSchema ecSchema, PLACE placement);

            std::vector<std::tuple<int, int, int>>  singlestriperesolve(const std::tuple<int, int, int> &);

            std::vector<SingleStripeLayout_bycluster>
            generatelayout(const std::tuple<int, int, int> &para,PLACE placement,int stripenum,int step = 2);



            void flushhistory();

            virtual ~FileSystemImpl();

            grpc::Status listStripe(::grpc::ServerContext *context, const::coordinator::StripeId *request,
                                    ::coordinator::StripeLocation *response) override;


            grpc::Status downloadStripe(::grpc::ServerContext *context, const::coordinator::StripeId *request,
                                        ::coordinator::StripeDetail *response) override;

            std::vector<bool> checknodesalive(const std::vector<std::string> & vector);

            bool analysisdecodable(std::vector<std::string> dn, std::vector<bool> alivedn,
                                   std::vector<std::string> lp,
                                   std::vector<bool> alivelp, std::vector<std::string> gp,
                                   std::vector<bool> alivegp);

            bool analysislocallyrepairable(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                std::vector<std::string> lp,
                                                std::vector<bool> alivelp);

            bool dolocallyrepair(std::vector<std::string> dn, std::vector<bool> alivedn, std::vector<std::string> lp,
                                 std::vector<bool> alivelp);

            bool docompleterepair(std::vector<std::string> dn, std::vector<bool> alivedn, std::vector<std::string> lp,
                                  std::vector<bool> alivelp, std::vector<bool> gp, std::vector<bool> alivegp);

            bool askDNservepull(std::unordered_set<std::string> reqnodes, std::string src, int stripeid);

            grpc::Status
            downloadStripeWithHint(::grpc::ServerContext *context, const::coordinator::StripeIdWithHint *request,
                                   ::coordinator::StripeLocation *response) override;

            Transition_Plan
            generate_transition_plan(const std::vector<SingleStripeLayout> & layout,
                                     const std::tuple<int, int, int> &para_before,
                                     const std::tuple<int, int, int> &para_after,
                                     int from,int end,
                                     bool partial_gp = true,
                                     bool partial_lp=  true,
                                     int step=2);


            std::pair<std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string>>>,
            std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>>
            generate_basic_transition_plan(std::unordered_map<int, std::vector<std::string>>& fsimage);

            std::vector<std::tuple<int,int,std::vector<std::string>,std::string,std::vector<std::string>>>
            generate_designed_transition_plan(std::unordered_map<int, std::vector<std::string>> &fsimage);

            bool delete_global_parity_of(int stripeid);

            bool delete_block_of(int stripeid,TYPE tp);

            bool delete_block_of_node(const std::string & uri,int stripeid,TYPE tp);

            bool rename_block_to(int oldstripeid, int newstripeid,const std::unordered_set<std::string> & skipset);

            bool rename_block_to_node(const std::string & node,int oldstripeid, int newstripeid);


            grpc::Status
            setplacementpolicy(::grpc::ServerContext *context, const::coordinator::SetPlacementPolicyCMD *request,
                               ::coordinator::RequestResult *response) override;
            
            std::vector<SingleStripeLayout>
            layout_convert_helper(
                    std::vector<SingleStripeLayout_bycluster> & layout,int blksz,int step =2);



            bool refreshfilesystemimagebasic(
                    const std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>> &codingplan,
                    const std::tuple<int, std::vector<std::string>, std::vector<std::string>> &migrationplan);

            bool refreshfilesystemimagebasicpartial(
                    const std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>> &codingplan,
                    const std::tuple<int, std::vector<std::string>, std::vector<std::string>> &migrationplan);

            bool refreshfilesystemimagedesigned(
                    const std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>> &codingplan,
                    const std::tuple<int, std::vector<std::string>, std::vector<std::string>> &migrationplan);

            StripeImage
            executetransitionplan(Transition_Plan tuple, const std::vector<SingleStripeLayout> & layout, int from, int to,int after,
                                  std::tuple<int,int,int> para_before,
                                  std::tuple<int,int,int> para_after,
                                  bool partiallp,
                                  bool partialgp);

            SingleStripeLayout_bycluster single_layout_convert_to_clusterid(const SingleStripeLayout &layout);

            void layouts_to_graph_model(const std::vector<FileSystemCN::FileSystemImpl::SingleStripeLayout> &layouts,int upcodeordowncode);
        };

        class CoordinatorImpl final :public coordinator::FromDataNode::Service {

            std::shared_ptr<FileSystemImpl> m_fsimpl_ptr;
        public:
            const std::shared_ptr<FileSystemImpl> &getMFsimplPtr() const;

            void setMFsimplPtr(const std::shared_ptr<FileSystemImpl> &mFsimplPtr);

            CoordinatorImpl();

            CoordinatorImpl(const std::shared_ptr<FileSystemImpl> &mFsimplPtr) = delete;
        };

        void Run() {
            //need a builder
            std::string fsimpl_rpc_uri = m_fsimpl.getMFsUri();

            grpc::EnableDefaultHealthCheckService(true);
            grpc::reflection::InitProtoReflectionServerBuilderPlugin();
            grpc::ServerBuilder builder;
            // Listen on the given address without any authentication mechanism.
            builder.AddListeningPort(fsimpl_rpc_uri, grpc::InsecureServerCredentials());
            // Register "service" as the instance through which we'll communicate with
            // clients. In this case it corresponds to an *synchronous* service.
            builder.RegisterService(&m_fsimpl);
            // Finally assemble the server.
            std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
            std::cout << fsimpl_rpc_uri <<std::endl;
            m_cn_logger->info("Server listening on {}", fsimpl_rpc_uri);
            std::cout << m_fsimpl.getMDnInfo().size() <<std::endl;

            // Wait for the server to shutdown. Note that some other thread must be
            // responsible for shutting down the server for this call to ever return.
            server->Wait();
        }

        bool isInitialzed() const{
            return m_initialized;
        }
    private:
        bool m_initialized{false};
//
//
//        std::string m_conf_path;
//        std::string m_meta_path;
//        std::string m_log_path;
//        std::string m_cluster_path;
//        std::string m_fs_uri;
//
//        ECSchema m_fs_defaultecschema;
//        std::unordered_set<std::string> m_fs_existeddir;
//        std::map<std::string,std::vector<StripeInfo>> m_fs_image;//read from meta[fsimage.xml] in initialize stage
//        std::map<std::string,DataNodeInfo> m_dn_info;

        //impl
        FileSystemImpl m_fsimpl;
        CoordinatorImpl m_cnimpl;
        std::shared_ptr<spdlog::logger> m_cn_logger;
        bool syncFileSystem() {
            //this func restore file system to last recorded history version
            //specifically , this function will enforce DNs to delete extra blocks
        }
    };
}


#endif //LRC_FILESYSTEMCN_H
