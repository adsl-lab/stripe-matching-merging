#ifndef LRC_FILESYSTEMCLIENT_H
#define LRC_FILESYSTEMCLIENT_H
#include "devcommon.h"
#include "asio/thread_pool.hpp"
#include "pugixml.hpp"
#include <grpcpp/grpcpp.h>
#include <asio/io_context.hpp>

#include "erasurecoding/LRCCoder.h"

#include "MetaInfo.h"
#include "coordinator.grpc.pb.h"

#include <spdlog/logger.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
namespace lrc{
    class FileSystemClient{


        //for validation
        std::string m_meta_path ;
        std::string m_conf_path ;

        asio::io_context m_ioc;
        //read from conf
        std::string m_fs_uri ; //  denotes coordinator
        std::string m_self_uri ;//denotes self
        std::string m_log_path ;// denotes log path

        short m_dn_transferport;
        int m_default_blk_size;
    public:
        int getMDefaultBlkSize() const;

        void setMDefaultBlkSize(int mDefaultBlkSize);

    private:

        // cache filesystem metadata for validation
        std::unordered_map<int,StripeInfo> stripe_info;

        // concurrent r/w stripe

        // client-namenode stub
        // no client-datanode stub, because client can see a filesystem abstraction via coordinator
        std::unique_ptr<coordinator::FileSystem::Stub> m_fileSystem_ptr;
        std::shared_ptr<spdlog::logger> m_client_logger;

        // need a socketfactory and a logger
    public:
        FileSystemClient(const std::string & p_conf_path="./conf/configuration.xml",const std::string & p_fsimage_path="./meta/fsimage.xml"):m_conf_path(p_conf_path)
        {
            //parse config file
            pugi::xml_document doc;
            doc.load_file(p_conf_path.data(),pugi::parse_default,pugi::encoding_utf8);

            pugi::xml_node properties_node = doc.child("properties");
            auto property_node = properties_node.child("property");
            for(auto attr = property_node.first_attribute();attr;attr=attr.next_attribute())
            {
                auto prop_name = attr.name();
                auto prop_value = attr.value();

                if(std::string{"fs_uri"} == prop_name)
                {
                    m_fs_uri = prop_value;
                }else if(std::string{"log_path"} == prop_name)
                {
                    m_log_path = prop_value;
                }/*else if(std::string{"default_block_size"} == prop_name)
                {
                    m_default_blk_size=std::stoi(prop_value);
                }*/else if(std::string{"datanodetransfer_port"}==prop_name)
                {
                    m_dn_transferport = std::stoi(prop_value);
                }
            }
            auto channel = grpc::CreateChannel(m_fs_uri,grpc::InsecureChannelCredentials());
            m_fileSystem_ptr = coordinator::FileSystem::NewStub(channel);
            m_client_logger = spdlog::stdout_color_mt("client_logger");
        }

    private:
        FileSystemClient(const FileSystemClient &) =  delete;

        FileSystemClient & operator=(const FileSystemClient &) =delete ;

        FileSystemClient(FileSystemClient &&) = delete ;

        FileSystemClient & operator=(FileSystemClient &&) = delete;

    public:
        ~FileSystemClient() = default;


        bool UploadStripe(const std::string & srcpath,int stripeid,const ECSchema & ecschema,bool trivial=false);


        bool DownLoadStripe(const std::string & srcpath,const std::string & dstpath,int stripe_id);


        bool CreateDir(const std::string & dstpath) ;

        bool DeleteDir(const std::string & dstpath) ;


        bool TransformRedundancy(coordinator::TransitionUpCMD_MODE mode=coordinator::TransitionUpCMD_MODE_BASIC,bool doubled = false,bool perfectmatch=false,int step = 2);

        std::vector<StripeInfo> ListStripes() const;

        enum PLACE{
            COMPACT,
            RANDOM,
            SPARSE
        };

        bool SetPlaceMentPolicy(PLACE p);
    };
}
#endif //LRC_FILESYSTEMCLIENT_H
