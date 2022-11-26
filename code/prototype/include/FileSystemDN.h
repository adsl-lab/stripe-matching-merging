#ifndef LRC_FILESYSTEMDN_H
#define LRC_FILESYSTEMDN_H


#include <grpc++/health_check_service_interface.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <spdlog/logger.h>
#include "coordinator.grpc.pb.h"
#include "datanode.grpc.pb.h"
#include "devcommon.h"
#include <spdlog/spdlog.h>

namespace lrc {
    class FileSystemDN {
        FileSystemDN(const FileSystemDN &) = delete;

        FileSystemDN(FileSystemDN &&) = delete;

        FileSystemDN &operator=(const FileSystemDN &) = delete;

        FileSystemDN &operator=(FileSystemDN &&) = delete;

        std::string m_conf_path;
        std::string m_data_path{"./data/"};
        std::string m_datanodeupload_port;//socket listenning
        std::string m_datanodedownload_port;//socket listenning
        std::string m_log_path;

        std::shared_ptr<spdlog::logger> m_dn_logger;


    public:

        FileSystemDN(int mDefaultblocksize = 64,const std::string mConfPath = "./conf/configuration.xml",
                     const std::string mLogPath = "./log/logFile.txt",
                     const std::string mDataPath = "./data/");

        ~FileSystemDN();

        void Run() {

            if (!m_dn_fromcnimpl_ptr->isInitialized()) {
                m_dn_logger->error("dnfromcnimpl is still not initialzed!");
                return;
            }

            //need a builder



            std::string dn_fromcnimpl_rpc_uri = m_dn_fromcnimpl_ptr->getMDnfromcnUri();
            auto blksz = m_dn_fromcnimpl_ptr->getMDefaultblocksize();
            std::cout << "default blksz:"<<blksz<<std::endl;
            grpc::EnableDefaultHealthCheckService(true);
            grpc::reflection::InitProtoReflectionServerBuilderPlugin();
            grpc::ServerBuilder builder;
            // Listen on the given address without any authentication mechanism.
            builder.AddListeningPort(dn_fromcnimpl_rpc_uri, grpc::InsecureServerCredentials());
            // Register "service" as the instance through which we'll communicate with
            // clients. In this case it corresponds to an *synchronous* service.
            builder.RegisterService(m_dn_fromcnimpl_ptr.get());
            // Finally assemble the server.
            std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
            m_dn_logger->info("DataNode Server listening on {}", dn_fromcnimpl_rpc_uri);

            // Wait for the server to shutdown. Note that some other thread must be
            // responsible for shutting down the server for this call to ever return.
            server->Wait();
        }

    public:
        class FromCoordinatorImpl
                : public std::enable_shared_from_this<lrc::FileSystemDN::FromCoordinatorImpl>,
                  public datanode::FromCoodinator::Service {
        private:
            std::mutex notworking;
            std::string m_fs_uri;
            std::string m_dnfromcn_uri;
            std::string m_confpath;
            std::string m_datanodeupload_port;
            std::string m_datanodedownload_port;
            int m_defaultblocksize;

            std::string m_datapath;//to read write clear ...
            //optional a buffer of a packet size ?
            std::shared_ptr<spdlog::logger> m_dnfromcnimpl_logger;
            std::shared_ptr<coordinator::FileSystem::Stub> m_fs_stub;
            std::shared_ptr<coordinator::FromDataNode::Stub> m_cnfromdn_stub;

            bool m_initialized{false};
            asio::io_context m_ioservice;

        public:
            /*
             * perfect factory
             * */
            template<typename ...T >
            static std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> getptr(T&& ...t){
                return std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl>(new FromCoordinatorImpl(std::forward<T>(t)...));
            }


        private:

            static std::shared_ptr<asio::ip::tcp::acceptor> prepareacceptor(FileSystemDN::FromCoordinatorImpl & dnimpl,short port) {

                using namespace asio;
                static std::unordered_map<int,std::shared_ptr<asio::ip::tcp::acceptor>> mappings;

                if(mappings.contains(port)) return mappings[port];

                auto acptptr = std::make_shared<ip::tcp::acceptor>(dnimpl.m_ioservice,
                                                                   ip::tcp::endpoint(asio::ip::tcp::v4(), port),true);
                mappings.insert({port,acptptr});

                return acptptr;//auto move
            }

            FromCoordinatorImpl(const std::string &mConfPath,
                                const std::string &mDatapath
            );
            FromCoordinatorImpl();
        public:
            virtual ~FromCoordinatorImpl();

            grpc::Status pull_perform_push(::grpc::ServerContext *context, const::datanode::OP *request,
                                           ::datanode::RequestResult *response) override;

            void setMDnfromcnUri(const std::string &mDnfromcnUri);

            void setMDatanodeuploadPort(const std::string &mDatanodeuploadPort);

            void setMDatanodedownloadPort(const std::string &mDatanodedownloadPort);

            void setMDatapath(const std::string &mDatapath);

            const std::shared_ptr<spdlog::logger> &getMDnfromcnimplLogger() const;

            void setMDnfromcnimplLogger(const std::shared_ptr<spdlog::logger> &mDnfromcnimplLogger);

            const asio::io_context &getMIoservice() const;

            grpc::Status handleupload(::grpc::ServerContext *context, const ::datanode::UploadCMD *request,
                                             ::datanode::RequestResult *response) override;

            grpc::Status dopartialcoding(::grpc::ServerContext *context, const ::datanode::NodesLocation *request,
                                         ::datanode::RequestResult *response) override;

            grpc::Status doglobalcoding(::grpc::ServerContext *context, const ::datanode::NodesLocation *request,
                                        ::datanode::RequestResult *response) override;

            grpc::Status handlepush(::grpc::ServerContext *context, const ::datanode::HandlePushCMD *request,
                                    ::datanode::RequestResult *response) override;

            grpc::Status handledownload(::grpc::ServerContext *context, const ::datanode::DownloadCMD *request,
                                      ::datanode::RequestResult *response) override;

            grpc::Status clearallstripe(::grpc::ServerContext *context, const ::datanode::ClearallstripeCMD *request,
                                        ::datanode::RequestResult *response) override;

            grpc::Status clearstripe(::grpc::ServerContext *context, const ::datanode::StripeId *request,
                                     ::datanode::RequestResult *response) override;


            grpc::Status checkalive(::grpc::ServerContext *context, const ::datanode::CheckaliveCMD *request,
                                    ::datanode::RequestResult *response) override;

            const std::string &getMFsUri() const;

            void setMFsUri(const std::string &mFsUri);


            bool initialize();


            bool initstub();


            std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> get_sharedholder();

            grpc::Status renameblock(::grpc::ServerContext *context, const::datanode::RenameCMD *request,
                                     ::datanode::RequestResult *response) override;


            grpc::Status handlepull(::grpc::ServerContext *context, const::datanode::HandlePullCMD *request,
                                    ::datanode::RequestResult *response) override;

            const std::string &getMDnfromcnUri() const;

            const std::string &getMDatanodeUploadPort() const;
            const std::string &getMDatanodeDownloadPort() const;

            const std::string &getMDatapath() const;

            int getMDefaultblocksize() const;

            void setMDefaultblocksize(int mDefaultblocksize);

            bool isInitialized() const;

            void setInitialized(bool initialized);


            grpc::Status dodownload(::grpc::ServerContext *context, const ::datanode::DodownloadCMD *request,
                                    ::datanode::RequestResult *response) override;
        };


        class FromDataNodeImpl : public datanode::FromDataNode::Service {
        public:


        };


        std::shared_ptr<FromCoordinatorImpl> m_dn_fromcnimpl_ptr;


    };
}
#endif //LRC_FILESYSTEMDN_H
