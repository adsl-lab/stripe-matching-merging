#include <datanode.grpc.pb.h>
#include <FileSystemDN.h>
#include <asio.hpp>
#include <grpc++/create_channel.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <boost/thread/thread.hpp>
#include <Jerasure/include/jerasure.h>
#include <erasurecoding/LRCCoder.h>
#include <boost/thread/barrier.hpp>
#include "ToolBox.h"

namespace lrc {

    grpc::Status lrc::FileSystemDN::FromCoordinatorImpl::handleupload(::grpc::ServerContext *context,
                                                                      const ::datanode::UploadCMD *request,

                                                                      ::datanode::RequestResult *response) {


        std::cout << "try prepare acceptor" << std::endl;
        std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> holdme = get_sharedholder();

        auto acptptr = prepareacceptor(*this, std::stoi(m_datanodeupload_port));

        if (acptptr) std::cout << "prepared acceptor on port " << m_datanodeupload_port << std::endl;
        else {
            std::cout << "prepared acceptor failed!" << std::endl;
        }
        auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        int blocksize = m_defaultblocksize * 1024;
        std::cout << "default blk size:" << m_defaultblocksize << std::endl;
        bool _aspart = request->aspart();
        auto handler = [_sockptr = std::move(
                sockptr), ptr = std::move(acptptr), defaultblksize = m_defaultblocksize, aspart = _aspart](
                std::string datadir, int total_blksize,
                std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> _this) mutable {
            //move in the outer uniqueptr,it's safe
            int stpid = -1;
            int blkid = -1;
            try {
                std::cout << "handle recv thread start" << std::endl;
                ptr->accept(*_sockptr);
                std::vector<char> buf(1024 * defaultblksize);
                std::cout << "accept client connection" << std::endl;
                asio::read(*_sockptr, asio::buffer(&stpid, sizeof(stpid)));
                if (aspart) {
                    asio::read(*_sockptr, asio::buffer(&blkid, sizeof(blkid)));
                    std::cout << "this connection will handle partial coded block " << stpid << "-" << blkid
                              << std::endl;
                } else {
                    std::cout << "this connection will handle block " << stpid << std::endl;
                }
                asio::read(*_sockptr, asio::buffer(buf, defaultblksize * 1024));
                if (aspart) {
                    std::cout << "receive partial coded block " << stpid << "-" << blkid << std::endl;
                } else {
                    std::cout << "receive block" << stpid << std::endl;
                }
                auto targetdir = aspart ? datadir + "part/" : datadir;
                auto writepath = aspart ? targetdir + std::to_string(stpid) + "-" + std::to_string(blkid) : targetdir +
                                                                                                            std::to_string(
                                                                                                                    stpid);
                if (!std::filesystem::exists(std::filesystem::path{targetdir}))
                    std::filesystem::create_directory(targetdir);
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                ofs.write(&buf[0], 1024 * defaultblksize);
                std::cout << "successfully write : " << ofs.tellp() << "bytes" << std::endl;
                ofs.flush();
            } catch (std::exception &e) {
                std::cout << "exception" << std::endl;
                std::cout << e.what() << std::endl;
            }
            grpc::ClientContext reportctx;
            coordinator::StripeId stripeId;
            stripeId.set_stripeid(stpid);
            coordinator::RequestResult reportres;
            std::cout << "datanode call reportblktransfer to cn!\n";
            const auto &status = _this->m_fs_stub->reportblockupload(&reportctx, stripeId, &reportres);
            std::cout << "datanode call reportblktransfer to cn success!\n";
            if (status.ok() && reportres.trueorfalse()) {
                std::cout << "report stripe transfer reach cn! " << std::endl;
                _this->m_dnfromcnimpl_logger->info("report stripe transfer reach cn!");

            } else {
                std::cout << "report stripe transfer not reach cn! " << std::endl;
                _this->m_dnfromcnimpl_logger->error("report stripe transfer not reach cn!");
            }
        };

        try {
            std::thread h(std::move(handler), m_datapath, blocksize, holdme);
            h.detach();
        } catch (std::exception &e) {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        std::cout << "receive askDNhandling rpc!\n";
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status lrc::FileSystemDN::FromCoordinatorImpl::clearallstripe(::grpc::ServerContext *context,
                                                                        const ::datanode::ClearallstripeCMD *request,
                                                                        ::datanode::RequestResult *response) {
        //clear dir mdatapth
        m_dnfromcnimpl_logger->info("clear data directory!");
        std::filesystem::remove_all(m_datapath);
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    FileSystemDN::FromCoordinatorImpl::FromCoordinatorImpl(
            const std::string &mConfPath,
            const std::string &mDatapath
    )
            : m_confpath(mConfPath), m_datapath(mDatapath) {
        m_dnfromcnimpl_logger = spdlog::stdout_color_mt("dncnimpl");
        if (!std::filesystem::exists(std::filesystem::path(m_datapath))) {
            std::filesystem::create_directory(std::filesystem::path(m_datapath));
        }
        auto res = initialize();
        if (!res) {
            m_dnfromcnimpl_logger->error("datanode fromcnimpl initialize failed!");
            return;
        }
        std::cout <<"datanode initialize success!" <<std::endl;
        m_dnfromcnimpl_logger->info("datanode initialize success!");
        std::cout <<m_dnfromcn_uri <<std::endl;
        res = initstub();
        std::cout <<"datanode initialize cn stub success!" <<std::endl;

        if (!res) {
            m_dnfromcnimpl_logger->error("datanode cnstub initialize failed!");
            return;
        }
        m_dnfromcnimpl_logger->info("datanode cnstub initialize success!");
        std::cout <<"datanode initialize cnstub success!" <<std::endl;
        m_initialized = true;

    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDnfromcnUri() const {
        return m_dnfromcn_uri;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDatanodeUploadPort() const {
        return m_datanodeupload_port;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDatapath() const {
        return m_datapath;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::clearstripe(::grpc::ServerContext *context, const ::datanode::StripeId *request,
                                                   ::datanode::RequestResult *response) {

        m_dnfromcnimpl_logger->info("clear stripe {}", request->stripeid());
        if (request->stripeid() == -1) std::filesystem::remove_all(m_datapath + "part/");
        else std::filesystem::remove(m_datapath + (std::to_string(request->stripeid())));
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::checkalive(::grpc::ServerContext *context,
                                                  const ::datanode::CheckaliveCMD *request,
                                                  ::datanode::RequestResult *response) {
        if (m_initialized) {
            response->set_trueorfalse(true);
        } else {
            response->set_trueorfalse(false);
        }
        return grpc::Status::OK;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDnfromcnUri(const std::string &mDnfromcnUri) {
        m_dnfromcn_uri = mDnfromcnUri;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDatanodeuploadPort(const std::string &mDatanodeuploadPort) {
        m_datanodeupload_port = mDatanodeuploadPort;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDatanodedownloadPort(const std::string &mDatanodedownloadPort) {
        m_datanodedownload_port = mDatanodedownloadPort;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDatapath(const std::string &mDatapath) {
        m_datapath = mDatapath;
    }

    const std::shared_ptr<spdlog::logger> &FileSystemDN::FromCoordinatorImpl::getMDnfromcnimplLogger() const {
        return m_dnfromcnimpl_logger;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::renameblock(::grpc::ServerContext *context, const ::datanode::RenameCMD *request,
                                                   ::datanode::RequestResult *response) {
        if (std::filesystem::exists(m_datapath + std::to_string(request->oldid()))) {
            std::filesystem::rename(m_datapath + std::to_string(request->oldid()),
                                    m_datapath + std::to_string(request->newid()));
        } else {
            std::cout << "old stripeid : " << request->newid() << " currently not exists" << std::endl;
        }
        return grpc::Status::OK;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDnfromcnimplLogger(
            const std::shared_ptr<spdlog::logger> &mDnfromcnimplLogger) {
        m_dnfromcnimpl_logger = mDnfromcnimplLogger;
    }

    const asio::io_context &FileSystemDN::FromCoordinatorImpl::getMIoservice() const {
        return m_ioservice;
    }


    bool FileSystemDN::FromCoordinatorImpl::isInitialized() const {
        return m_initialized;
    }

    void FileSystemDN::FromCoordinatorImpl::setInitialized(bool initialized) {
        FromCoordinatorImpl::m_initialized = initialized;
    }

    bool FileSystemDN::FromCoordinatorImpl::initialize() {
        //parse /conf/configuration.xml
        //parse xml
        pugi::xml_document xdoc;
        xdoc.load_file(m_confpath.c_str(), pugi::parse_default, pugi::encoding_utf8);
        auto propertynode = xdoc.child("properties").child("property");

        for (auto propattr = propertynode.first_attribute(); propattr; propattr = propattr.next_attribute()) {
            auto propname = propattr.name();
            auto propvalue = propattr.value();
            if (std::string{"fs_uri"} == propname) {
                m_fs_uri = propvalue;
            } else if (std::string{"datanodeupload_port"} == propname) {
                m_datanodeupload_port = propvalue;
            } else if (std::string{"datanodedownload_port"} == propname) {
                m_datanodedownload_port = propvalue;
            } else if (std::string{"default_block_size"} == propname) {
                m_defaultblocksize = std::stoi(propvalue);
            } else if (std::string{"datanode_uri"} == propname) {
                m_dnfromcn_uri = propvalue;
            }
        }
        return true;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMFsUri() const {
        return m_fs_uri;
    }

    void FileSystemDN::FromCoordinatorImpl::setMFsUri(const std::string &mFsUri) {
        m_fs_uri = mFsUri;
    }

    int FileSystemDN::FromCoordinatorImpl::getMDefaultblocksize() const {
        return m_defaultblocksize;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDefaultblocksize(int mDefaultblocksize) {
        m_defaultblocksize = mDefaultblocksize;
    }

    bool FileSystemDN::FromCoordinatorImpl::initstub() {
        m_fs_stub = std::move(coordinator::FileSystem::NewStub(grpc::CreateChannel(
                m_fs_uri, grpc::InsecureChannelCredentials()
        )));
        std::cout <<"cn uri:"<<m_fs_uri <<std::endl;

        return true;
    }

    std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> FileSystemDN::FromCoordinatorImpl::get_sharedholder() {
        return shared_from_this();
    }

    FileSystemDN::FromCoordinatorImpl::FromCoordinatorImpl() {

    }

    FileSystemDN::FromCoordinatorImpl::~FromCoordinatorImpl() {

    }


    grpc::Status
    FileSystemDN::FromCoordinatorImpl::handledownload(::grpc::ServerContext *context,
                                                      const ::datanode::DownloadCMD *request,
                                                      ::datanode::RequestResult *response) {
        std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> holdme = get_sharedholder();
        auto acptptr = prepareacceptor(*this, std::stoi(m_datanodedownload_port));
        auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        int blocksize = m_defaultblocksize * 1024;
        bool _aspart = request->aspart();
        auto handler = [_sockptr = std::move(
                sockptr), ptr = std::move(acptptr), defaultblksize = m_defaultblocksize, aspart = _aspart,_selfip = lrc::uritoipaddr(m_dnfromcn_uri)](
                std::string datadir, int total_blksize,
                std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> _this) mutable {
            //move in the outer uniqueptr,it's safe
            int stpid = -1;
            int blkid = -1;
            std::vector<char> buf(1024 * defaultblksize);
            ptr->accept(*_sockptr);
            asio::read(*_sockptr, asio::buffer(&stpid, sizeof(stpid)));
            if (aspart) {
                asio::read(*_sockptr, asio::buffer(&blkid, sizeof(blkid)));
                std::cout << "accept client connection for download partial coded block " << stpid << "-" << blkid
                          << std::endl;
            } else {
                std::cout << "accept client connection for download original block " << stpid << std::endl;
            }
            auto readdir = datadir;
            if (aspart) readdir = datadir + "part/";
            auto readpath = aspart ? readdir + std::to_string(stpid) + "-" + std::to_string(blkid) : readdir +
                                                                                                     std::to_string(
                                                                                                             stpid);
            if (!std::filesystem::exists(std::filesystem::path{readpath})) {
                _this->m_dnfromcnimpl_logger->warn("stripe not exists!");
                std::cout << "stripe not exists!" << std::endl;
                return;
            }

            //local , just give path to be read from peer
            if(const auto & remoteepstr = _sockptr->remote_endpoint().address().to_string();remoteepstr == _selfip)
            {
                auto absreadpath = std::filesystem::absolute(std::filesystem::path{readpath}).string();
                int pathlen = absreadpath.size();
                asio::write(*_sockptr, asio::buffer(&pathlen,sizeof(pathlen)));
                asio::write(*_sockptr,asio::buffer(absreadpath.c_str(),pathlen));
                std::cout << "self ip : " <<_selfip << " give local peer "<<remoteepstr << " downloadpath : " <<absreadpath<<std::endl;
                return ;
            }
            std::ifstream ifs(readpath, std::ios::binary | std::ios::in);
            ifs.seekg(0);
            ifs.read(&buf[0], 1024 * defaultblksize);
            std::cout << "successfully read : " << ifs.tellg() << "bytes from " << readpath
                      << std::endl;
            asio::write(*_sockptr, asio::buffer(buf, defaultblksize * 1024));
            std::cout << "successfully send !\n";
            _sockptr->close();
        };
        try {
            std::thread h(std::move(handler), m_datapath, blocksize, holdme);
            h.detach();
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
        }
        std::cout << "download prepared!\n";
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDatanodeDownloadPort() const {
        return m_datanodedownload_port;
    }


    grpc::Status
    FileSystemDN::FromCoordinatorImpl::pull_perform_push(::grpc::ServerContext *context, const ::datanode::OP *request,
                                                         ::datanode::RequestResult *response) {
        auto mode = request->op();
        auto holdme = get_sharedholder();
        int srcnums = request->from_size();
        int tonums = request->to_size();
        int stripeid = request->stripeid();

        //delete
        int shift = 0;
        int idx = 0;


        std::cout << "mode" << mode << "\n";
        std::cout << "this thread" << holdme.get() << "\n";
        std::cout << "srcnums" << srcnums << "\n";
        std::cout << "tonums" << tonums << "\n";
        std::cout << "stripeid" << stripeid << "\n";
        std::shared_ptr<boost::barrier> oneshot_barrier(std::make_shared<boost::barrier>(srcnums + 1));
        std::shared_ptr<boost::barrier> receiving_barrier(std::make_shared<boost::barrier>(srcnums + 1));
        auto inmemorypushtask = [&](char *src, const std::string &dstip, short dstport,
                                    int _defaultblocksize, int stripe_id) {
            asio::io_context pushctx;
            asio::ip::tcp::socket sock(pushctx);

            std::cout << "will push in memory data to " << dstip << ":" << dstport << "as " << stripeid << std::endl;
            asio::ip::tcp::endpoint ep(asio::ip::address_v4::from_string(dstip), dstport);
            sock.connect(ep);
            std::cout << "connection established with " << dstip << ":" << dstport << std::endl;
            asio::write(sock, asio::buffer(&stripe_id, sizeof(stripe_id)));
            asio::write(sock, asio::buffer(src, _defaultblocksize * 1024));

            std::cout << "successfully push a block to ipaddr:port" << dstip << ":" << dstport << std::endl;
        };

        auto pulltask = [](
                const std::string &pullingip, short pullingport,
                const std::string &_datapath, char *target_region,
                int _defaultblocksize, int _stripeid,
                bool beflush = false) {
            asio::io_context pullctx;
            asio::ip::tcp::socket sock(pullctx);
            std::cout << "thread prepare to pull from ipaddr:port" << pullingip << ":" << pullingport << std::endl;
            try {
                asio::ip::tcp::endpoint ep(asio::ip::address::from_string(pullingip), pullingport);
                sock.connect(ep);
                std::cout << "connected  ,will pull block " << _stripeid << " from  ip:" << pullingip << " port:"
                          << pullingport
                          << std::endl;
                asio::write(sock, asio::buffer(&_stripeid, sizeof(_stripeid)));
                std::vector<char> res(_defaultblocksize * 1024);
                auto readn = asio::read(sock, asio::buffer(&res[0], _defaultblocksize * 1024));
                std::cout << readn << " bytes block successfully pulled " << std::endl;
                if (beflush) {
                    std::string blkpath(_datapath + std::to_string(_stripeid));
                    std::ofstream ofs(blkpath);
                    ofs.write(&res[0], _defaultblocksize * 1024);
                    ofs.flush();
                    std::cout << "succesfully flush to local\n";
                } else {
                    //copy to target region
                    std::copy(res.cbegin(), res.cend(), target_region);
                    std::cout << "succesfully copy to target region\n";

                }
                std::cout << "successfully pull a block\n";
            } catch (std::exception &e) {
                std::cout << e.what() << std::endl;
            }
        };

        auto pushtask = [&](const std::string &dstip, short port,
                            const std::string &_datapath,
                            int _defaultblocksize, int src_stripeid, int dst_stripeid,
                            bool beremove = true) {
            asio::io_context pushctx;
            asio::ip::tcp::endpoint endpoint(asio::ip::address_v4::from_string(dstip), port);
            asio::ip::tcp::socket sock(pushctx);
            std::cout << "thread prepare block " << src_stripeid << "will push to ipaddr:port" << dstip << ":" << port
                      << "as" << "block " << dst_stripeid << std::endl;
            sock.connect(endpoint);
            std::cout << "connected to dst " << endpoint << std::endl;
            std::string blkpath{_datapath + std::to_string(src_stripeid)};
            if (!std::filesystem::exists(blkpath)) {
                std::cout << "block not exists\n";
                return;
            }
            try {
                std::vector<char> blk(_defaultblocksize * 1024);
                std::ifstream ifs(blkpath);
                ifs.read(&blk[0], _defaultblocksize * 1024);
                std::cout << "read local block size: " << ifs.tellg() << std::endl;
                asio::write(sock, asio::buffer(&dst_stripeid, sizeof(dst_stripeid)));
                asio::write(sock, asio::buffer(&blk[0], _defaultblocksize * 1024));
                //delete original
                if (beremove) {
                    std::filesystem::remove(blkpath);
                    std::cout << "remove block " << stripeid << std::endl;
                }
            } catch (std::exception &e) {
                std::cout << e.what() << std::endl;
            }
        };
        if (mode == datanode::OP_CODEC_NO) {
            //just pull a block or push a block , note : means delete original
            //push or pull
            std::cout << m_dnfromcn_uri << "in migration status" << std::endl;
            if (0 == tonums && 0 == srcnums) {
                //impossible case ?
            } else if (1 == tonums && 0 == srcnums) {
                //push mode
                // migration same as handlingpull but once connection over , delete original block
                //index = 0
                //push local stripe stripeid
                const std::string &dsturi = request->to(0);
                const std::string &dstip = uritoipaddr(dsturi);
                short dstport = std::stoi(dsturi.substr(dsturi.find(':') + 1)) + 12220; // upload
                std::cout << m_dnfromcn_uri << "will push block " << stripeid << " to " << dstip << ":" << dstport
                          << std::endl;

                std::thread t(pushtask,
                              dstip, dstport,
                              m_datapath,
                              m_defaultblocksize, stripeid, stripeid - 1,
                              true);
                t.detach();
            }
        } else {
            if (mode == datanode::OP_CODEC_LRC) {
                if (0 != srcnums) {
                    //worker
                    std::cout << m_dnfromcn_uri << " is a worker " << std::endl;
                    std::cout << m_dnfromcn_uri << "will perform coding job " << std::endl;
                    std::cout << "schema: " << srcnums << " " << static_cast<int>(srcnums / (tonums + 1)) << " "
                              << tonums + 1 << std::endl;
                    char **gpbuffer = new char *[srcnums];
                    char **res = new char *[tonums + 1];
                    for (int i = 0; i < srcnums; ++i) {
                        gpbuffer[i] = new char[m_defaultblocksize * 1024];//packet
                    }
                    for (int i = 0; i < tonums + 1; ++i) {
                        res[i] = new char[m_defaultblocksize * 1024];
                    }
                    //perform coding
                    std::cout << m_dnfromcn_uri << "will pull blocks first" << std::endl;
                    boost::thread_group tp;
                    for (int i = 0; i < srcnums; ++i) {
                        const auto &uri = request->from(i);
                        const auto &ip = uritoipaddr(uri);
                        short port = std::stoi(uri.substr(uri.find(':') + 1)) + 22220;//download
                        tp.create_thread(std::bind(pulltask,
                                                   ip, port,
                                                   m_datapath, gpbuffer[i],
                                                   m_defaultblocksize, (2 * i < srcnums ? stripeid : stripeid + 1),
                                                   false));
                    }

                    tp.join_all();
                    auto ecschema = std::make_tuple(srcnums, static_cast<int>(srcnums / (tonums + 1)),
                                                    tonums + 1);
                    LRCCoder coder(ecschema, true);
                    int l = srcnums / (tonums + 1); //  2*(k' div g' = l')
                    int w = l * ceil(log2(tonums + 2));// w = 2*l'*...
                    std::cout << "pulling blocks is completed , word length is : 2^" << w << std::endl;
                    coder.display_matrix();
                    coder.encode(gpbuffer, NULL, res, m_defaultblocksize * 1024);
                    //push to ...+12220
                    std::cout << "coding job completed , forwarding to others " << std::endl;
                    boost::thread_group tp2;
                    for (int i = 1; i < tonums + 1; ++i) {
                        const std::string &uri = request->to(i - 1);
                        const std::string &dstip = uritoipaddr(uri);
                        short dstport = std::stoi(uri.substr(uri.find(':') + 1)) + 12220;

                        tp2.create_thread(
                                std::bind(inmemorypushtask, res[i], dstip, dstport, m_defaultblocksize, stripeid));
                    }
                    tp2.join_all();

                    std::ofstream ofs(m_datapath + std::to_string(stripeid));
                    ofs.write(res[0], m_defaultblocksize * 1024);
                    ofs.flush();
                    std::cout << "successfully write new global pairty block in stripe " << stripeid
                              << " to local\n";
                    for (int i = 0; i < srcnums; ++i) {
                        delete[] gpbuffer[i];
                    }
                    for (int i = 0; i < tonums + 1; ++i) {
                        delete[] res[i];
                    }
                    delete[] gpbuffer;
                    delete[] res;
                } else {
                    //impossible case ?
                }
            } else if (mode == datanode::OP_CODEC_REUSE) {
                if (0 != srcnums) {
                    int multiby = pow(2, shift);
                    char **gpbuffer = new char *[srcnums];
                    for (int i = 0; i < srcnums; ++i) {
                        gpbuffer[i] = new char[m_defaultblocksize * 1024];//packet
                    }

                    //  1 [q1=(2^shift)] q1^2  q1^3...
                    // 1 [q2=(2^shift)^2] q2^2 ...


                    int q = multiby;
                    std::vector<int> codingmatrix((srcnums / 2) * (srcnums));
                    char **res = new char *[tonums + 1];
                    for (int i = 0; i < tonums + 1; ++i) {
                        res[i] = new char[m_defaultblocksize * 1024];
                        codingmatrix[i * (srcnums) + i] = 1;
                        codingmatrix[i * (srcnums) + i + (tonums + 1)] = q;
                        q = galois_single_multiply(q, multiby, shift * 2);
                    }
                    boost::thread_group tp;
                    const std::string &localip = uritoipaddr(m_dnfromcn_uri);
                    short listeningport_offset = std::stoi(m_dnfromcn_uri.substr(m_dnfromcn_uri.find(':') + 1)) + 22221;
                    for (int i = 0; i < srcnums; ++i) {
                        //please no flush
                        short port = listeningport_offset + idx * 23;
                        tp.create_thread(std::bind(pulltask,
                                                   localip, port,
                                                   m_datapath, gpbuffer[i],
                                                   m_defaultblocksize, stripeid,
                                                   false));
                    }
                    tp.join_all();
                    //perform reuse style computation
                    int *bitmatrix = jerasure_matrix_to_bitmatrix((srcnums), (srcnums / 2), shift * 2,
                                                                  &codingmatrix[0]);
                    jerasure_bitmatrix_encode((srcnums), (srcnums / 2), shift * 2, bitmatrix, gpbuffer,
                                              res,
                                              m_defaultblocksize * 1024, sizeof(long));
                    //forwarding global parities
                    boost::thread_group tp2;
                    for (int i = 0; i < tonums; ++i) {
                        const std::string &dsturi = request->to(i);
                        const std::string &dstip = uritoipaddr(dsturi);
                        short dstport = std::stoi(dsturi.substr(dsturi.find(':') + 1)) + 12220;
                        tp2.create_thread(std::bind(inmemorypushtask, res[i + 1],
                                                    dstip, dstport, m_defaultblocksize, stripeid));
                    }
                    tp2.join_all();
                    //flush res[0] to local
                    std::ofstream ofs(m_datapath + std::to_string(stripeid));
                    ofs.write(res[0], m_defaultblocksize * 1024);
                    ofs.flush();
                    std::cout << "successfully write new global pairty block in stripe " << stripeid
                              << " to local\n";
                    for (int i = 0; i < srcnums; ++i) {
                        delete[] gpbuffer[i];
                    }
                    for (int i = 0; i < tonums + 1; ++i) {
                        delete[] res[i];
                    }
                    delete[] gpbuffer;
                    delete[] res;


                } else {
                    //impossible case ?
                }
            } else if (mode == datanode::OP_CODEC_XOR) {
                //perfrom pull and xor and store
                char **lpbuffer = new char *[srcnums];
                char *xorres = new char[m_defaultblocksize * 1024];
                std::fill(xorres, xorres + m_defaultblocksize * 1024, 0);
                for (int i = 0; i < srcnums; ++i) {
                    lpbuffer[i] = new char[m_defaultblocksize * 1024];
                }

                boost::thread_group tp;
                for (int i = 0; i < srcnums; ++i) {
                    const auto &uri = request->from(i);
                    const auto &srcip = uritoipaddr(uri);
                    short srcport = std::stoi(uri.substr(uri.find(':') + 1)) + 22220;
                    tp.create_thread(std::bind(pulltask, srcip, srcport, m_datapath, lpbuffer[i],
                                               m_defaultblocksize, stripeid, false));
                }
                tp.join_all();

                //perform xoring
                for (int i = 0; i < srcnums; ++i) {
                    galois_region_xor(lpbuffer[i], xorres, m_defaultblocksize * 1024);
                }

                //flush to local
                std::ofstream ofs(m_datapath + std::to_string(stripeid));
                ofs.write(xorres, m_defaultblocksize * 1024);
                ofs.flush();
                std::cout << "successfully write " << ofs.tellp() << "bytes to local as block " << stripeid
                          << std::endl;

                //start another thread !
                //keep alive
                auto reporttask = [_holdme = holdme, _stripeid = stripeid]() {
                    grpc::ClientContext reportctx;
                    coordinator::StripeId stripeId;
                    stripeId.set_stripeid(_stripeid);
                    coordinator::RequestResult reportres;
                    _holdme->m_fs_stub->reportblockupload(&reportctx, stripeId, &reportres);
                };
                std::thread report_t(reporttask);
                report_t.detach();
            }
        }
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    //these method used for migration[with deletion]
    grpc::Status
    FileSystemDN::FromCoordinatorImpl::handlepull(::grpc::ServerContext *context,
                                                  const ::datanode::HandlePullCMD *request,
                                                  ::datanode::RequestResult *response) {
        short port = std::stoi(m_datanodeupload_port) + 20000;
        auto acptptr = prepareacceptor(*this, port);
        auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        bool _aspart = request->aspart();
        auto handler = [_sockptr = std::move(sockptr), ptr = std::move(
                acptptr), defaultblksize = m_defaultblocksize, datadir = m_datapath, aspart = _aspart]() {
            //move in the outer uniqueptr,it's safe
            int stpid = -1;
            int blkid = -1;
            std::vector<char> buf(1024 * defaultblksize);
            ptr->accept(*_sockptr);
            asio::read(*_sockptr, asio::buffer(&stpid, sizeof(stpid)));
            std::cout << "accept client connection for download block " << stpid << std::endl;
            auto readpath = aspart ? datadir + "part/" + std::to_string(stpid) + "-" + std::to_string(blkid) : datadir +
                                                                                                               std::to_string(
                                                                                                                       stpid);
            if (!std::filesystem::exists(std::filesystem::path{readpath})) {
                std::cout << "stripe not exists!" << std::endl;
                return;
            }
            if (aspart) {
                std::cout << "pull a partial coded block : \n";
            } else {
                std::cout << "pull an original block : \n";
            }
            std::ifstream ifs(readpath, std::ios::binary | std::ios::in);
            ifs.seekg(0);
            ifs.read(&buf[0], 1024 * defaultblksize);
            std::cout << "successfully read : " << ifs.tellg() << "bytes from " << readpath
                      << std::endl;
            asio::write(*_sockptr, asio::buffer(buf, defaultblksize * 1024));
            std::filesystem::remove(readpath);
            std::cout << "successfully send and delete!\n";
        };

        //lambda catch a noncopyable but only movable sockptr [uniqueptr]
        std::thread t(std::move(handler));
        t.detach();
    }


    grpc::Status FileSystemDN::FromCoordinatorImpl::dopartialcoding(::grpc::ServerContext *context,
                                                                    const ::datanode::NodesLocation *request,
                                                                    ::datanode::RequestResult *response) {
        //download from nodes CMD.aspart = request.ispart ?
        //perform xor currently
        //store result aspart?
        //stored as stpid-blkid blks
        auto fromnum = request->nodesuri().size();
        std::vector<std::string> fromuris(fromnum);
        std::copy(request->nodesuri().cbegin(), request->nodesuri().cend(), fromuris.begin());
        std::vector<int> fromstrpids(fromnum);
        std::copy(request->nodesstripeid().cbegin(), request->nodesstripeid().cend(), fromstrpids.begin());
        std::vector<int> fromblkids(fromnum);
        std::copy(request->nodesblkid().cbegin(), request->nodesblkid().cend(), fromblkids.begin());
        std::vector<bool> isparts(fromnum);
        std::copy(request->ispart().cbegin(), request->ispart().cend(), isparts.begin());
        bool aspart = request->aspart();
        int targetstrpid = request->targetstripeid();
        int targetblknum = request->targetblks();

        std::cout << "perform partial coding task\n";
        //1.prepare buffer for from nodes
        std::vector<std::vector<char>> srcbuffers(fromnum, std::vector<char>(m_defaultblocksize * 1024));

        auto downloadtask = [_defaultblksize = m_defaultblocksize,_selfip = lrc::uritoipaddr(m_dnfromcn_uri)](
                std::vector<char> targetbuffer, std::string _datapath,
                const std::string &pullingip, short pullingport, int stpid, int blkid, bool _ispart,
                bool beflush = false) {
            asio::io_context pullctx;
            asio::ip::tcp::socket sock(pullctx);
            std::cout << "thread prepare to pull from ipaddr:port" << pullingip << ":" << pullingport << std::endl;


            try {

                asio::ip::tcp::endpoint ep(asio::ip::address::from_string(pullingip), pullingport);
                sock.connect(ep);
                if (_ispart) {
                    std::cout << "connected  ,will pull partial coded block " << stpid << "-" << blkid << " from  ip:"
                              << pullingip << " port:"
                              << pullingport
                              << std::endl;
                } else {
                    std::cout << "connected  ,will pull original block " << stpid << " from  ip:" << pullingip
                              << " port:"
                              << pullingport
                              << std::endl;
                }
                asio::write(sock, asio::buffer(&stpid, sizeof(stpid)));
                if (_ispart) asio::write(sock, asio::buffer(&blkid, sizeof(blkid)));
                std::vector<char> res(_defaultblksize *1024);
                int readn = 0;
                if(_selfip == pullingip)
                {
                    //local
                    int pathlen = -1;
                    asio::read(sock, asio::buffer(&pathlen, sizeof(pathlen)));
                    std::string localreadpath(pathlen,'\0');
                    asio::read(sock, asio::buffer(localreadpath.data(), pathlen));
                    std::cout << "read from local filesystem path:"<<localreadpath<<std::endl;
                    std::ifstream ifs(localreadpath);
                    ifs.read(&res[0],_defaultblksize*1024);
                    readn = ifs.tellg();
                    if(ifs.good()) std::cout << readn << " bytes block successfully pulled " << std::endl;
                }else {
                    readn = asio::read(sock, asio::buffer(&res[0], _defaultblksize * 1024));
                    std::cout << readn << " bytes block successfully pulled " << std::endl;
                }
                if (beflush) {
                    std::ofstream ofs(_datapath);
                    ofs.write(&res[0], _defaultblksize * 1024);
                    ofs.flush();
                    std::cout << "succesfully flush to local\n";
                } else {
                    //copy to target region
                    std::copy(res.cbegin(), res.cend(), &targetbuffer[0]);
                    std::cout << "succesfully copy to target region\n";

                }
                std::cout << "successfully download a block\n";
            } catch (std::exception &e) {
                std::cout << e.what() << std::endl;
            }
        };

        boost::thread_group downloadtp;
        for (int i = 0; i < fromnum; ++i) {
            const auto &ipaddr = lrc::uritoipaddr(fromuris[i]);
            auto port = lrc::uritoport(fromuris[i]) +22222 - 10001 ;
            auto targetpath = (isparts[i] ? m_datapath + "part/" : m_datapath);
            downloadtp.create_thread(
                    std::bind(downloadtask, srcbuffers[i], targetpath, ipaddr,
                              port, fromstrpids[i], fromblkids[i],
                              isparts[i], false));
        }

        downloadtp.join_all();
        std::cout << "download task done!\n";
        //xored result
        std::vector<char> xorres(m_defaultblocksize * 1024);
        //perform xoring
        for (int i = 0; i < fromnum; ++i) {
            galois_region_xor(&srcbuffers[i][0], &xorres[0], m_defaultblocksize * 1024);
        }

        //stored result
        //check dir existed or not
        if(!std::filesystem::exists(std::filesystem::path{m_datapath})) std::filesystem::create_directory(m_datapath);
        const auto &targetwritepath = aspart ? m_datapath + "part/" : m_datapath;
        if(!std::filesystem::exists(std::filesystem::path{targetwritepath})) std::filesystem::create_directory(targetwritepath);
        if (aspart) {
            std::cout << "will perform flush dowloaded block as partial block\n";
            if (targetblknum < 0) {
                targetblknum = -targetblknum;
                for (int i = 0; i < targetblknum; ++i) {
                    if(std::filesystem::exists(std::filesystem::path{targetwritepath + std::to_string(targetstrpid) + "-" + std::to_string(i)}))
                    {
                        continue;
                    }
                    std::ofstream ofs(targetwritepath + std::to_string(targetstrpid) + "-" + std::to_string(i),
                                      std::ios::binary | std::ios::out | std::ios::trunc);
                    ofs.write(&xorres[0], m_defaultblocksize * 1024);
                    ofs.flush();
                    ofs.close();
                }
            } else {
                std::ofstream ofs(targetwritepath + std::to_string(targetstrpid) + "-" + std::to_string(targetblknum),
                                  std::ios::binary | std::ios::out | std::ios::trunc);
                ofs.write(&xorres[0], m_defaultblocksize * 1024);
                ofs.flush();
                ofs.close();
            }
        } else {
            std::cout << "will perform flush dowloaded block as normal block\n";
            std::ofstream ofs(targetwritepath + std::to_string(targetstrpid),
                              std::ios::binary | std::ios::out | std::ios::trunc);
            ofs.write(&xorres[0], m_defaultblocksize * 1024);
            ofs.flush();
            ofs.close();
        }

        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status FileSystemDN::FromCoordinatorImpl::doglobalcoding(::grpc::ServerContext *context,
                                                                   const ::datanode::NodesLocation *request,
                                                                   ::datanode::RequestResult *response) {

        return Service::doglobalcoding(context, request, response);
    }

    grpc::Status FileSystemDN::FromCoordinatorImpl::handlepush(::grpc::ServerContext *context,
                                                               const ::datanode::HandlePushCMD *request,
                                                               ::datanode::RequestResult *response) {


        return Service::handlepush(context, request, response);
    }

    grpc::Status FileSystemDN::FromCoordinatorImpl::dodownload(::grpc::ServerContext *context,
                                                               const ::datanode::DodownloadCMD *request,
                                                               ::datanode::RequestResult *response) {

        const auto &fromuri = request->nodesuri();
        auto fromstpid = request->nodesstripeid();
        auto fromblkid = request->nodesblkid();
        auto ispart = request->ispart();
        auto aspart = request->aspart();
        auto targetstpid = request->targetstripeid();
        auto targetblknum = request->targetblks();
        std::cout << "perform download task\n";
        //1.prepare buffer for from nodes
        std::vector<char> srcbuffer(m_defaultblocksize * 1024);

        auto downloadtask = [_defaultblksize = m_defaultblocksize,_selfip = lrc::uritoipaddr(m_dnfromcn_uri)](
                std::vector<char> targetbuffer, std::string _datapath,
                const std::string &pullingip, short pullingport, int stpid, int blkid, bool _ispart,
                bool beflush = false) {
            asio::io_context pullctx;
            asio::ip::tcp::socket sock(pullctx);
            std::cout << "thread prepare to pull from ipaddr:port" << pullingip << ":" << pullingport << std::endl;

            try {
                asio::ip::tcp::endpoint ep(asio::ip::address::from_string(pullingip), pullingport);
                sock.connect(ep);
                if (_ispart) {
                    std::cout << "connected  ,will pull partial coded block " << stpid << "-" << blkid << " from  ip:"
                              << pullingip << " port:"
                              << pullingport
                              << std::endl;
                } else {
                    std::cout << "connected  ,will pull original block " << stpid << " from  ip:" << pullingip
                              << " port:"
                              << pullingport
                              << std::endl;
                }
                asio::write(sock, asio::buffer(&stpid, sizeof(stpid)));
                if (_ispart) asio::write(sock, asio::buffer(&blkid, sizeof(blkid)));
                std::vector<char> res(_defaultblksize *1024);
                int readn = 0;
                //local
                if(_selfip == pullingip)
                {
                    //local
                    int pathlen = -1;
                    asio::read(sock, asio::buffer(&pathlen, sizeof(pathlen)));
                    std::string localreadpath(pathlen,'\0');
                    asio::read(sock, asio::buffer(localreadpath.data(), pathlen));
                    std::cout << "read from local filesystem path:"<<localreadpath<<std::endl;
                    std::ifstream ifs(localreadpath);
                    ifs.read(&res[0],_defaultblksize*1024);
                    readn = ifs.tellg();
                    if(ifs.good()) std::cout << readn << " bytes block successfully pulled " << std::endl;
                }else {
                    readn = asio::read(sock, asio::buffer(&res[0], _defaultblksize * 1024));
                    std::cout << readn << " bytes block successfully pulled " << std::endl;
                }

                readn = asio::read(sock, asio::buffer(&res[0], _defaultblksize * 1024));
                std::cout << readn << " bytes block successfully pulled " << std::endl;
                if (beflush) {
                    std::ofstream ofs(_datapath);
                    ofs.write(&res[0], _defaultblksize * 1024);
                    ofs.flush();
                    std::cout << "succesfully flush to local\n";
                } else {
                    //copy to target region
                    std::copy(res.cbegin(), res.cend(), &targetbuffer[0]);
                    std::cout << "succesfully copy to target region\n";

                }
                std::cout << "successfully download a block\n";
            } catch (std::exception &e) {
                std::cout << e.what() << std::endl;
            }
        };

        const auto &ipaddr = lrc::uritoipaddr(fromuri);
        auto port = lrc::uritoport(fromuri) + 22222 - 10001;
        std::thread downloadthread(
                std::bind(downloadtask, srcbuffer, (ispart ? m_datapath + "part/" : m_datapath), ipaddr,
                          port, fromstpid, fromblkid,
                          ispart, false));


        downloadthread.join();
        std::cout << "download task done!\n";

        //stored result
        if(!std::filesystem::exists(std::filesystem::path{m_datapath})) std::filesystem::create_directory(m_datapath);
        const auto &targetwritepath = aspart ? m_datapath + "part/" : m_datapath;
        if(!std::filesystem::exists(std::filesystem::path{targetwritepath})) std::filesystem::create_directory(targetwritepath);
        if (aspart) {
            std::cout << "will perform flush dowloaded block as partial block\n";
            for (int i = 0; i < targetblknum; ++i) {
                std::ofstream ofs(targetwritepath + std::to_string(targetstpid) + "-" + std::to_string(i),
                                  std::ios::out | std::ios::binary | std::ios::trunc);
                ofs.write(&srcbuffer[0], m_defaultblocksize * 1024);
                ofs.flush();
                ofs.close();
            }
        } else {
            std::cout << "will perform flush dowloaded block as normal block\n";
            std::ofstream ofs(targetwritepath + std::to_string(targetstpid),
                              std::ios::out | std::ios::binary | std::ios::trunc);
            ofs.write(&srcbuffer[0], m_defaultblocksize * 1024);
            ofs.flush();
            ofs.close();
        }

        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }


    FileSystemDN::~FileSystemDN() {
    }

    FileSystemDN::FileSystemDN(int mDefaultblocksize,const std::string mConfPath, const std::string mLogPath,
                               const std::string mDataPath) : m_conf_path(mConfPath), m_log_path(mLogPath),
                                                              m_data_path(mDataPath),
                                                              m_dn_fromcnimpl_ptr(
                                                                      FileSystemDN::FromCoordinatorImpl::getptr(
                                                                              mConfPath, mDataPath)) {

        m_dn_fromcnimpl_ptr->setMDefaultblocksize(mDefaultblocksize);
        m_dn_logger = spdlog::stdout_color_mt("datanode_logger");
        m_datanodeupload_port = m_dn_fromcnimpl_ptr->getMDatanodeUploadPort();
        m_datanodedownload_port = m_dn_fromcnimpl_ptr->getMDatanodeDownloadPort();
    }
}