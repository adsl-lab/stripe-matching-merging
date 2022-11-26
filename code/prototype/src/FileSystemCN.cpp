#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include "ToolBox.h"
#include "FileSystemCN.h"
#include "coordinator.grpc.pb.h"
#include "combination_generator.h"

namespace lrc {
    FileSystemCN::FileSystemCN(const std::string &mConfPath,
                               const std::string &mMetaPath,
                               const std::string &mLogPath,
                               const std::string &mClusterPath) : m_fsimpl(mConfPath, mMetaPath, mLogPath,
                                                                           mClusterPath) {
        m_cn_logger = m_fsimpl.getMCnLogger(); //must wait until impl initialized first!
        if (!m_fsimpl.isMInitialized()) {
            m_cn_logger->error("initialize FileSystemImpl failed!");
            return;
        }
        m_initialized = true;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::createDir(::grpc::ServerContext *context, const ::coordinator::Path *request,
                                            ::coordinator::RequestResult *response) {
        auto _dstpath = request->dstpath();
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::uploadStripe(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                               ::coordinator::StripeDetail *response) {
        std::scoped_lock slk(m_fsimage_mtx, m_stripeupdatingcount_mtx);
        int stripeid = m_fs_nextstripeid++;
        auto retstripeloc = response->mutable_stripelocation();
        int k = request->stripe_k();
        int l = request->stripe_l();
        int g = request->stripe_g();
        int sz = request->blksize();
        auto [cand_dn, cand_lp, cand_gp] = placement_resolve(
                {k, l, g, sz}, m_placementpolicy);
        m_cn_logger->info("placement resolved for stripe {}", stripeid);
        if (m_fs_image.count(stripeid)) {
            goto addstripelocation;
        } else {
            if (stripe_in_updating.contains(stripeid)) {
                m_cn_logger->info("inupdating for stripe {},will return cancel status", stripeid);
                return grpc::Status::CANCELLED;
            }

            StripeLayoutGroup combined;
            for (const auto &dn: cand_dn) {
                combined.insert(dn);
            }
            for (const auto &lp: cand_lp) {
                combined.insert(lp);
            }
            for (const auto &gp: cand_gp) {
                combined.insert(gp);
            }
            stripe_in_updating.insert({stripeid, combined});
            if (m_placementpolicy == PLACE::COMPACT_PLACE) {
                stripeid_in_updating_compact.insert(stripeid);
            } else if (m_placementpolicy == PLACE::SPARSE_PLACE) {
                stripeid_in_updating_sparse.insert(stripeid);
            } else {
                stripeid_in_updating_random.insert(stripeid);
            }
            stripe_in_updatingcounter[stripeid] = stripe_in_updating[stripeid].size();
        }

        m_cn_logger->info("picked datanodes for storing stripe {} :", stripeid);
        for (auto c: stripe_in_updating[stripeid]) {
            m_cn_logger->info("{}", c.first);
        }
        addstripelocation:
        m_cn_logger->info("addstripelocation for stripe {}", stripeid);
        std::vector<std::string> dn_uris(k);
        std::vector<std::string> lp_uris(l);
        std::vector<std::string> gp_uris(g);
        for (const auto &p: cand_dn) {
            const auto &[_uri, _stripegrp] = p;
            const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
            dn_uris[_blkid] = _uri;
            if (!askDNhandling(p.first, stripeid)) return grpc::Status::CANCELLED;
        }


        for (const auto &p: cand_lp) {
            const auto &[_uri, _stripegrp] = p;
            const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
            lp_uris[_blkid] = _uri;
            if (!askDNhandling(p.first, stripeid)) return grpc::Status::CANCELLED;
        }
        for (const auto &p: cand_gp) {
            const auto &[_uri, _stripegrp] = p;
            const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
            gp_uris[_blkid] = _uri;
            if (!askDNhandling(p.first, stripeid)) return grpc::Status::CANCELLED;
        }


        for (const auto &uri: dn_uris) {
            retstripeloc->add_dataloc(uri);
        }
        for (const auto &uri: lp_uris) {
            retstripeloc->add_localparityloc(uri);
        }
        for (const auto &uri: gp_uris) {
            retstripeloc->add_globalparityloc(uri);
        }
        auto stripeId = response->mutable_stripeid();
        stripeId->set_stripeid(stripeid);
        return grpc::Status::OK;
    }

    FileSystemCN::FileSystemImpl::FileSystemImpl(const std::string &mConfPath, const std::string &mMetaPath,
                                                 const std::string &mLogPath,
                                                 const std::string &mClusterPath)
            : m_conf_path(mConfPath), m_meta_path(mMetaPath), m_log_path(mLogPath), m_cluster_path(mClusterPath) {
        m_cn_logger = spdlog::stdout_color_mt("cn_logger");
        if (!std::filesystem::exists(std::filesystem::path{m_conf_path})) {
            m_cn_logger->error("configure file not exist!");
            return;
        }
        auto init_res = initialize();
        if (!init_res) {
            m_cn_logger->error("configuration file error!");
            return;
        }
        std::cout << "coordinator start init cluster" << std::endl;

        auto cluster_res = initcluster();
        if (!cluster_res) {
            m_cn_logger->error("cluster file error!");
            return;
        }
        std::cout << "coordinator start init " << m_cluster_info.size() << " clustersinfo" << std::endl;

        auto m_cluster_info_backup = m_cluster_info;
        /* init all stubs to dn */
        std::cout << "coordinator start init dn stubs" << std::endl;

        for (int i = 0; i < m_cluster_info_backup.size(); ++i) {

            auto dn_alive = std::vector<std::string>();
            for (auto p: m_cluster_info_backup[i].datanodesuri) {
                //filter out offline DNs
                //by call stubs checkalive
                std::cout << "coordinator  start init " << p << " stub " << std::endl;

                auto _stub = datanode::FromCoodinator::NewStub(
                        grpc::CreateChannel(p, grpc::InsecureChannelCredentials()));
                std::cout << "coordinator  init " << p << " stub success" << std::endl;
                std::cout << "coordinator  will try to checkalive " << p << std::endl;

                int retry = 3;//default redetect 3 times
                while (0 != retry) {

                    grpc::ClientContext clientContext;
                    datanode::CheckaliveCMD Cmd;
                    datanode::RequestResult result;
                    grpc::Status status;

                    status = _stub->checkalive(&clientContext, Cmd, &result);


                    if (status.ok()) {
                        m_cn_logger->info("{} is living !", p);
                        if (!result.trueorfalse()) {
                            m_cn_logger->warn("but not initialized!");
                            // 3 * 10s is deadline
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                            retry--;
                        } else {
                            dn_alive.push_back(p);
                            m_dn_ptrs.insert(std::make_pair(p, std::move(_stub)));
                            retry = 0;
                        }
                    } else {
                        m_dn_info.erase(p);
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        retry--;
                    }

                }
                std::cout << "coordinator  check " << p << " alive " << std::endl;

            }
            if (!dn_alive.empty()) m_cluster_info[i].datanodesuri = dn_alive;
            else {
                //whole cluster offline
                m_cluster_info.erase(i);
            }
            //timeout,this node is unreachable ...
        }

        if (!std::filesystem::exists(std::filesystem::path{mMetaPath})) {
            auto clear_res = clearexistedstripes();
            if (!clear_res) {
                m_cn_logger->error("Metapath {} does not exists , clear file system failed!", mMetaPath);
                return;
            }
            //create mMetaPath
            std::filesystem::create_directory(std::filesystem::path{m_meta_path}.parent_path());

        } else {
//            loadhistory(); //todo
            clearexistedstripes();
        }
        m_cn_logger->info("cn initialize success!");
        m_initialized = true;
    }

    bool FileSystemCN::FileSystemImpl::initialize() {
        /* set default ecschema initialize all DN stubs , load DN info Cluster info */
        //parse /conf/configuration.xml
        //parse xml
        pugi::xml_document xdoc;
        xdoc.load_file(m_conf_path.c_str(), pugi::parse_default, pugi::encoding_utf8);
        auto propertynode = xdoc.child("properties").child("property");
        for (auto propattr = propertynode.first_attribute(); propattr; propattr = propattr.next_attribute()) {
            auto propname = propattr.name();
            auto propvalue = propattr.value();
            if (std::string{"fs_uri"} == propname) {
                m_fs_uri = propvalue;
                std::cout << "my coordinator uri :" << propvalue << std::endl;
            }
        }

        return true;
    }

    bool FileSystemCN::FileSystemImpl::initcluster() {
        //parse  cluster.xml
        try {
            pugi::xml_document xdoc;
            xdoc.load_file(m_cluster_path.c_str());
            auto clustersnode = xdoc.child("clusters");
            for (auto clusternode = clustersnode.child(
                    "cluster"); clusternode; clusternode = clusternode.next_sibling()) {

                auto id = clusternode.attribute("id").value();
                auto gatewayuri = clusternode.attribute("gateway").value();
                auto datanodes = clusternode.child("nodes");
                int cluster_id = std::stoi(id);
                DataNodeInfo dninfo;
                dninfo.clusterid = cluster_id;
                dninfo.crosscluster_routeruri = gatewayuri;

                std::vector<std::string> dns;
                for (auto eachdn = datanodes.first_child(); eachdn; eachdn = eachdn.next_sibling()) {
                    m_dn_info[eachdn.attribute("uri").value()] = dninfo;
                    dns.push_back(eachdn.attribute("uri").value());
                }
                ClusterInfo clusterInfo{dns, gatewayuri, cluster_id, 0};
                m_cluster_info[cluster_id] = clusterInfo;

            }
        } catch (...) {
            return false;
        }
        return true;
    }

    bool FileSystemCN::FileSystemImpl::isMInitialized() const {
        return m_initialized;
    }

    const std::shared_ptr<spdlog::logger> &FileSystemCN::FileSystemImpl::getMCnLogger() const {
        return m_cn_logger;
    }

    const std::string &FileSystemCN::FileSystemImpl::getMFsUri() const {
        return m_fs_uri;
    }

    const ECSchema &FileSystemCN::FileSystemImpl::getMFsDefaultecschema() const {
        return m_fs_defaultecschema;
    }

    const std::unordered_map<int, std::vector<std::string>> &FileSystemCN::FileSystemImpl::getMFsImage() const {
        return m_fs_image;
    }

    const std::unordered_map<std::string, DataNodeInfo> &FileSystemCN::FileSystemImpl::getMDnInfo() const {
        return m_dn_info;
    }

    bool FileSystemCN::FileSystemImpl::clearexistedstripes() {
        //call stubs to invoke DNs clear operation
        datanode::ClearallstripeCMD clearallstripeCmd;
        grpc::Status status;
        for (auto &stub: m_dn_ptrs) {
            grpc::ClientContext clientContext;
            datanode::RequestResult result;
            status = stub.second->clearallstripe(&clientContext, clearallstripeCmd, &result);
            if (status.ok()) {
                if (!result.trueorfalse()) {
                    m_cn_logger->error("{} clear all stripe failed!", stub.first);
                    return false;
                }
            } else {
                m_cn_logger->error("{} rpc error!", stub.first);
            }
        }
        return true;
    }

    void FileSystemCN::FileSystemImpl::loadhistory() {
        //parse /meta/fsimage.xml
        pugi::xml_document xdoc;
        xdoc.load_file("./meta/fsimage.xml");

        auto histnode = xdoc.child("history");
        if (!histnode) return;
        auto schemanode = histnode.child("ecschema");
        auto schemastr = std::string(schemanode.value());
        //k,l,g
        int k, l, g;
        int pos1 = 0, pos2 = 0;
        pos2 = schemastr.find(',', pos1);
        k = std::stoi(schemastr.substr(pos1, pos2 - pos1));
        pos1 = pos2;
        pos2 = schemastr.find(',', pos1);
        l = std::stoi(schemastr.substr(pos1, pos2));
        g = std::stoi(schemastr.substr(pos2));

        auto stripesnode = histnode.child("stripes");
        std::stringstream uri;
        for (auto stripenode = stripesnode.child("stripe"); stripenode; stripenode = stripenode.next_sibling()) {
            std::vector<std::string> _dns;
            int id = std::stoi(stripenode.attribute("id").value());
            std::string urilist = std::string(stripenode.attribute("nodes").value());
            int cursor = 0;
            while (cursor < urilist.size()) {
                if (urilist[cursor] != '#') {
                    uri.putback(urilist[cursor]);
                } else {
                    _dns.push_back(uri.str());
                    uri.clear();
                }
                cursor++;
            }
            m_fs_image.insert({id, _dns});
        }
    }

    void FileSystemCN::FileSystemImpl::updatestripeupdatingcounter(int stripeid, std::string fromdatanode_uri) {

        std::scoped_lock lockGuard(m_stripeupdatingcount_mtx);
        std::cout << fromdatanode_uri << " updatestripeupdatingcounter for" << stripeid << std::endl;

        // stripe_in_updating[stripeid][fromdatanode_uri] = true; //this line truely distributed env only
        if (stripe_in_updatingcounter.contains(stripeid) &&
            stripe_in_updatingcounter[stripeid] == 1)//all blks are ready in DN
        {
            if (!m_fs_image.contains(stripeid)) {
                m_fs_image.insert({stripeid, std::vector<std::string>()});

                std::vector<std::string> datauris;
                std::vector<std::string> lpuris;
                std::vector<std::string> gpuris;
                int blksz = 0;
                int k = 0;
                int l = 0;
                int g = 0;
                for (auto &p: stripe_in_updating[stripeid]) {
                    const auto &[_stripeuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
                    blksz = _blksz;
                    if (TYPE::DATA == _type) {
                        k++;
                    } else if (TYPE::LP == _type) {
                        l++;
                    } else {
                        g++;
                    }
                    m_dn_info[_stripeuri].stored_stripeid.insert(stripeid);
                }
                datauris.assign(k, "");
                lpuris.assign(l, "");
                gpuris.assign(g, "");
                for (const auto &p: stripe_in_updating[stripeid]) {
                    const auto &[_stripeuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
                    if (TYPE::DATA == _type) {
                        datauris[_blkid] = _stripeuri;
                    } else if (TYPE::LP == _type) {
                        lpuris[_blkid] = _stripeuri;
                    } else {
                        gpuris[_blkid] = _stripeuri;
                    }
                }

                m_fs_image[stripeid].insert(
                        m_fs_image[stripeid].end(), datauris.begin(), datauris.end());
                m_fs_image[stripeid].push_back("d");//marker
                m_fs_image[stripeid].insert(
                        m_fs_image[stripeid].end(), lpuris.begin(), lpuris.end());
                m_fs_image[stripeid].push_back("l");//marker
                m_fs_image[stripeid].insert(
                        m_fs_image[stripeid].end(), gpuris.begin(), gpuris.end());
                m_fs_image[stripeid].push_back("g");//marker

                //associate schema to this stripe
                if (stripeid_in_updating_compact.contains(stripeid)) {
                    m_compactstripeids.insert({stripeid, {k, l, g, blksz}});
                } else if (stripeid_in_updating_random.contains(stripeid)) {
                    m_randomstripeids.insert({stripeid, {k, l, g, blksz}});
                } else {
                    m_sparsestripeids.insert({stripeid, {k, l, g, blksz}});
                }
            }
            stripeid_in_updating_compact.erase(stripeid);
            stripeid_in_updating_random.erase(stripeid);
            stripeid_in_updating_sparse.erase(stripeid);
            stripe_in_updating.erase(stripeid);
            stripe_in_updatingcounter.erase(stripeid);
            m_updatingcond.notify_all();
        } else if (stripe_in_updatingcounter.contains(stripeid)) {
            stripe_in_updatingcounter[stripeid]--;
        }
    }

    void FileSystemCN::FileSystemImpl::flushhistory() {

        std::scoped_lock scopedLock(m_fsimage_mtx);
        //flush back fs_image
        pugi::xml_document xdoc;
        auto rootnode = xdoc.append_child("history");
        auto ecschemanode = rootnode.append_child("ecschema");
        std::string ec{std::to_string(m_fs_defaultecschema.datablk)
                       + std::to_string(m_fs_defaultecschema.localparityblk)
                       + std::to_string(m_fs_defaultecschema.globalparityblk)};
        ecschemanode.set_value(ec.c_str());
        auto stripesnode = rootnode.append_child("stripes");
        std::string urilist;
        for (auto p: m_fs_image) {
            auto singlestripenode = stripesnode.append_child("stripe");
            singlestripenode.append_child("id").set_value(std::to_string(p.first).c_str());
            auto urilistsnode = singlestripenode.append_child("nodes");
            for (auto &nodeuri: p.second) {
                //dn1#dn2#...#*#lp1#lp2#...#*#gp1#gp2#...#*#
                urilist.append(("d" == nodeuri || "l" == nodeuri || "g" == nodeuri) ? "*" : nodeuri).append("#");
            }
            //throw away last"#"
            urilist.pop_back();
            singlestripenode.set_value(urilist.c_str());
            urilist.clear();
        }
        xdoc.save_file("./meta/fsimage.xml");
    }

    FileSystemCN::FileSystemImpl::~FileSystemImpl() {
        m_cn_logger->info("cn im-memory image flush back to metapath!");
        flushhistory();
    }


    std::vector<std::tuple<int, int, int>>
    FileSystemCN::FileSystemImpl::singlestriperesolve(const std::tuple<int, int, int> &para) {
        std::vector<std::tuple<int, int, int>> ret;
        auto [k, l, g] = para;
        int r = k / l;

        //cases
        //case1:
        int theta1 = 0;
        int theta2 = 0;

        if (r <= g) {
            theta1 = g / r;
            int i = 0;
            for (i = 0; i + theta1 <= l; i += theta1) ret.emplace_back(theta1 * r, theta1, 0);
            if (i < l) {
                ret.emplace_back((l - i) * r, (l - i), 0);
            }
            ret.emplace_back(0, 0, g);
            ret.emplace_back(-1, theta1, l - i);
        } else if (0 == (r % (g + 1))) {
            theta2 = r / (g + 1);
            for (int i = 0; i < l; ++i) {
                for (int j = 0; j < theta2; ++j) ret.emplace_back(g + 1, 0, 0);
            }
            ret.emplace_back(0, l, g);
            ret.emplace_back(-1, -1, 0);
        } else {
            int m = r % (g + 1);
            theta2 = r / (g + 1);
            //each local group remains m data blocks and 1 lp block
            // m[<=>r] < g -> case1
            theta1 = g / m;
            for (int i = 0; i < l; ++i) {
                for (int j = 0; j < theta2; ++j) {
                    ret.emplace_back(g + 1, 0, 0);
                }
            }

            ret.emplace_back(-1, -1, -1);//acts as a marker

            int i = 0;
            for (i = 0; i + theta1 <= l; i += theta1) ret.emplace_back(theta1 * m, theta1, 0);
            if (i < l) {
                ret.emplace_back((l - i) * m, (l - i), 0);
            }
            ret.emplace_back(0, 0, g);

            ret.emplace_back(-1, theta1, l - i);
        }
        return ret;
    }

    std::vector<std::tuple<std::vector<int>, std::vector<int>, std::vector<int>>>
    FileSystemCN::FileSystemImpl::generatelayout(const std::tuple<int, int, int> &para,
                                                 FileSystemCN::FileSystemImpl::PLACE placement, int stripenum,
                                                 int step) {

        int c = m_cluster_info.size();
        std::vector<int> totalcluster(c, 0);
        std::iota(totalcluster.begin(), totalcluster.end(), 0);
        std::vector<std::tuple<std::vector<int>, std::vector<int>, std::vector<int>>> stripeslayout;
        auto [k, l, g] = para;
        int r = k / l;
        m_cn_logger->info("generate layout for ec {},{},{}", k, l, g);
        auto layout = singlestriperesolve(para);
        m_cn_logger->info("generate layout :");
        for(auto i:layout){
            auto [a,b,c]=i;
            std::cout << a <<" "<<b<<" "<<c<<"\n" ;
        }
        std::cout << std::endl;
        if (placement == PLACE::SPARSE_PLACE) {
            for (int j = 0; j * step < stripenum; ++j) {
                std::vector<std::vector<int>> datablock_location(step, std::vector<int>(k, -1));
                std::vector<std::vector<int>> lpblock_location(step, std::vector<int>(l, -1));
                std::vector<std::vector<int>> gpblock_location(step, std::vector<int>(g, -1));
                if (r <= g) {
                    // case1
                    // s1: D0D1L0 D2D3L1 G0G1G2
                    // s2: D0D1L0 D2D3L1 G0G1G2
                    std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
                    std::random_shuffle(clusters.begin(), clusters.end());
                    int idx_l = 0;
                    int idx_d = 0;
                    int n = 0;
                    //to pack residue
                    auto [_ignore1, theta1, res_grpnum] = layout.back();

                    int lim = (0 == res_grpnum ? layout.size() - 2 : layout.size() - 3);
                    for (int i = 0; i < lim; ++i) {
                        auto [cluster_k, cluster_l, cluster_g] = layout[i];
                        for (int u = 0; u < step; ++u) {
                            int idx_l1 = idx_l;
                            int idx_d1 = idx_d;
                            for (int x = 0; x < cluster_l; ++x) {
                                lpblock_location[u][idx_l1] = clusters[n];
                                idx_l1++;
                                for (int m = 0; m < r; ++m) {
                                    datablock_location[u][idx_d1] = clusters[n];
                                    idx_d1++;
                                }
                            }
                            n++;
                        }
                        idx_d += cluster_k;
                        idx_l += cluster_l;
                    }
                    if (res_grpnum) {
                        int cur_res_grp = 0;
                        for (int x = 0; x < step; ++x) {
                            if (cur_res_grp + res_grpnum > theta1) {
                                n++;//next cluster
                                cur_res_grp = 0;//zero
                            }

                            int cur_res_idxl = idx_l;
                            int cur_res_idxd = idx_d;

                            for (int y = 0; y < res_grpnum; ++y) {
                                lpblock_location[x][cur_res_idxl++] = clusters[n];
                                for (int i = 0; i < r; ++i) {
                                    datablock_location[x][cur_res_idxd++] = clusters[n];
                                }
                            }
                            cur_res_grp += res_grpnum;
                            //put res_grpnum residue group into cluster
                        }
                    }
                    //global cluster
                    for (int x = 0; x < step; ++x) {
                        for (int i = 0; i < g; ++i) gpblock_location[x][i] = clusters[n];
                        n++;
                    }
                } else if (0 == r % (g + 1)) {
                    //case2
                    //D0D1D2   D3D4D5   G0G1L0L1
                    std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
                    std::random_shuffle(clusters.begin(), clusters.end());
                    int n = 0;
                    for (int i = 0; i < step; ++i) {
                        int idxd = 0;
                        for (int x = 0; x < layout.size() - 2; ++x) {
                            for (int y = 0; y < g + 1; ++y) {
                                datablock_location[i][idxd++] = clusters[n];
                            }
                            n++;
                        }
                        //g and l parity cluster
                        for (int x = 0; x < g; ++x) {
                            gpblock_location[i][x] = clusters[n];
                        }
                        for (int x = 0; x < l; ++x) {
                            lpblock_location[i][x] = clusters[n];
                        }
                        n++;
                    }
                } else {
                    //special case3
                    std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
                    std::random_shuffle(clusters.begin(), clusters.end());
                    int residue = std::find(layout.cbegin(), layout.cend(), std::tuple<int, int, int>{-1, -1, -1}) -
                                  layout.cbegin();
                    int round = (r / (g + 1)) * (g + 1);
                    // [round,r)*cursor+lp
                    auto [_ignore1, pack_cluster_cap, frac_cluster_num] = layout.back();
                    int s = 0;
                    int packed_cluster_num = 0;
                    int packed_residue = 0;
                    if (frac_cluster_num) {
                        packed_cluster_num = (pack_cluster_cap / frac_cluster_num);
                        packed_residue = step % packed_cluster_num;
                        s = (0 != packed_residue) ? step / packed_cluster_num + 1 : step / packed_cluster_num;
                    }//require s clusters to pack the residue groups
                    int n = s;
                    int m = residue + 1;
                    int x = 0;
                    int y = 0;
                    int cursor = 1;
                    int lim = (0 == frac_cluster_num ? layout.size() - 2 : layout.size() - 3);

                    //TODO : cache optimization nested for loop
                    for (; m < lim; ++m) {
                        auto [residuecluster_k, residuecluster_l, residuecluster_g] = layout[m];
                        int residuecluster_r = residuecluster_k / residuecluster_l;
                        for (int u = 0; u < step; ++u) {
                            int cur_cursor = cursor;
                            for (x = 0; x < residuecluster_l; ++x) {
                                for (y = 0; y < residuecluster_r; ++y) {
                                    datablock_location[u][(cur_cursor - 1) * r + y + round] = clusters[n];
                                }
                                lpblock_location[u][cur_cursor - 1] = clusters[n];
                                cur_cursor++;
                            }
                            n++;
                        }
                        cursor += residuecluster_l;

                    }
                    if (frac_cluster_num) {
                        auto [res_k, res_l, res_g] = layout[m];
                        int res_r = res_k / res_l;
                        int cur_back = cursor;
                        for (int u1 = 0; u1 < step; ++u1) {
                            cursor = cur_back;
                            if (0 != u1 && 0 == (u1 % packed_cluster_num)) {
                                n++;
                            }
                            for (int x1 = 0; x1 < res_l; ++x1) {
                                lpblock_location[u1][cursor - 1] = clusters[n];
                                for (int y1 = 0; y1 < res_r; ++y1) {
                                    datablock_location[u1][(cursor - 1) * r + round + y1] = clusters[n];
                                }
                                cursor++;
                            }
                        }
                    }

                    n++;
                    //pack remained

                    for (int u = 0; u < step; ++u) {
                        for (x = 0; x < l; ++x) {
                            for (y = 0; y < round; ++y) {
                                datablock_location[u][x * r + y] = clusters[n];
                                if (0 == ((y + 1) % (g + 1))) n++;
                            }
                        }
                    }
                    for (int u = 0; u < step; ++u) {
                        for (x = 0; x < g; ++x) {
                            gpblock_location[u][x] = clusters[n];
                        }
                        n++;
                    }

                }
                for (int i = 0; i < step; ++i) {
                    stripeslayout.emplace_back(datablock_location[i], lpblock_location[i], gpblock_location[i]);
                }

                datablock_location.assign(step, std::vector<int>(k, -1));
                lpblock_location.assign(step, std::vector<int>(l, -1));
                gpblock_location.assign(step, std::vector<int>(g, -1));
            }
        } else if (placement == PLACE::COMPACT_PLACE) {
            for (int j = 0; j * step < stripenum; ++j) {
                std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
                std::random_shuffle(clusters.begin(), clusters.end());
                std::vector<int> datablock_location(k, -1);
                std::vector<int> lpblock_location(l, -1);
                std::vector<int> gpblock_location(g, -1);
                //ignore tuple if any element <0
                int idxd = 0;
                int idxl = 0;
                int n = 0;
                int r = k / l;
                int idxr = 0;
                int res_idxl = 0;
                int round = (r / (g + 1)) * (g + 1);
                for (auto cluster: layout) {
                    if (auto [_k, _l, _g] = cluster;_k < 0 || _l < 0 || _g < 0) {
                        continue;
                    } else {
                        //put _k data blocks ,_l lp blocks and _g gp blocks into this cluster

                        if (r <= g) {
                            for (int i = 0; i < _l; ++i) {
                                lpblock_location[idxl++] = clusters[n];
                            }
                            for (int i = 0; i < _k; ++i) {
                                datablock_location[idxd++] = clusters[n];
                            }
                            for (int i = 0; i < _g; ++i) {
                                gpblock_location[i] = clusters[n];
                            }

                            n++;
                        } else if (0 == (r % (g + 1))) {
                            //inner group index idxr
                            //group index idxl
                            //each cluster contain idxl group's subgroup
                            //D0D1   D2D3    G0L0
                            //D4D5   D6D7    G1L1

                            for (int i = 0; i < _k; ++i) {
                                datablock_location[idxl * r + idxr] = clusters[n];
                                idxr++;
                            }
                            if (idxr == r) {
                                idxl++;//+= theta1
                                idxr = 0;
                            }
                            for (int i = 0; i < _l; ++i) {
                                lpblock_location[i] = clusters[n];
                            }
                            for (int i = 0; i < _g; ++i) {
                                gpblock_location[i] = clusters[n];
                            }
                            n++;
                        } else {
                            //special case3
                            //D0D1D2   D3D4D5   D6L0   G1G2   D7D8D9   D10D11D12   D14D15D16   D17L2
                            //                  D13L1
                            if (0 == _l && 0 == _g) {
                                for (int i = 0; i < _k; ++i) {
                                    datablock_location[idxl * r + idxr] = clusters[n];
                                    idxr++;
                                }
                                if (idxr == round) {
                                    idxl++;
                                    idxr = 0;
                                }
                            } else if (0 == _g) {
                                idxr = round;
                                int res_idxr = idxr;
                                for (int i = 0; i < _l; ++i) {
                                    for (int x = 0; x < (_k / _l); ++x) {
                                        datablock_location[res_idxl * r + res_idxr] = clusters[n];
                                        res_idxr++;
                                        if (res_idxr == r) {
                                            lpblock_location[res_idxl] = clusters[n];
                                            res_idxl++;
                                            res_idxr = round;
                                        }
                                    }
                                }
                            } else {
                                //gp cluster
                                for (int i = 0; i < _g; ++i) {
                                    gpblock_location[i] = clusters[n];
                                }
                            }
                            n++;
                        }
                    }
                }

                for (int i = 0; i < step; ++i) {
                    stripeslayout.emplace_back(datablock_location, lpblock_location, gpblock_location);
                }
            }
        } else {

            //random
            std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
            for (int j = 0; j < stripenum; ++j) {
                std::random_shuffle(clusters.begin(), clusters.end());
                std::vector<int> datablock_location(k, -1);
                std::vector<int> lpblock_location(l, -1);
                std::vector<int> gpblock_location(g, -1);
                int n = 0;
                int idxl = 0;
                int idxd = 0;
                int idxr = 0;
                if (r <= g) {
                    for (auto cluster: layout) {
                        auto [_k, _l, _g] = cluster;
                        if (_k < 0) continue;
                        for (int x = 0; x < _l; ++x) {
                            int _r = _k / _l;//_r == r
                            for (int y = 0; y < _r; ++y) {
                                datablock_location[idxd] = clusters[n];
                                idxd++;
                            }
                            lpblock_location[idxl] = clusters[n];
                            idxl++;
                        }

                        for (int x = 0; x < _g; ++x) {
                            gpblock_location[x] = clusters[n];
                        }
                        n++;
                    }
                } else if (0 == (r % (g + 1))) {
                    for (auto cluster: layout) {
                        auto [_k, _l, _g] = cluster;
                        if (_k < 0 || _l < 0 || _g < 0) continue;
                        for (int i = 0; i < _k; ++i) {
                            datablock_location[idxd++] = clusters[n];
                        }
                        for (int i = 0; i < _l; ++i) {

                            lpblock_location[idxl++] = clusters[n];
                        }
                        for (int i = 0; i < _g; ++i) {
                            gpblock_location[i] = clusters[n];
                        }
                        n++;
                    }
                } else {
                    int round = ((k / l) / (g + 1)) * (g + 1);

                    for (auto cluster: layout) {
                        auto [_k, _l, _g] = cluster;
                        if (_k < 0 || _l < 0 || _g < 0) continue;
                        if (_l == 0 && _g == 0) {
                            //normal cluster
                            for (int x = 0; x < g + 1; ++x) {
                                datablock_location[idxd++] = clusters[n];
                            }
                            if (0 == (idxd % round)) {
                                idxd = (idxd / r + 1) * r;
                            }
                            n++;
                        } else if (_g == 0) {
                            //residue cluster
                            for (int x = 0; x < _l; ++x) {
                                lpblock_location[idxl] = clusters[n];
                                for (int y = 0; y < (_k / _l); ++y) {
                                    datablock_location[idxl * r + round + y] = clusters[n];
                                }
                                idxl++;
                            }
                            n++;
                        } else {
                            //gp cluster
                            for (int x = 0; x < _g; ++x) {
                                gpblock_location[x] = clusters[n];
                            }
                        }

                    }

                }
                stripeslayout.emplace_back(datablock_location, lpblock_location, gpblock_location);
            }
        }
        return stripeslayout;
    }

    FileSystemCN::FileSystemImpl::SingleStripeLayout_bycluster
    FileSystemCN::FileSystemImpl::single_layout_convert_to_clusterid(
            const FileSystemCN::FileSystemImpl::SingleStripeLayout &layout
    ) {
        const auto &[data_dns, lp_dns, gp_dns] = layout;
        std::vector<int> data_clusters(data_dns.size(), -1);
        std::vector<int> lp_clusters(lp_dns.size(), -1);
        std::vector<int> gp_clusters(gp_dns.size(), -1);

        for (const auto &[dn_uri, stripegrp]: data_dns) {
            data_clusters[std::get<0>(stripegrp)] = m_dn_info[dn_uri].clusterid;
        }

        for (const auto &[lp_uri, stripegrp]: lp_dns) {
            lp_clusters[std::get<0>(stripegrp)] = m_dn_info[lp_uri].clusterid;
        }

        for (const auto &[gp_uri, stripegrp]: gp_dns) {
            gp_clusters[std::get<0>(stripegrp)] = m_dn_info[gp_uri].clusterid;
        }
        return {data_clusters, lp_clusters, gp_clusters};
    }

    static int compute_overlap(const FileSystemCN::FileSystemImpl::SingleStripeLayout_bycluster &s1,
                               const FileSystemCN::FileSystemImpl::SingleStripeLayout_bycluster &s2,
                               int upcodeordowncode) {
        if (upcodeordowncode < 0) {
            const auto &[dc1, lc1, gc1] = s1;
            const auto &[dc2, lc2, gc2] = s2;
            int k = dc1.size();
            int l = lc1.size();
            int g = gc1.size();
            int r = k / l;
            int cost = 0;
            std::map<int, int> cluster_cnt;
            for (auto cluster: dc1) {
                cluster_cnt[cluster]++;
            }

            for (auto cluster: dc2) {
                cluster_cnt[cluster]++;
            }

            for (auto [cluster, cnt]: cluster_cnt) {
                cost += std::min(2 * g, cnt);
            }
            return cost;
        } else {
            const auto &[dc1, lc1, gc1] = s1;
            const auto &[dc2, lc2, gc2] = s2;
            int k = dc1.size();
            int l = lc1.size();
            int g = gc1.size();
            int r = k / l;
            int cost = 0;
            if (r <= g) {
                // return overlap and overflow cluster number
                // weighti for overlap cluster type I
                int theta = (g / r) * r;
                int type4_cnt = 2 * theta;
                int type3_cnt = theta + k % (theta);
                int type2_cnt = 2 * (k % theta);
                int weight1 = 1;
                int weight2 = 2;
                int weight3 = 3;
                int weight4 = 4;
                std::map<int, int> cluster_cnt;
                for (auto cluster: dc1) {
                    cluster_cnt[cluster]++;
                }

                for (auto cluster: dc2) {
                    cluster_cnt[cluster]++;
                }

                for (auto [cluster, cnt]: cluster_cnt) {
//            if (cnt == theta || cnt == (k % theta)) {
//                cost += weight1;
//            } else if (cnt == type2_cnt) {
//                cost += weight2;
//            } else if (cnt == type3_cnt) {
//                cost += weight3;
//            } else {
//                cost += weight4;
//            }
                    cost += std::min(g, cnt);
                    if (cnt > theta) cost += cnt - theta;
                }

                return cost;
            } else if (r % (g + 1) == 0) {
                // 16 2 3
                int type1_cnt = 2 * (g + 1);
                int type2_cnt = (g + 1);
                int weight1 = 1;
                int weight2 = 2;
                std::map<int, int> cluster_cnt;
                for (auto cluster: dc1) {
                    cluster_cnt[cluster]++;
                }

                for (auto cluster: dc2) {
                    cluster_cnt[cluster]++;
                }

                for (auto [cluster, cnt]: cluster_cnt) {
//            if (cnt == type1_cnt) {
//                cost += weight1;
//            } else {
//                cost += weight2;
//            }
                    cost += g;
                    if (cnt > (g + 1)) cost += cnt - (g + 1);
                }

                return cost;
            } else {
                // 8 2 2
                // 10 2 2
                int m = r % (g + 1);
                int res = l * m;
                int theta = (g / m) * m;
                int type_a1_cnt = 2 * (g + 1);
                int type_a2_cnt = (g + 1);
                int type_b1_cnt = 2 * theta;
                int type_b2_cnt = theta;
                int type_b3_cnt = theta + (res % theta);
                int type_b4_cnt = 2 * (res % theta);
                int type_b5_cnt = res % theta;
                int type_ab1_cnt = (g + 1) + (res % theta);
                int type_ab2_cnt = (g + 1) + theta;

                int weight_a1 = g + 1;
                int weight_a2 = 0;
                int weight_b1 = (theta/m)*(m+1);
                int weight_b2 = 0;
                int weight_b3 = ((res % theta)/m)*(m+1);
                int weight_b4 = ((2 * (res % theta) - theta)/m)*(m+1);
                int weight_b5 = 0;
                int weight_ab1 = ((res % theta)/m)*(m+1);
                int weight_ab2 = std::min(g+1,theta/m*(m+1));
                std::map<int, int> cluster_cnt;
                for (auto cluster: dc1) {
                    cluster_cnt[cluster]++;
                }

                for (auto cluster: dc2) {
                    cluster_cnt[cluster]++;
                }

                for (auto [cluster, cnt]: cluster_cnt) {
                    if (cnt == type_a1_cnt) {
                        cost += weight_a1;
                    } else if (cnt == type_a2_cnt) {
                        cost += weight_a2;
                    } else if (cnt == type_b1_cnt) {
                        cost += weight_b1;
                    } else if (cnt == type_b2_cnt) {
                        cost += weight_b2;
                    } else if (cnt == type_b3_cnt) {
                        cost += weight_b3;
                    } else if (cnt == type_b4_cnt) {
                        cost += weight_b4;
                    } else if (cnt == type_b5_cnt) {
                        cost += weight_b5;
                    } else if (cnt == type_ab1_cnt) {
                        cost += weight_ab1;
                    } else if (cnt == type_ab2_cnt) {
                        cost += weight_ab2;
                    }
                    cost += std::min(cnt, g);
                }

                return cost;
            }

        }
    }

    void FileSystemCN::FileSystemImpl::layouts_to_graph_model(
            const std::vector<FileSystemCN::FileSystemImpl::SingleStripeLayout> &layouts,
            int upcodeordowncode) {
        auto stripenum = layouts.size();
        std::vector<FileSystemCN::FileSystemImpl::SingleStripeLayout_bycluster> layouts_byclusterid(stripenum);

        for (int i = 0; i < stripenum; ++i) {
            layouts_byclusterid[i] = single_layout_convert_to_clusterid(layouts[i]);
        }

        std::vector<std::vector<int>> graph(stripenum, std::vector<int>(stripenum, 0));
        for (int i = 0; i < stripenum; ++i) {
            for (int j = 0; j <= i; ++j) {
                if (i == j) {
                    continue;
                }
                graph[i][j] = graph[j][i] = compute_overlap(layouts_byclusterid[i], layouts_byclusterid[j],
                                                            upcodeordowncode);
            }
        }

        // save graph to file GRAPH.txt
        std::ofstream ofs("GRAPH.txt");
        ofs << stripenum << " " << (stripenum * (stripenum - 1)) / 2 << "\n";
        for (int i = 0; i < stripenum; ++i) {
            for (int j = i + 1; j < stripenum; ++j) {
                ofs << i << " " << j << " " << graph[i][j] << "\n";
            }
        }
        ofs.flush();
        ofs.close();


    }


    std::vector<FileSystemCN::FileSystemImpl::SingleStripeLayout>
    FileSystemCN::FileSystemImpl::layout_convert_helper(
            std::vector<SingleStripeLayout_bycluster> &layout, int blksz, int step) {
        //{c0,c0,c0,c1,c1,c1...}{...}{cn,cn,cn,...} -> {d00 d01 d02,d10,d11,d12,...}
        std::vector<SingleStripeLayout> ret;
        for (int j = 0; j * step < layout.size(); ++j) {
            std::unordered_map<int, int> cluster_counter;
            std::unordered_set<int> done;
            for (int i = 0; i < step; ++i) {
                const auto &[data_cluster, lp_cluster, gp_cluster] = layout[j * step + i];
                for (auto c: data_cluster) {
                    if (cluster_counter.contains(c)) {
                        cluster_counter[c]++;
                    } else {
                        cluster_counter[c] = 1;
                    }
                }
                for (auto c: lp_cluster) {
                    if (cluster_counter.contains(c)) {
                        cluster_counter[c]++;
                    } else {
                        cluster_counter[c] = 1;
                    }
                }
                for (auto c: gp_cluster) {
                    if (cluster_counter.contains(c)) {
                        cluster_counter[c]++;
                    } else {
                        cluster_counter[c] = 1;
                    }
                }
            }
//            std::cout << "cluster counter over"<<std::endl;
            std::unordered_map<int, std::vector<std::string>> picked_nodes;
            for (auto c: cluster_counter) {
//                std::cout << "cluster :" <<c.first<<"require"<<c.second<<"nodes\n";
                std::vector<std::string> picked;
                std::sample(m_cluster_info[c.first].datanodesuri.cbegin(), m_cluster_info[c.first].datanodesuri.cend(),
                            std::back_inserter(picked), c.second, std::mt19937{std::random_device{}()});
                picked_nodes.insert({c.first, picked});
            }
//            std::cout << "pick cluster nodes over"<<std::endl;

            //second pass
            for (int i = 0; i < step; ++i) {
                StripeLayoutGroup data_nodes;
                StripeLayoutGroup lp_nodes;
                StripeLayoutGroup gp_nodes;
                auto [data_cluster, lp_cluster, gp_cluster] = layout[j * step + i];

                int blk_id = 0;
                for (auto c: data_cluster) {
                    //block id
                    data_nodes.insert(
                            {picked_nodes[c][cluster_counter[c] - 1],
                             std::make_tuple(blk_id, TYPE::DATA, blksz, false)});
                    blk_id++;
                    cluster_counter[c]--;
                    if (0 == cluster_counter[c]) {
                        cluster_counter.erase(c);
                    }
                }


                blk_id = 0;
                for (auto c: lp_cluster) {
                    lp_nodes.insert(
                            {picked_nodes[c][cluster_counter[c] - 1], std::make_tuple(blk_id, TYPE::LP, blksz, false)});
                    blk_id++;
                    cluster_counter[c]--;
                    if (0 == cluster_counter[c]) {
                        cluster_counter.erase(c);
                    }
                }

                blk_id = 0;
                for (auto c: gp_cluster) {
                    gp_nodes.insert(
                            {picked_nodes[c][cluster_counter[c] - 1], std::make_tuple(blk_id, TYPE::GP, blksz, false)});
                    blk_id++;
                    cluster_counter[c]--;
                    if (0 == cluster_counter[c]) {
                        cluster_counter.erase(c);
                    }
                }
                ret.push_back(std::make_tuple(data_nodes, lp_nodes, gp_nodes));

            }
        }

//        std::cout << "layout convert helper return "<<std::endl;
        return ret;
    }

    FileSystemCN::FileSystemImpl::SingleStripeLayout
    FileSystemCN::FileSystemImpl::placement_resolve(ECSchema ecSchema,
                                                    PLACE placement) {

        //propose step = 3 4 as future work
        //move correspoding cursor
        if (placement == PLACE::RANDOM_PLACE) {
//            m_cn_logger->info("random placement resolve");
            if (random_placement_layout_cursor.contains(ecSchema)) {
                //if require resize
//                m_cn_logger->info("random placement already resolved");

                int cursor = random_placement_layout_cursor[ecSchema];
                if (cursor == random_placement_layout[ecSchema].size()) {
                    //double size
//                    m_cn_logger->info("random placement resolve resize");

                    int appendsize = cursor;
                    auto genentry = generatelayout(
                            {ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                            PLACE::RANDOM_PLACE, appendsize);

                    auto appendentry = layout_convert_helper(genentry, ecSchema.blksize);
                    random_placement_layout[ecSchema].insert(random_placement_layout[ecSchema].cend(),
                                                             appendentry.cbegin(), appendentry.cend());
                }
                random_placement_layout_cursor[ecSchema]++;
                return random_placement_layout[ecSchema][cursor];
            } else {
                //initially 100 stripes
                //resize double ...
//                m_cn_logger->info("random placement resolve initial");

                auto initentry = generatelayout({ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                                                PLACE::RANDOM_PLACE, 100);
//                m_cn_logger->info("random placement resolve success");
//                for (auto ie: initentry) {
//                    auto [dns, lns, gns]=ie;
//                    for (auto dn:dns) {
//                        std::cout << dn <<" ";
//                    }
//                    std::cout << std::endl;
//                    for (auto ln:lns) {
//                        std::cout << ln <<" ";
//                    }
//                    std::cout << std::endl;
//                    for (auto gn:gns) {
//                        std::cout << gn <<" ";
//                    }
//                    std::cout << std::endl;
//                }

                random_placement_layout.insert({ecSchema,
                                                layout_convert_helper(initentry, ecSchema.blksize)});
                random_placement_layout_cursor[ecSchema] = 0;
                return random_placement_layout[ecSchema][random_placement_layout_cursor[ecSchema]++];
            }
        } else if (placement == PLACE::COMPACT_PLACE) {
//            m_cn_logger->info("compact placement resolve");

            if (compact_placement_layout_cursor.contains(ecSchema)) {
                //if require resize
//                m_cn_logger->info("compact placement already resolved");

                int cursor = compact_placement_layout_cursor[ecSchema];
                if (cursor == compact_placement_layout[ecSchema].size()) {
                    //double size

//                    m_cn_logger->info("compact placement resolve resize");
                    int appendsize = cursor;
                    auto genentry = generatelayout(
                            {ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                            PLACE::COMPACT_PLACE, appendsize);
                    auto appendentry = layout_convert_helper(genentry, ecSchema.blksize);
                    compact_placement_layout[ecSchema].insert(compact_placement_layout[ecSchema].cend(),
                                                              appendentry.cbegin(), appendentry.cend());
                }
                compact_placement_layout_cursor[ecSchema]++;
                return compact_placement_layout[ecSchema][cursor];
            } else {
                //initially 100 stripes
                //resize double ...
//                m_cn_logger->info("compact placement resolve initial");

                auto initentry = generatelayout({ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                                                PLACE::COMPACT_PLACE, 100);

                compact_placement_layout.insert({ecSchema,
                                                 layout_convert_helper(initentry, ecSchema.blksize)});
                compact_placement_layout_cursor[ecSchema] = 0;
                return compact_placement_layout[ecSchema][compact_placement_layout_cursor[ecSchema]++];
            }
        } else {
//            m_cn_logger->info("sparse placement resolve");

            if (sparse_placement_layout_cursor.contains(ecSchema)) {
                //if require resize
//                m_cn_logger->info("sparse placement already resolved");

                int cursor = sparse_placement_layout_cursor[ecSchema];
                if (cursor == sparse_placement_layout[ecSchema].size()) {

                    //double size
//                    m_cn_logger->info("sparse placement resolve resize");

                    int appendsize = cursor;
                    auto genentry = generatelayout(
                            {ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                            PLACE::SPARSE_PLACE, appendsize);
                    auto appendentry = layout_convert_helper(genentry, ecSchema.blksize);
                    sparse_placement_layout[ecSchema].insert(sparse_placement_layout[ecSchema].cend(),
                                                             appendentry.cbegin(), appendentry.cend());
                }
                sparse_placement_layout_cursor[ecSchema]++;
                return sparse_placement_layout[ecSchema][cursor];
            } else {
                //initially 100 stripes
                //resize double ...
//                m_cn_logger->info("sparse placement resolve initial");

                auto initentry = generatelayout({ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                                                PLACE::SPARSE_PLACE, 100);
                sparse_placement_layout.insert({ecSchema,
                                                layout_convert_helper(initentry, ecSchema.blksize)});
                sparse_placement_layout_cursor[ecSchema] = 0;
                return sparse_placement_layout[ecSchema][sparse_placement_layout_cursor[ecSchema]++];
            }
        }
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::uploadCheck(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                              ::coordinator::RequestResult *response) {
        //handle client upload check
        //check if stripeid success or not
        std::unique_lock uniqueLock(m_stripeupdatingcount_mtx);
        //60s deadline
        auto res = m_updatingcond.wait_for(uniqueLock, std::chrono::seconds(60), [&]() {
            return !stripe_in_updatingcounter.contains(request->stripeid());
        });
        response->set_trueorfalse(res);
        if (res) flushhistory();
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::reportblockupload(::grpc::ServerContext *context,
                                                    const ::coordinator::StripeId *request,
                                                    ::coordinator::RequestResult *response) {
        std::cout << "datanode " << context->peer() << " receive block of stripe " << request->stripeid()
                  << " from client successfully!\n";
        m_cn_logger->info("datanode {} receive block of stripe {} from client successfully!", context->peer(),
                          request->stripeid());
//        std::scoped_lock lockGuard(m_stripeupdatingcount_mtx);
        updatestripeupdatingcounter(request->stripeid(), context->peer());
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    bool
    FileSystemCN::FileSystemImpl::askDNhandling(const std::string &dnuri, int stripeid, bool isupload, bool ispart) {
        m_cn_logger->info("ask {} to wait for client", dnuri);
        grpc::ClientContext handlectx;
        datanode::RequestResult handlereqres;
        grpc::Status status;
        if (isupload) {
            datanode::UploadCMD uploadCmd;
            if (ispart) uploadCmd.set_aspart(ispart);
            status = m_dn_ptrs[dnuri]->handleupload(&handlectx, uploadCmd, &handlereqres);
        } else {
            datanode::DownloadCMD downloadCmd;
            if (ispart) downloadCmd.set_aspart(ispart);
            status = m_dn_ptrs[dnuri]->handledownload(&handlectx, downloadCmd, &handlereqres);
        }
        if (status.ok()) {
            return handlereqres.trueorfalse();
        } else {
            std::cout << "rpc askDNhandlestripe error!" << dnuri << std::endl;
            m_cn_logger->error("rpc askDNhandlestripe error!");
            return false;
        }
    }


    grpc::Status
    FileSystemCN::FileSystemImpl::deleteStripe(::grpc::ServerContext *context, const ::coordinator::StripeId *request,
                                               ::coordinator::RequestResult *response) {

        std::cout << "delete stripe" << request->stripeid() << std::endl;
        std::scoped_lock slk(m_stripeupdatingcount_mtx, m_fsimage_mtx);
        for (auto dnuri: stripe_in_updating[request->stripeid()]) {
            grpc::ClientContext deletestripectx;
            datanode::StripeId stripeId;
            stripeId.set_stripeid(request->stripeid());
            datanode::RequestResult deleteres;
            m_dn_ptrs[dnuri.first]->clearstripe(&deletestripectx, stripeId, &deleteres);
            m_dn_info[dnuri.first].stored_stripeid.erase(request->stripeid());
        }
        //delete
        stripe_in_updating.erase(request->stripeid());
        stripe_in_updatingcounter.erase(request->stripeid());
        m_fs_image.erase(request->stripeid());

        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::listStripe(::grpc::ServerContext *context, const ::coordinator::StripeId *request,
                                             ::coordinator::StripeLocation *response) {

        std::scoped_lock scopedLock(m_fsimage_mtx);
        int stripeid = request->stripeid();

        auto &loc_str = m_fs_image[stripeid];
        int start = 0;
        for (int i = 0; i < 3; ++i) {
            while (start < loc_str.size() && "d" != loc_str[start]) {
                response->add_dataloc(loc_str[start]);
                start++;
            }
            start++;//skip "dlg"
            while (start < loc_str.size() && "l" != loc_str[start]) {
                response->add_localparityloc(loc_str[start]);
                start++;
            }
            start++;
            while (start < loc_str.size() && "g" != loc_str[start]) {
                response->add_globalparityloc(loc_str[start]);
                start++;
            }
        }


        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::downloadStripe(::grpc::ServerContext *context, const ::coordinator::StripeId *request,
                                                 ::coordinator::StripeDetail *response) {

        std::scoped_lock slk(m_fsimage_mtx);
        auto retstripeloc = response->mutable_stripelocation();
        int stripeid = request->stripeid();
        std::vector<std::string> datauris;//extractdatablklocation(request->stripeid());
        std::vector<std::string> lpuris;//extractlpblklocation(request->stripeid());
        std::vector<std::string> gpuris;//extractgpblklocation(request->stripeid());
        goto serverequest;

        serverequest:
        int start = 0;
        while (start < m_fs_image[stripeid].size() &&
               m_fs_image[stripeid][start] != "d") {
            datauris.push_back(m_fs_image[stripeid][start]);
            start++;
        }
        //serve download
        bool res = askDNservepull(std::unordered_set<std::string>(datauris.cbegin(), datauris.cend()), "", stripeid);
        if (!res) {
            m_cn_logger->info("datanodes can not serve client download request!");
            return grpc::Status::CANCELLED;
        }

        for (int i = 0; i < datauris.size(); ++i) {
            retstripeloc->add_dataloc(datauris[i]);
        }

        std::cout << "returned locations!\n";
        return grpc::Status::OK;
    }

    bool FileSystemCN::FileSystemImpl::analysisdecodable(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                         std::vector<std::string> lp, std::vector<bool> alivelp,
                                                         std::vector<std::string> gp,
                                                         std::vector<bool> alivegp) {

        std::vector<int> remained(lp.size(), 0);
        int totalremain = 0;
        int r = gp.size();
        for (int i = 0; i < lp.size(); ++i) {
            //for each local group
            int currentgroupneedrepair = 0;
            for (int j = 0; j < r; ++j) {
                if (!alivedn[i * r + j]) currentgroupneedrepair++;
            }
            remained[i] = currentgroupneedrepair + (alivelp[i] ? -1 : 0);
            totalremain += remained[i];
        }

        if (totalremain <= r) {
            return true;
        } else {
            return false;
        }
    }

    bool
    FileSystemCN::FileSystemImpl::analysislocallyrepairable(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                            std::vector<std::string> lp, std::vector<bool> alivelp) {
        int r = dn.size() / lp.size();

        for (int i = 0; i < lp.size(); ++i) {
            //for each local group
            int currentgroupneedrepair = 0;
            for (int j = 0; j < r; ++j) {
                if (!alivedn[i * r + j]) currentgroupneedrepair++;
            }
            currentgroupneedrepair += (alivelp[i] ? -1 : 0);
            if (0 != currentgroupneedrepair) {
                return false;
            }
        }
        return true;
    }


    bool FileSystemCN::FileSystemImpl::docompleterepair(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                        std::vector<std::string> lp, std::vector<bool> alivelp,
                                                        std::vector<bool> gp, std::vector<bool> alivegp) {
        return false;
    }

    bool FileSystemCN::FileSystemImpl::askDNservepull(std::unordered_set<std::string> reqnodes, std::string src,
                                                      int stripeid) {
        for (const auto &node: reqnodes) {
            grpc::ClientContext downloadctx;
            datanode::DownloadCMD downloadCmd;
            datanode::RequestResult res;
            std::cout << "ask datanode : " << node << " to serve client download request" << std::endl;
            auto status = m_dn_ptrs[node]->handledownload(&downloadctx, downloadCmd, &res);
            if (!status.ok()) {
                std::cout << " datanode :" << node << " no response ! try next ... " << std::endl;
                //maybe have a blacklist
                m_cn_logger->info("choosen datanode {} , can not serve download request!", node);
                return false;
            }
        }
        return true;
    }

    grpc::Status FileSystemCN::FileSystemImpl::listAllStripes(::grpc::ServerContext *context,
                                                              const ::coordinator::ListAllStripeCMD *request,
                                                              ::grpc::ServerWriter<::coordinator::StripeLocation> *writer) {
        std::scoped_lock slk(m_fsimage_mtx);
        for (int i = 0; i < m_fs_image.size(); ++i) {
            coordinator::StripeLocation stripeLocation;
            int j = 0;
            for (; j < m_fs_image[i].size(); ++j) {
                if ("d" != m_fs_image[i][j]) {
                    stripeLocation.add_dataloc(m_fs_image[i][j]);
                    stripeLocation.add_dataloc(std::to_string(m_dn_info[m_fs_image[i][j]].clusterid));

                } else {
                    break;
                }
            }
            j++;
            for (; j < m_fs_image[i].size(); ++j) {
                if ("l" != m_fs_image[i][j]) {
                    stripeLocation.add_localparityloc(m_fs_image[i][j]);
                    stripeLocation.add_localparityloc(std::to_string(m_dn_info[m_fs_image[i][j]].clusterid));
                } else {
                    break;
                }
            }
            j++;
            for (; j < m_fs_image[i].size(); ++j) {
                if ("g" != m_fs_image[i][j]) {
                    stripeLocation.add_globalparityloc(m_fs_image[i][j]);
                    stripeLocation.add_globalparityloc(std::to_string(m_dn_info[m_fs_image[i][j]].clusterid));
                } else {
                    break;
                }
            }
            writer->Write(stripeLocation);
        }
        return grpc::Status::OK;
    }

    std::vector<bool> FileSystemCN::FileSystemImpl::checknodesalive(const std::vector<std::string> &vector) {
        return std::vector<bool>();
    }

    grpc::Status FileSystemCN::FileSystemImpl::downloadStripeWithHint(::grpc::ServerContext *context,
                                                                      const ::coordinator::StripeIdWithHint *request,
                                                                      ::coordinator::StripeLocation *response) {
        return Service::downloadStripeWithHint(context, request, response);
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::transitionup(::grpc::ServerContext *context,
                                               const ::coordinator::TransitionUpCMD *request,
                                               ::coordinator::RequestResult *response) {
        std::scoped_lock slk(m_fsimage_mtx, m_stripeupdatingcount_mtx);
        std::cout << "begin transition ... \n";
        auto totstart = std::chrono::high_resolution_clock::now();

        //stop the world
        coordinator::TransitionUpCMD_MODE mode = request->mode();
        Transition_Plan transitionPlan;
        decltype(m_fs_image) new_image;
        int step = request->step();
        int scaleratio = request->doublegp() ? 2 : 1;
        coordinator::TransitionUpCMD_MATCH match = request->match();


        std::vector<int> sparselayout2stripeid;
        std::for_each(m_sparsestripeids.cbegin(), m_sparsestripeids.cend(), [&sparselayout2stripeid](const auto &p) {
            sparselayout2stripeid.emplace_back(p.first);
        });
        std::vector<int> compactlayout2stripeid;
        std::for_each(m_compactstripeids.cbegin(), m_compactstripeids.cend(), [&compactlayout2stripeid](const auto &p) {
            compactlayout2stripeid.emplace_back(p.first);
        });
        std::vector<int> randomlayout2stripeid;
        std::for_each(m_randomstripeids.cbegin(), m_randomstripeids.cend(), [&randomlayout2stripeid](const auto &p) {
            randomlayout2stripeid.emplace_back(p.first);
        });
        std::vector<SingleStripeLayout> bemerged;
        //sparse first
        int mergedstripeid = 0;
        bool partialgp = false;
        bool partiallp = false;
        if (mode == coordinator::TransitionUpCMD_MODE_BASIC) {
            //pick one cluster and one node as a gp-node in that cluster (simplified , just pick one from original g nodes in odd stripe)
            partialgp = false;
            partiallp = false;
        } else if (mode == coordinator::TransitionUpCMD_MODE_BASIC_PART) {
            partialgp = true;
            partiallp = true;
        } else {
            partialgp = true;
            partiallp = true;
        }
        // for each existed schema , perform transition
        for (auto it = sparse_placement_layout.begin(); it != sparse_placement_layout.end(); ++it) {
            auto &[schema, layouts] = *it;
            if (match == coordinator::TransitionUpCMD_MATCH_SEQ) { ;
            } else if (match == coordinator::TransitionUpCMD_MATCH_RANDOM) { ;
            } else {
                // transform format and build graph
                layouts_to_graph_model(layouts, scaleratio == 1 ? 1 : -1);
                // do perfect matching
                system("./blossom -e GRAPH.txt -w RESULT.txt");

                // read result from file RESULT.txt
                std::ifstream ifs("RESULT.txt");

                // rearrange stripes id
                auto layouts_rearranged = layouts;
                int cursor_rearranged = 0;
                std::string line;
                while (std::getline(ifs, line)) {
                    std::stringstream ss(line);
                    int s_id = -1;
                    while (ss >> s_id) {
                        layouts_rearranged[cursor_rearranged++] = layouts[s_id];
                    }
                }
                layouts = layouts_rearranged;
            }

            int stripenum = sparse_placement_layout_cursor[schema];
            bemerged.assign(layouts.cbegin(), layouts.cbegin() + stripenum);

            for (int i = 0; i + step <= stripenum; i += step) {
                transitionPlan = generate_transition_plan(bemerged,
                                                          {schema.datablk, schema.localparityblk,
                                                           schema.globalparityblk},
                                                          {schema.datablk * 2, schema.localparityblk * 2,
                                                           schema.globalparityblk * scaleratio}, i, mergedstripeid,
                                                          partialgp, partiallp, 2);
                auto start = std::chrono::high_resolution_clock::now();
                const auto &new_image_stripe = executetransitionplan(transitionPlan, bemerged, i, i + step,
                                                                     mergedstripeid++,
                                                                     {schema.datablk, schema.localparityblk,
                                                                      schema.globalparityblk},
                                                                     {schema.datablk * 2, schema.localparityblk * 2,
                                                                      schema.globalparityblk * scaleratio},
                                                                     partialgp, partiallp);
                auto end = std::chrono::high_resolution_clock::now();
                auto diff = end - start;
                std::cout << std::chrono::duration<double, std::milli>(diff).count() << "ms" << std::endl;
                std::cout << std::chrono::duration<double, std::nano>(diff).count() << "ns" << std::endl;
                new_image.insert(new_image_stripe);
                std::cout << "sparse : [from,to)" << ":[" << i << "," << (i + step) << ")" << " do "
                          << "transition up in basic mode ...." << std::endl;
            }
        }

        for (auto it = compact_placement_layout.begin(); it != compact_placement_layout.end(); ++it) {
            auto &[schema, layouts] = *it;
            if (match == coordinator::TransitionUpCMD_MATCH_SEQ) { ;
            } else if (match == coordinator::TransitionUpCMD_MATCH_RANDOM) { ;
            } else {
                // transform format and build graph
                layouts_to_graph_model(layouts, scaleratio == 1 ? 1 : -1);
                // do perfect matching
                system("./blossom -e GRAPH.txt -w RESULT.txt");

                // read result from file RESULT.txt
                std::ifstream ifs("RESULT.txt");

                // rearrange stripes id
                auto layouts_rearranged = layouts;
                int cursor_rearranged = 0;
                std::string line;
                while (std::getline(ifs, line)) {
                    std::stringstream ss(line);
                    int s_id = -1;
                    while (ss >> s_id) {
                        layouts_rearranged[cursor_rearranged++] = layouts[s_id];
                    }
                }
                layouts = layouts_rearranged;
            }

            int stripenum = compact_placement_layout_cursor[schema];
            bemerged.assign(layouts.cbegin(), layouts.cbegin() + stripenum);
            for (int i = 0; i + step <= stripenum; i += step) {
                transitionPlan = generate_transition_plan(bemerged,
                                                          {schema.datablk, schema.localparityblk,
                                                           schema.globalparityblk},
                                                          {schema.datablk * 2, schema.localparityblk * 2,
                                                           schema.globalparityblk * scaleratio}, i, mergedstripeid,
                                                          partialgp, partiallp, 2);
                auto start = std::chrono::high_resolution_clock::now();
                const auto &new_image_stripe = executetransitionplan(transitionPlan, bemerged, i, i + step,
                                                                     mergedstripeid++,
                                                                     {schema.datablk, schema.localparityblk,
                                                                      schema.globalparityblk},
                                                                     {schema.datablk * 2, schema.localparityblk * 2,
                                                                      schema.globalparityblk * scaleratio},
                                                                     partialgp, partiallp);
                auto end = std::chrono::high_resolution_clock::now();
                auto diff = end - start;
                std::cout << std::chrono::duration<double, std::milli>(diff).count() << "ms" << std::endl;
                std::cout << std::chrono::duration<double, std::nano>(diff).count() << "ns" << std::endl;
                new_image.insert(new_image_stripe);
                std::cout << "compact : [from,to)" << ":[" << i << "," << (i + step) << ")" << " do "
                          << "transition up in basic mode ...." << std::endl;
            }
        }

        for (auto it = random_placement_layout.begin(); it != random_placement_layout.end(); ++it) {
            auto &[schema, layouts] = *it;
            if (match == coordinator::TransitionUpCMD_MATCH_SEQ) { ;
            } else if (match == coordinator::TransitionUpCMD_MATCH_RANDOM) { ;
            } else {
                // transform format and build graph
                layouts_to_graph_model(layouts, scaleratio == 1 ? 1 : -1);
                // do perfect matching
                system("./blossom -e GRAPH.txt -w RESULT.txt");

                // read result from file RESULT.txt
                std::ifstream ifs("RESULT.txt");

                // rearrange stripes id
                auto layouts_rearranged = layouts;
                int cursor_rearranged = 0;
                std::string line;
                while (std::getline(ifs, line)) {
                    std::stringstream ss(line);
                    int s_id = -1;
                    while (ss >> s_id) {
                        layouts_rearranged[cursor_rearranged++] = layouts[s_id];
                    }
                }
                layouts = layouts_rearranged;
            }


            int stripenum = random_placement_layout_cursor[schema];
            bemerged.assign(layouts.cbegin(), layouts.cbegin() + stripenum);
            for (int i = 0; i + step <= stripenum; i += step) {
                transitionPlan = generate_transition_plan(bemerged,
                                                          {schema.datablk, schema.localparityblk,
                                                           schema.globalparityblk},
                                                          {schema.datablk * 2, schema.localparityblk * 2,
                                                           schema.globalparityblk * scaleratio}, i, mergedstripeid,
                                                          partialgp, partiallp, 2);
                auto start = std::chrono::high_resolution_clock::now();
                const auto &new_image_stripe = executetransitionplan(transitionPlan, bemerged, i, i + step,
                                                                     mergedstripeid++,
                                                                     {schema.datablk, schema.localparityblk,
                                                                      schema.globalparityblk},
                                                                     {schema.datablk * 2, schema.localparityblk * 2,
                                                                      schema.globalparityblk * scaleratio},
                                                                     partialgp, partiallp);
                auto end = std::chrono::high_resolution_clock::now();
                auto diff = end - start;
                std::cout << std::chrono::duration<double, std::milli>(diff).count() << "ms" << std::endl;
                std::cout << std::chrono::duration<double, std::nano>(diff).count() << "ns" << std::endl;
                new_image.insert(new_image_stripe);
                std::cout << "random : [from,to)" << ":[" << i << "," << (i + step) << ")" << " do "
                          << "transition up in basic mode ...." << std::endl;
            }
        }
        auto totend = std::chrono::high_resolution_clock::now();
        auto totdiff = totend - totstart;
        std::cout << "total transition time : \n";
        std::cout << std::chrono::duration<double, std::milli>(totdiff).count() << "ms" << std::endl;
        std::cout << std::chrono::duration<double, std::nano>(totdiff).count() << "ns" << std::endl;
        m_fs_image = new_image;

        return grpc::Status::OK;
    }

    std::pair<std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string >>>,
            std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>>
    FileSystemCN::FileSystemImpl::generate_basic_transition_plan(
            std::unordered_map<int, std::vector<std::string>> &fsimage) {

        std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>> ret1;
        std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string >>> ret2;
        //todo load balance and priority schedule
        auto extractor = [&](const std::vector<std::string> &stripelocs) ->
                std::tuple<std::unordered_set<int>, std::unordered_set<int>, std::unordered_set<int>> {
            std::unordered_set<int> datacluster;
            std::unordered_set<int> globalcluster;
            bool flag = false;// following will be gp cluster
            int i = 0;
            for (; i < stripelocs.size(); ++i) {
                if (stripelocs[i] != "d" && stripelocs[i] != "l") {

                    datacluster.insert(m_dn_info[stripelocs[i]].clusterid);
                } else {
                    if (stripelocs[i] == "l") break;
                    i++;
                    continue;
                }
            }
            i++;
            for (; i < stripelocs.size(); ++i) {
                if (stripelocs[i] != "g") {
                    globalcluster.insert(m_dn_info[stripelocs[i]].clusterid);
                } else {
                    break;
                }
            }

            return {datacluster, {}, globalcluster};
        };

        int totalcluster = m_cluster_info.size();
        std::vector<int> total(totalcluster, 0);
        std::iota(total.begin(), total.end(), 0);
        for (int i = 1; i < fsimage.size(); i += 2) {
            auto kpos = std::find(fsimage[i - 1].cbegin(), fsimage[i - 1].cend(), "d");
            auto lpos = std::find(kpos, fsimage[i - 1].cend(), "l");
            auto gpos = std::find(lpos, fsimage[i - 1].cend(), "g");

            int k = kpos - fsimage[i - 1].cbegin();
            int l = lpos - kpos - 1;
            int g = gpos - lpos - 1;
            std::random_shuffle(total.begin(), total.end());
            auto [excluded, _ignore1, candg] = extractor(fsimage[i - 1]);
            auto [currentk, _ignore2, currentg] = extractor(fsimage[i]);
            std::vector<int> candcluster;//cand data cluster
            std::vector<int> overlap;
            std::vector<std::string> overlap_d_nodes;
            std::vector<std::string> overlap_l_nodes;
            std::vector<std::string> to_d_nodes;
            int u = 0;
            std::unordered_set<int> consideronce;
            for (; u < k; ++u) {
                int c = m_dn_info[fsimage[i][u]].clusterid;
                if (excluded.contains(c) || candg.contains(c)) {
                    if (!consideronce.contains(c)) {
                        overlap.push_back(c);
                        consideronce.insert(c);
                    }
                    overlap_d_nodes.push_back(fsimage[i][u]);
                }
            }
            ++u;
            for (; fsimage[i][u] != "l"; ++u) {
                int c = m_dn_info[fsimage[i][u]].clusterid;
                if (excluded.contains(c) || candg.contains(c)) {
                    if (!consideronce.contains(c)) {
                        overlap.push_back(c);
                        consideronce.insert(c);
                    }
                    overlap_l_nodes.push_back(fsimage[i][u]);
                }
            }

            for (int j = 0; j < totalcluster && overlap.size() > candcluster.size(); ++j) {
                if (!excluded.contains(total[j]) && !candg.contains(total[j])) {
                    candcluster.push_back(total[j]);
                }
            }

            //pick k datanodes l localparity nodes g globalparity nodes...
            //for this plan generator just pick 1 globalparity from candg , encoding, and forward to g-1 others
            auto target_g_cands = std::vector<std::string>(m_cluster_info[*candg.cbegin()].datanodesuri);
            std::random_shuffle(target_g_cands.begin(), target_g_cands.end());
            auto &target_coding_nodeuri = target_g_cands.front();
            std::vector<std::string> to_g_nodes(target_g_cands.cbegin() + 1, target_g_cands.cbegin() + g);
            std::vector<std::string> from_d_nodes(fsimage[i - 1].cbegin(), kpos);
            from_d_nodes.insert(from_d_nodes.end(),
                                fsimage[i].cbegin(),
                                std::find(fsimage[i].cbegin(), fsimage[i].cend(), "d"));

            std::vector<std::string> overlap_nodes;
            int idx = 0;
            while (idx < overlap_l_nodes.size()) {
                overlap_nodes.insert(overlap_nodes.end(),
                                     overlap_d_nodes.begin() + idx * k,
                                     overlap_d_nodes.begin() + (idx + 1) * k);
                overlap_nodes.push_back(overlap_l_nodes[idx]);
                std::vector<std::string> thiscluster(m_cluster_info[candcluster[idx]].datanodesuri);
                idx++;
                std::random_shuffle(thiscluster.begin(), thiscluster.end());
                to_d_nodes.insert(to_d_nodes.end(), thiscluster.cbegin(),
                                  thiscluster.cbegin() + k + 1);//at lease k+1 nodes !
            }


            ret1.push_back(
                    std::make_tuple(i - 1, std::move(from_d_nodes), target_coding_nodeuri, std::move(to_g_nodes)));
            ret2.push_back(
                    std::make_tuple(i - 1, std::move(overlap_nodes), std::move(to_d_nodes))
            );

        }
        return {ret2, ret1};
    }

    std::vector<std::tuple<int, int, std::vector<std::string>, std::string, std::vector<std::string>>>
    FileSystemCN::FileSystemImpl::generate_designed_transition_plan(
            std::unordered_map<int, std::vector<std::string>> &fsimage) {
        std::vector<std::tuple<int, int, std::vector<std::string>, std::string, std::vector<std::string>>> ret;

        auto extractor = [&](const std::vector<std::string> &stripelocs) ->
                std::tuple<std::unordered_set<int>, std::unordered_set<int>, std::unordered_set<int>> {
            std::unordered_set<int> datacluster;
            std::unordered_set<int> globalcluster;
            bool flag = false;// following will be gp cluster
            int i = 0;
            for (; i < stripelocs.size(); i++) {
                if (stripelocs[i] != "d" && stripelocs[i] != "l") {

                    datacluster.insert(m_dn_info[stripelocs[i]].clusterid);
                } else {
                    if (stripelocs[i] == "l") break;
                    i++;
                    continue;
                }
            }
            i++;
            for (; i < stripelocs.size(); ++i) {
                if (stripelocs[i] != "g") {
                    globalcluster.insert(m_dn_info[stripelocs[i]].clusterid);
                } else {
                    break;
                }
            }

            return {datacluster, {}, globalcluster};
        };

        for (int i = 1; i < fsimage.size(); i += 2) {
            auto kpos = std::find(fsimage[i - 1].cbegin(), fsimage[i - 1].cend(), "d");
            auto lpos = std::find(kpos, fsimage[i - 1].cend(), "l");
            auto gpos = std::find(lpos, fsimage[i - 1].cend(), "g");

            int k = kpos - fsimage[i - 1].cbegin();
            int l = lpos - kpos - 1;
            int g = gpos - lpos - 1;
            int shift = l * ceil(log2(g + 1));

            auto [_ignore1, _ignore2, candg] = extractor(fsimage[i - 1]);
            assert(candg.size() == 1);
            auto cand_g_nodes = m_cluster_info[*candg.cbegin()].datanodesuri;
            std::random_shuffle(cand_g_nodes.begin(), cand_g_nodes.end());
            //pick a worker
            auto target_coding_node = cand_g_nodes.front();
            std::vector<std::string> to_g_nodes(cand_g_nodes.begin() + 1, cand_g_nodes.begin() + g);
            std::vector<std::string> from_g_nodes(lpos + 1, gpos);
            from_g_nodes.insert(from_g_nodes.end(), std::find(fsimage[i].cbegin(), fsimage[i].cend(), "l") + 1,
                                std::find(fsimage[i].cbegin(), fsimage[i].cend(), "g"));

            ret.push_back(
                    std::make_tuple(i - 1, shift, std::move(from_g_nodes), target_coding_node,
                                    std::move(to_g_nodes)));
        }

        return ret;
    }

    bool FileSystemCN::FileSystemImpl::delete_global_parity_of(int stripeid) {
        auto it = std::find(m_fs_image[stripeid].cbegin(), m_fs_image[stripeid].cend(), "l");
        ++it;
        grpc::Status status;
        for (; *it != "g"; ++it) {
            grpc::ClientContext deletectx;
            datanode::RequestResult deleteres;
            datanode::StripeId stripeId;
            stripeId.set_stripeid(stripeid);
            std::cout << "delete " << *it << " global parity blocks of stripe : " << stripeid << std::endl;
            status = m_dn_ptrs[*it]->clearstripe(&deletectx, stripeId, &deleteres);
        }
        return true;
    }

    bool FileSystemCN::FileSystemImpl::rename_block_to(int oldstripeid, int newstripeid,
                                                       const std::unordered_set<std::string> &skipset) {
        //rename if exist otherwise nop
        for (const auto &node: m_fs_image[oldstripeid]) {
            if (node == "d" || skipset.contains(node)) continue;
            if (node == "l") break;
            rename_block_to_node(node, oldstripeid, newstripeid);
        }
        return true;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::setplacementpolicy(::grpc::ServerContext *context,
                                                     const ::coordinator::SetPlacementPolicyCMD *request,
                                                     ::coordinator::RequestResult *response) {
        if (coordinator::SetPlacementPolicyCMD_PLACE_RANDOM == request->place()) {
            m_placementpolicy = PLACE::RANDOM_PLACE;
        } else if (coordinator::SetPlacementPolicyCMD_PLACE_COMPACT == request->place()) {
            m_placementpolicy = PLACE::COMPACT_PLACE;
        } else {
            m_placementpolicy = PLACE::SPARSE_PLACE;
        }
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    bool FileSystemCN::FileSystemImpl::refreshfilesystemimagebasic(
            const std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string >> &codingplan,
            const std::tuple<int, std::vector<std::string>, std::vector<std::string >> &migrationplan) {
        //copy stripe datanodes and put stripe+1 datanodes into a set
        //replace stripe+1 nodes set ∩ migration src nodes with migration dst nodes
        //merge stripe+1 set with stripe set
        //set gp nodes with worker node and forwarding dst nodes
        int modified_stripe = std::get<0>(codingplan) + 1;
        int k = std::get<1>(codingplan).size();//new k
        int g = std::get<3>(codingplan).size() + 1;
        int l = k / g;
        const auto &be_migrated = std::get<1>(migrationplan);
        const auto &dst_migrated = std::get<2>(migrationplan);
        const auto &total_node = std::get<1>(codingplan);
        const auto &new_gp_node = std::get<3>(codingplan);
        std::vector<std::string> new_stripelocation(k + 1 + l + 1 + g + 1, "");
        std::unordered_map<std::string, std::string> be_migratedset_to_dst;
        int j = 0;
        for (int i = 0; i < be_migrated.size(); ++i) {
            be_migratedset_to_dst[be_migrated[i]] = dst_migrated[i];
        }
        std::vector<std::string> total_datanodeset;
        for (int i = 0; i < total_node.size() / 2; ++i) {
            new_stripelocation[j++] = total_node[i];
        }
        for (int i = total_node.size() / 2; i < total_node.size(); ++i) {
            //all datanodes in both stripe
            if (be_migratedset_to_dst.contains(total_node[i])) {
                new_stripelocation[j++] = be_migratedset_to_dst[total_node[i]];
            } else {
                new_stripelocation[j++] = total_node[i];
            }
        }
        new_stripelocation[j++] = "d";
        std::vector<std::string> total_lpnodeset;
        for (int i = k / 2 + 1; "l" != m_fs_image[modified_stripe - 1][i]; ++i) {
            //stayed stripe lp
            new_stripelocation[j++] = m_fs_image[modified_stripe - 1][i];
        }
        for (int i = k / 2 + 1; "l" != m_fs_image[modified_stripe][i]; ++i) {
            //stayed stripe lp
            if (!be_migratedset_to_dst.contains(m_fs_image[modified_stripe][i]))
                new_stripelocation[j++] = m_fs_image[modified_stripe][i];
            else new_stripelocation[j++] = be_migratedset_to_dst[m_fs_image[modified_stripe][i]];
        }
        new_stripelocation[j++] = "l";
        new_stripelocation[j++] = std::get<2>(codingplan);
        for (const auto &gp: new_gp_node) {
            new_stripelocation[j++] = gp;
        }
        new_stripelocation[j++] = "g";
        m_fs_image.erase(modified_stripe - 1);
        m_fs_image.erase(modified_stripe);
        m_fs_image.insert({modified_stripe - 1, std::move(new_stripelocation)});
        return true;
    }

    bool FileSystemCN::FileSystemImpl::refreshfilesystemimagebasicpartial(
            const std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>> &codingplan,
            const std::tuple<int, std::vector<std::string>, std::vector<std::string >> &migrationplan) {

    }

    bool FileSystemCN::FileSystemImpl::refreshfilesystemimagedesigned(
            const std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>> &codingplan,
            const std::tuple<int, std::vector<std::string>, std::vector<std::string >> &migrationplan) {
        //simplest
        //merge both stripe datanodes and lp nodes
        //reset gp nodes

        std::vector<std::string> new_stripelocation;
        int modified_stripe = std::get<0>(codingplan) + 1;
        const auto &unmodified_location2 = m_fs_image[modified_stripe];
        const auto &unmodified_location1 = m_fs_image[modified_stripe - 1];

        auto d_marker1 = std::find(unmodified_location1.cbegin(), unmodified_location1.cend(), "d");
        auto l_marker1 = std::find(unmodified_location1.cbegin(), unmodified_location1.cend(), "l");
        auto d_marker2 = std::find(unmodified_location2.cbegin(), unmodified_location2.cend(), "d");
        auto l_marker2 = std::find(unmodified_location2.cbegin(), unmodified_location2.cend(), "l");
        for_each(unmodified_location1.begin(), d_marker1, [&](const std::string &loc) {
            new_stripelocation.push_back(loc);
        });
        for_each(unmodified_location2.begin(), d_marker2, [&](const std::string &loc) {
            new_stripelocation.push_back(loc);
        });
        for_each(d_marker1 + 1, l_marker1, [&](const std::string &loc) {
            new_stripelocation.push_back(loc);
        });
        for_each(d_marker2 + 1, l_marker2, [&](const std::string &loc) {
            new_stripelocation.push_back(loc);
        });

        const auto &forwarding_gp_node = std::get<3>(codingplan);
        new_stripelocation.emplace_back(std::get<2>(codingplan));
        new_stripelocation.insert(new_stripelocation.end(), forwarding_gp_node.cbegin(), forwarding_gp_node.cend());

        m_fs_image.erase(modified_stripe);
        m_fs_image.erase(modified_stripe - 1);

        m_fs_image.insert({modified_stripe - 1, std::move(new_stripelocation)});

        return true;
    }

    FileSystemCN::FileSystemImpl::Transition_Plan
    FileSystemCN::FileSystemImpl::generate_transition_plan(const std::vector<SingleStripeLayout> &layout,
                                                           const std::tuple<int, int, int> &para_before,
                                                           const std::tuple<int, int, int> &para_after,
                                                           int from, int after,
                                                           bool partial_gp,
                                                           bool partial_lp,
                                                           int step) {
        std::vector<Partial_Coding_Plan> p1;
        std::vector<Global_Coding_Plan> p2;
        std::vector<Partial_Coding_Plan> p4;
        auto [k, l, g] = para_before;
        auto [_k, _l, _g] = para_after;
        int r = k / l;
        if (r == g) {
            //shortcircuit partial lp
            partial_lp = false;
        }
        std::unordered_set<int> union_cluster;
        std::unordered_set<int> candgp_cluster;
        std::unordered_set<int> residue_cluster;
        std::unordered_set<int> overlap_cluster;
        std::unordered_set<int> to_cluster;
        std::unordered_set<int> gp_cluster;
        std::unordered_set<int> partial_cluster;

        for (int i = 0; i < m_cluster_info.size(); ++i) {
            candgp_cluster.insert(i);
            to_cluster.insert(i);
        }
        int cluster_cap = 0;
        int residuecluster_cap = 0;
        //clusterid,{dn1,stripe1i,blkid1j},{dn2,stripeid2i,blk2j},...
        std::unordered_map<int, std::unordered_map<std::string, std::tuple<int, int, TYPE>>> cluster_tmp_total;
        std::unordered_map<int, std::unordered_map<std::string, std::tuple<int, int, TYPE>>> residuecluster_tmp_total;
        std::unordered_map<int, std::unordered_map<std::string, std::tuple<int, int, TYPE>>> cluster_tmp_datablock;//differentiate partial decoding or naive decoding




        std::unordered_map<int, std::vector<std::string>> migration_mapping;
        std::vector<int> migration_stripe;
        std::vector<std::string> migration_from;
        std::vector<std::string> migration_to;
        std::unordered_set<std::string> gp_skip;


        if (r <= g) {
            for (int i = 0; i < step; ++i) {
                auto [dataloc, lploc, gploc] = layout[from + i];
                for (const auto &ci: gploc) {
                    const auto &[_dnuri, _stripegrp] = ci;
                    int _clusterid = m_dn_info[_dnuri].clusterid;
                    gp_cluster.insert(_clusterid);
                    candgp_cluster.insert(_clusterid);
                }
            }
            for (int i = 0; i < step; ++i) {
                auto [dataloc, lploc, gploc] = layout[from + i];
                for (const auto &ci: lploc) {
                    const auto &[_dnuri, _stripegrp] = ci;
                    int _clusterid = m_dn_info[_dnuri].clusterid;
                    union_cluster.insert(_clusterid);
                    residue_cluster.insert(_clusterid);
                    to_cluster.erase(_clusterid);
                    candgp_cluster.erase(_clusterid);
                }
            }
            std::unordered_map<int, int> cluster_counted;
            int countdown = _g / g;

            for (int i = 0; i < step; ++i) {
                auto [dataloc, lploc, gploc] = layout[from + i];
                for (const auto &ci: dataloc) {
                    const auto &[_dnuri, _stripegrp] = ci;
                    int _clusterid = m_dn_info[_dnuri].clusterid;
                    union_cluster.insert(_clusterid);
                    candgp_cluster.erase(_clusterid);
                    to_cluster.erase(_clusterid);
                }
            }
            cluster_cap = _g;
            //migration
            for (int i = 0; i < step; ++i) {
                const auto &[dataloc, lploc, gploc] = layout[from + i];
                for (const auto &p: dataloc) {
                    const auto &[_dnuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
                    int ci = m_dn_info[_dnuri].clusterid;
                    if (0 != cluster_tmp_total.count(ci) && cluster_tmp_total[ci].size() < cluster_cap) {
                        cluster_tmp_total[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                    } else if (0 != cluster_tmp_total.count(ci)) {
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        migration_stripe.emplace_back(from + i);
                        migration_from.emplace_back(_dnuri);
                        migration_mapping.emplace(ci, std::vector<std::string>());
                    } else {
                        //unseen cluster
                        cluster_tmp_total.emplace(ci, std::unordered_map<std::string, std::tuple<int, int, TYPE>>());
                        cluster_tmp_datablock.emplace(ci,
                                                      std::unordered_map<std::string, std::tuple<int, int, TYPE>>());
                        cluster_tmp_total[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                    }
                }
            }

            for (int i = 0; !partial_lp && i < step; ++i) {
                const auto &[dataloc, lploc, gploc] = layout[from + i];
                for (const auto &p: lploc) {
                    const auto &[_lpuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
                    int ci = m_dn_info[_lpuri].clusterid;
                    if (0 != cluster_tmp_total.count(ci) && cluster_tmp_datablock[ci].size() > cluster_cap) {
                        if (!cluster_counted.contains(ci)) cluster_counted.emplace(ci, 0);
                        cluster_counted[ci]++;
                        if (cluster_counted[ci] > countdown) {
                            migration_stripe.emplace_back(from + i);
                            migration_from.emplace_back(_lpuri);
                        }
                    }
                }
            }

            //reconsx
            std::vector<int> stripeids, blkids;
            std::vector<std::string> fromdns;
            std::vector<std::string> todns_globalplan;
            //pick gp nodes
            std::vector<int> pickedgpcluster;
            int skiplp = 0;
            for (auto ci: residue_cluster) {
                if (candgp_cluster.contains(ci)) {
                    if (pickedgpcluster.empty()) pickedgpcluster.push_back(ci);
                    skiplp += l;
                }
            }

            if (pickedgpcluster.empty()) {
                std::sample(candgp_cluster.cbegin(), candgp_cluster.cend(), std::back_inserter(pickedgpcluster), 1,
                            std::mt19937{std::random_device{}()});
            }
            to_cluster.erase(pickedgpcluster[0]);
            auto candgps_list = m_cluster_info[pickedgpcluster[0]].datanodesuri;
            std::unordered_set<std::string> candgps;
            for (const auto &uri: candgps_list) {
                if (!gp_skip.contains(uri)) candgps.emplace(uri);
            }
            std::sample(candgps.cbegin(), candgps.cend(), std::back_inserter(todns_globalplan), _g,
                        std::mt19937_64{std::random_device{}()});


            const auto &worker = todns_globalplan[0];

            for (const auto &c: cluster_tmp_datablock) {
                if (partial_gp && c.second.size() > _g) {
                    //need partial
                    for (int j = 0; j < _g; ++j) {
                        for (const auto &clusterdninfo: c.second) {
                            const auto &[_dnuri, _blkinfo] = clusterdninfo;
                            const auto &[_stripeid, _blkid, _type] = _blkinfo;
                            fromdns.emplace_back(_dnuri);
                            stripeids.emplace_back(_stripeid);
                            blkids.emplace_back(_blkid);
                        }
                        auto gatewayuri = m_cluster_info[c.first].gatewayuri;
                        p1.emplace_back(std::make_tuple(stripeids, blkids, fromdns, gatewayuri));
                        //clear
                        fromdns.clear();
                        stripeids.clear();
                        blkids.clear();
                    }
                } else {
                    //or send data blocks to gp cluster gateway as a special partial plan
                    for (const auto &clusterdninfo: c.second) {
                        const auto &[_dnuri, _blkinfo] = clusterdninfo;
                        const auto &[_stripeid, _blkid, _blktype] = _blkinfo;
                        stripeids.push_back(_stripeid);
                        blkids.push_back(_blkid);
                        fromdns.emplace_back(_dnuri);
                        p1.emplace_back(std::make_tuple(stripeids, blkids, fromdns, worker));
                        fromdns.clear();
                        stripeids.clear();
                        blkids.clear();
                    }
                }
            }


            for (int j = 0; j < _g; ++j) {

                for (const auto &c: cluster_tmp_datablock) {
                    if (partial_gp && c.second.size() > _g) {
                        //already partial coded , stored on that cluster's gateway
                        auto gatewayuri = m_cluster_info[c.first].gatewayuri;
                        fromdns.emplace_back(gatewayuri);
                        stripeids.emplace_back(after);//will be replced by new stripeid when executeplan
                        blkids.emplace_back(-j);//will be replced by new gp id when executeplan
                    } else {
                        //or send data blocks to
                        for (const auto &clusterdninfo: c.second) {
                            const auto &[_dnuri, _blkinfo] = clusterdninfo;
                            const auto &[_stripeid, _blkid, _blktype] = _blkinfo;
                            fromdns.emplace_back(worker);//from gp cluster gateway
                            stripeids.emplace_back(_stripeid);//require maintain original info
                            blkids.emplace_back(_blkid);//require maintain original info
                        }

                    };
                }
                p2.emplace_back(std::make_tuple(stripeids, blkids, fromdns, todns_globalplan[j]));
                stripeids.clear();
                blkids.clear();
                fromdns.clear();
            }


            std::sort(migration_from.begin(), migration_from.end());
            auto tocluster_iter = to_cluster.begin();
            std::string *todns_iter = nullptr;
            int tmpc = -1;
            int lastc = -1;
            //just migration
            std::vector<std::string> to_partialfrom;
            for (int j = 0; j < migration_from.size(); ++j) {
                int srcci = m_dn_info[migration_from[j]].clusterid;
                if (partial_lp && 0 != j && 0 == j % r) {
                    const auto &partworker = *todns_iter;
                    ++todns_iter;
                    p4.emplace_back(std::make_tuple(std::vector<int>(to_partialfrom.size(), from),
                                                    std::vector<int>(to_partialfrom.size(), -1), to_partialfrom,
                                                    partworker));
                    to_partialfrom.clear();
                }

                if (srcci != lastc) {
                    todns_iter = nullptr;
                    lastc = srcci;
                    tmpc = *tocluster_iter;
                    ++tocluster_iter;
                }

                if (nullptr == todns_iter) {
                    todns_iter = &m_cluster_info[tmpc].datanodesuri[0];
                }
                migration_to.emplace_back(*todns_iter);
                if (partial_lp) {
                    to_partialfrom.emplace_back(*todns_iter);
                }
                ++todns_iter;
            }
            if (partial_lp && !to_partialfrom.empty()) {
                const auto &partworker = *todns_iter;
                ++todns_iter;
                p4.emplace_back(std::make_tuple(std::vector<int>(to_partialfrom.size(), from),
                                                std::vector<int>(to_partialfrom.size(), -1), to_partialfrom,
                                                partworker));
            }

            Migration_Plan p3 = std::make_tuple(migration_stripe, migration_from, migration_to);
            return {p1, p2, p3, p4};
        } else if (0 == (r % (g + 1))) {

            for (int i = 0; i < step; ++i) {
                auto [dataloc, lploc, gploc] = layout[from + i];
                for (const auto &ci: gploc) {
                    const auto &[_dnuri, _stripegrp] = ci;
                    int _clusterid = m_dn_info[_dnuri].clusterid;
                    gp_cluster.insert(_clusterid);
                    candgp_cluster.insert(_clusterid);
                }
            }
            for (int i = 0; i < step; ++i) {
                auto [dataloc, lploc, gploc] = layout[from + i];
                for (const auto &ci: lploc) {
                    const auto &[_dnuri, _stripegrp] = ci;
                    int _clusterid = m_dn_info[_dnuri].clusterid;
                    union_cluster.insert(_clusterid);
                    residue_cluster.insert(_clusterid);
                    gp_skip.emplace(_dnuri);
//                   to_cluster.erase(_clusterid);
                    candgp_cluster.insert(_clusterid);
                }
            }
            for (int i = 0; i < step; ++i) {
                auto [dataloc, lploc, gploc] = layout[from + i];
                for (const auto &ci: dataloc) {
                    const auto &[_dnuri, _stripegrp] = ci;
                    int _clusterid = m_dn_info[_dnuri].clusterid;
                    union_cluster.insert(_clusterid);
                    candgp_cluster.erase(_clusterid);
                    to_cluster.erase(_clusterid);
                }
            }

            cluster_cap = _g + (_g / g);
            //migration
            for (int i = 0; i < step; ++i) {
                const auto &[dataloc, lploc, gploc] = layout[from + i];
                for (const auto &p: dataloc) {
                    const auto &[_dnuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
                    int ci = m_dn_info[_dnuri].clusterid;
                    if (0 != cluster_tmp_total.count(ci) && cluster_tmp_total[ci].size() < cluster_cap) {
                        cluster_tmp_total[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                    } else if (0 != cluster_tmp_total.count(ci)) {
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        migration_stripe.emplace_back(from + i);
                        migration_from.emplace_back(_dnuri);
                        migration_mapping.emplace(ci, std::vector<std::string>());
                    } else {
                        //unseen cluster
                        cluster_tmp_total.emplace(ci, std::unordered_map<std::string, std::tuple<int, int, TYPE>>());
                        cluster_tmp_datablock.emplace(ci,
                                                      std::unordered_map<std::string, std::tuple<int, int, TYPE>>());
                        cluster_tmp_total[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                    }
                }


            }


            //reconsx
            std::vector<int> stripeids, blkids;
            std::vector<std::string> fromdns;
            std::vector<std::string> todns_globalplan;
            //pick gp nodes
            std::vector<int> pickedgpcluster;
            int skiplp = 0;
            for (auto ci: residue_cluster) {
                if (candgp_cluster.contains(ci)) {
                    if (pickedgpcluster.empty()) pickedgpcluster.push_back(ci);
                    skiplp += l;
                }
            }

            if (pickedgpcluster.empty()) {
                std::sample(candgp_cluster.cbegin(), candgp_cluster.cend(), std::back_inserter(pickedgpcluster), 1,
                            std::mt19937{std::random_device{}()});
            }
            to_cluster.erase(pickedgpcluster[0]);
            auto candgps_list = m_cluster_info[pickedgpcluster[0]].datanodesuri;
            std::unordered_set<std::string> candgps;
            for (const auto &uri: candgps_list) {
                if (!gp_skip.contains(uri)) candgps.emplace(uri);
            }
            std::sample(candgps.cbegin(), candgps.cend(), std::back_inserter(todns_globalplan), _g,
                        std::mt19937_64{std::random_device{}()});


            const auto &worker = todns_globalplan[0];

            for (const auto &c: cluster_tmp_datablock) {
                if (partial_gp && c.second.size() > _g) {
                    //need partial
                    for (int j = 0; j < _g; ++j) {
                        for (const auto &clusterdninfo: c.second) {
                            const auto &[_dnuri, _blkinfo] = clusterdninfo;
                            const auto &[_stripeid, _blkid, _type] = _blkinfo;
                            fromdns.emplace_back(_dnuri);
                            stripeids.emplace_back(_stripeid);
                            blkids.emplace_back(_blkid);
                        }
                        auto gatewayuri = m_cluster_info[c.first].gatewayuri;
                        p1.emplace_back(std::make_tuple(stripeids, blkids, fromdns, gatewayuri));
                        //clear
                        fromdns.clear();
                        stripeids.clear();
                        blkids.clear();
                    }
                } else {
                    //or send data blocks to gp cluster gateway as a special partial plan
                    for (const auto &clusterdninfo: c.second) {
                        const auto &[_dnuri, _blkinfo] = clusterdninfo;
                        const auto &[_stripeid, _blkid, _blktype] = _blkinfo;
                        stripeids.push_back(_stripeid);
                        blkids.push_back(_blkid);
                        fromdns.emplace_back(_dnuri);
                        p1.emplace_back(std::make_tuple(stripeids, blkids, fromdns, worker));
                        fromdns.clear();
                        stripeids.clear();
                        blkids.clear();
                    }
                }
            }


            for (int j = 0; j < _g; ++j) {

                for (const auto &c: cluster_tmp_datablock) {
                    if (partial_gp && c.second.size() > _g) {
                        //already partial coded , stored on that cluster's gateway
                        auto gatewayuri = m_cluster_info[c.first].gatewayuri;
                        fromdns.emplace_back(gatewayuri);
                        stripeids.emplace_back(after);//will be replced by new stripeid when executeplan
                        blkids.emplace_back(-j);//will be replced by new gp id when executeplan
                    } else {
                        //or send data blocks to
                        for (const auto &clusterdninfo: c.second) {
                            const auto &[_dnuri, _blkinfo] = clusterdninfo;
                            const auto &[_stripeid, _blkid, _blktype] = _blkinfo;
                            fromdns.emplace_back(worker);//from gp cluster gateway
                            stripeids.emplace_back(_stripeid);//require maintain original info
                            blkids.emplace_back(_blkid);//require maintain original info
                        }

                    };
                }
                p2.emplace_back(std::make_tuple(stripeids, blkids, fromdns, todns_globalplan[j]));
                stripeids.clear();
                blkids.clear();
                fromdns.clear();
            }


            auto tocluster_iter = to_cluster.begin();
            std::string *todns_iter = nullptr;
            int tmpc = -1;
            int lastc = -1;
            //just migration
            //sort uri by clusterid default behaviour
            std::sort(migration_from.begin(), migration_from.end());
            for (int j = 0; j < migration_from.size(); ++j) {
                int srcci = m_dn_info[migration_from[j]].clusterid;
                if (srcci != lastc) {
                    todns_iter = nullptr;
                    lastc = srcci;
                    tmpc = *tocluster_iter;
                    ++tocluster_iter;
                }

                if (nullptr == todns_iter) {
                    todns_iter = &m_cluster_info[tmpc].datanodesuri[0];
                }
                migration_to.emplace_back(*todns_iter);
                ++todns_iter;
            }


            Migration_Plan p3 = std::make_tuple(migration_stripe, migration_from, migration_to);
            return {p1, p2, p3, p4};
        } else {
            for (int i = 0; i < step; ++i) {
                auto [dataloc, lploc, gploc] = layout[from + i];
                for (const auto &ci: gploc) {
                    const auto &[_dnuri, _stripegrp] = ci;
                    int _clusterid = m_dn_info[_dnuri].clusterid;
                    gp_cluster.insert(_clusterid);
                    candgp_cluster.insert(_clusterid);
                }
            }
            for (int i = 0; i < step; ++i) {
                auto [dataloc, lploc, gploc] = layout[from + i];
                for (const auto &ci: lploc) {
                    const auto &[_dnuri, _stripegrp] = ci;
                    int _clusterid = m_dn_info[_dnuri].clusterid;
                    union_cluster.insert(_clusterid);
                    residue_cluster.insert(_clusterid);
                    to_cluster.erase(_clusterid);
                    candgp_cluster.erase(_clusterid);
                }
            }
            for (int i = 0; i < step; ++i) {
                auto [dataloc, lploc, gploc] = layout[from + i];
                for (const auto &ci: dataloc) {
                    const auto &[_dnuri, _stripegrp] = ci;
                    int _clusterid = m_dn_info[_dnuri].clusterid;
                    union_cluster.insert(_clusterid);
                    candgp_cluster.erase(_clusterid);
                    to_cluster.erase(_clusterid);
                }
            }


            cluster_cap = _g + (_g / g);
            int m = (k / l) % (g + 1);//residue_datablock per group
            residuecluster_cap = (_g / m) * (m + 1);//count lp in
            int mix_cap = _g + 1 + 1 * (g / m);//_g+1 data blocks plus at most g/m lps
            //dddd
            //dl
            //dl
            //pick up residue cluster id
            std::unordered_map<int, std::vector<int>> cluster_precount;
            for (auto ci: union_cluster) {
                cluster_precount.insert({ci, std::vector<int>(step, 0)});
            }
            for (int i = 0; i < step; ++i) {
                const auto &[dataloc, lploc, gploc] = layout[from + i];
                for (const auto &p: dataloc) {
                    const auto &[_dnuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
                    int ci = m_dn_info[_dnuri].clusterid;
                    cluster_precount[ci][i]++;
                }
                for (const auto &p: lploc) {
                    const auto &[_dnuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
                    int ci = m_dn_info[_dnuri].clusterid;
                    cluster_precount[ci][i]++;
                }
            }

            std::unordered_map<int, std::unordered_set<int>> special_cluster_mover;
            std::unordered_set<int> mixpack_cluster;
            for (const auto &stat: cluster_precount) {
                const auto &[c, countlist] = stat;
                std::set<int> dedup(countlist.cbegin(), countlist.cend());
                if (dedup.size() > 1) {
                    //mixed
                    mixpack_cluster.insert(c);
                }
                int cap = *max_element(countlist.cbegin(), countlist.cend());
                if (accumulate(countlist.cbegin(), countlist.cend(), 0) <= (mix_cap)) {
                    continue;
                }
                if (residue_cluster.contains(c)) {
                    if (cap > g + 1) {
                        if (!dedup.contains(g + 1))// ==2 but all residue
                        {
                            continue;
                        } else {
                            // ==2 one normal one residue , move normal
                            if (!special_cluster_mover.contains(c)) {
                                special_cluster_mover.insert(
                                        {c, std::unordered_set<int>{}});
                            }
                            for (int i = 0; i < step; ++i) {
                                if (countlist[i] == g + 1) {
                                    special_cluster_mover[c].insert(i);
                                }
                            }
                        }
                    } else if (cap == (g + 1)) {
                        if (!special_cluster_mover.contains(c)) {
                            special_cluster_mover.insert(
                                    {c, std::unordered_set<int>{}});
                        }
                        for (int i = 0; i < step; ++i) {
                            if (countlist[i] != g + 1) {
                                special_cluster_mover[c].insert(i);
                            }
                        }
                    } else {
                        // can be concat
                    }
                }

            }

            for (int i = 0; i < step; ++i) {
                const auto &[dataloc, lploc, gploc] = layout[from + i];
                for (const auto &p: dataloc) {
                    const auto &[_dnuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
                    int ci = m_dn_info[_dnuri].clusterid;
                    if (0 != cluster_tmp_total.count(ci)) {
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        if (special_cluster_mover.contains(ci) && special_cluster_mover[ci].contains(i)) {
                            migration_stripe.push_back(from + i);
                            migration_from.emplace_back(_dnuri);
                            migration_mapping.emplace(ci, std::vector<std::string>());
                            continue;
                        }
                        int cap = (0 == residue_cluster.count(ci)) ? cluster_cap : residuecluster_cap;
                        if (mixpack_cluster.contains(ci)) cap = mix_cap;
                        if (cluster_tmp_total[ci].size() < cap) {
                            cluster_tmp_total[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        } else {
                            migration_stripe.push_back(from + i);
                            migration_from.emplace_back(_dnuri);
                        }
                    } else {
                        cluster_tmp_total.emplace(ci, std::unordered_map<std::string, std::tuple<int, int, TYPE>>());
                        cluster_tmp_datablock.emplace(ci,
                                                      std::unordered_map<std::string, std::tuple<int, int, TYPE>>());
                        cluster_tmp_total[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        if (special_cluster_mover.contains(ci) && special_cluster_mover[ci].contains(i)) {
                            migration_stripe.push_back(from + i);
                            migration_from.emplace_back(_dnuri);
                            migration_mapping.emplace(ci, std::vector<std::string>());
                            continue;
                        }
                    }
                }


                for (const auto &p: lploc) {
                    const auto &[_lpuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _blksz, _flag] = _stripegrp;
                    int ci = m_dn_info[_lpuri].clusterid;
                    if (special_cluster_mover.contains(ci) && special_cluster_mover[ci].contains(i)) {
                        migration_stripe.push_back(from + i);
                        migration_from.emplace_back(_lpuri);
                        migration_mapping.emplace(ci, std::vector<std::string>());
                        continue;
                    }
                    int cap = (0 == residue_cluster.count(ci)) ? cluster_cap : residuecluster_cap;
                    if (mixpack_cluster.contains(ci)) cap = mix_cap;
                    if (cluster_tmp_total[ci].size() < cap) {
                        cluster_tmp_total[ci].emplace(_lpuri, std::make_tuple(from + i, _blkid, TYPE::LP));
                    } else {
                        migration_stripe.push_back(from + i);
                        migration_from.emplace_back(_lpuri);
                    }
                }

            }
            //reconsx
            std::vector<int> stripeids, blkids;
            std::vector<std::string> fromdns;
            std::vector<std::string> todns_globalplan;
            //pick gp nodes
            int pickedgpcluster = *candgp_cluster.cbegin();
            to_cluster.erase(pickedgpcluster);
            auto candgps = m_cluster_info[pickedgpcluster].datanodesuri;
            std::sample(candgps.cbegin(), candgps.cend(), std::back_inserter(todns_globalplan), _g,
                        std::mt19937{std::random_device{}()});


            const auto &worker = todns_globalplan[0];

            for (const auto &c: cluster_tmp_datablock) {
                if (partial_gp && c.second.size() > _g) {
                    //need partial
                    for (int j = 0; j < _g; ++j) {
                        for (const auto &clusterdninfo: c.second) {
                            const auto &[_dnuri, _blkinfo] = clusterdninfo;
                            const auto &[_stripeid, _blkid, _type] = _blkinfo;
                            fromdns.emplace_back(_dnuri);
                            stripeids.emplace_back(_stripeid);
                            blkids.emplace_back(_blkid);
                        }
                        auto gatewayuri = m_cluster_info[c.first].gatewayuri;
                        p1.emplace_back(std::make_tuple(stripeids, blkids, fromdns, gatewayuri));
                        //clear
                        fromdns.clear();
                        stripeids.clear();
                        blkids.clear();
                    }
                } else {
                    //or send data blocks to gp cluster gateway as a special partial plan
                    for (const auto &clusterdninfo: c.second) {
                        const auto &[_dnuri, _blkinfo] = clusterdninfo;
                        const auto &[_stripeid, _blkid, _blktype] = _blkinfo;
                        stripeids.push_back(_stripeid);
                        blkids.push_back(_blkid);
                        fromdns.emplace_back(_dnuri);
                        p1.emplace_back(std::make_tuple(stripeids, blkids, fromdns, worker));
                        fromdns.clear();
                        stripeids.clear();
                        blkids.clear();
                    }
                }
            }


            for (int j = 0; j < _g; ++j) {

                for (const auto &c: cluster_tmp_datablock) {
                    if (partial_gp && c.second.size() > _g) {
                        //already partial coded , stored on that cluster's gateway
                        auto gatewayuri = m_cluster_info[c.first].gatewayuri;
                        fromdns.emplace_back(gatewayuri);
                        stripeids.emplace_back(after);
                        blkids.emplace_back(-j);
                    } else {
                        //or send data blocks to
                        for (const auto &clusterdninfo: c.second) {
                            const auto &[_dnuri, _blkinfo] = clusterdninfo;
                            const auto &[_stripeid, _blkid, _blktype] = _blkinfo;
                            fromdns.emplace_back(worker);//from gp cluster gateway
                            stripeids.emplace_back(_stripeid);
                            blkids.emplace_back(_blkid);
                        }

                    };
                }
                p2.emplace_back(std::make_tuple(stripeids, blkids, fromdns, todns_globalplan[j]));
                stripeids.clear();
                blkids.clear();
                fromdns.clear();
            }
        }

        std::sort(migration_from.begin(), migration_from.end());
        auto tocluster_iter = to_cluster.begin();
        std::string *todns_iter = nullptr;
        int tmpc = -1;
        int lastc = -1;
        //just migration
        for (int j = 0; j < migration_from.size(); ++j) {
            int srcci = m_dn_info[migration_from[j]].clusterid;
            if (srcci != lastc) {
                todns_iter = nullptr;
                lastc = srcci;
                tmpc = *tocluster_iter;
                ++tocluster_iter;
            }

            if (nullptr == todns_iter) {
                todns_iter = &m_cluster_info[tmpc].datanodesuri[0];
            }
            migration_to.emplace_back(*todns_iter);
            ++todns_iter;
        }


        Migration_Plan p3 = std::make_tuple(migration_stripe, migration_from, migration_to);
        return {p1, p2, p3, p4};

    }

    FileSystemCN::FileSystemImpl::StripeImage
    FileSystemCN::FileSystemImpl::executetransitionplan(Transition_Plan transitionPlan,
                                                        const std::vector<SingleStripeLayout> &layout,
                                                        int from, int to, int after,
                                                        std::tuple<int, int, int> para_before,
                                                        std::tuple<int, int, int> para_after,
                                                        bool partialgp,
                                                        bool partiallp) {

        //mutexes are obtained
        //...
        std::unordered_map<std::string, int> image_map_data;
        int idx_data = 0;
        std::unordered_map<std::string, int> image_map_lp;
        int idx_lp = 0;
        std::unordered_map<std::string, int> image_map_gp;
        int idx_gp = 0;
        //record migration mapping
        std::unordered_map<std::string, int> rename_records;//{fromuri:{fromstpid}}
        std::unordered_map<std::string, int> deleteblk_records;//{fromuri:{fromstpid}}
        std::unordered_set<std::string> deletepartdir_records;//gateways
        for (int i = from; i < to; ++i) {
            const auto &uris = m_fs_image[i];
            int j = 0;
            for (; uris[j] != "d"; ++j) {
                rename_records.emplace(uris[j], i);
                image_map_data.emplace(uris[j], idx_data);
                idx_data++;
            }
            j++;
            for (; uris[j] != "l"; ++j) {
                rename_records.emplace(uris[j], i);
                image_map_lp.emplace(uris[j], idx_lp);
                idx_lp++;
            }
            j++;
            //first delete orginal gps
            for (; uris[j] != "g"; j++)
                delete_block_of_node(uris[j], i, TYPE::GP);
        }

        auto [k, l, g] = para_before;
        auto [_k, _l, _g] = para_after;
        StripeImage ret = std::make_pair(after, std::vector<std::string>(_k + _l + _g + 3, ""));
        int r = k / l;
        const auto &[localpartialplan_before, globalpartialplan, migrationplan, localpartialplan_after] = transitionPlan;

        //perform local partial coding ,results are stored in corresponding cluster gateway node
        //source node handledownload from gateway
        //localplan may dup _g times

        for (const auto &ci: localpartialplan_before) {
            //each partial coding cluster
            int targetstpid_beforepart = after;
            int targetblkid_beforepart = -_g;
            const auto &[stpids, blkids, fromuris, workeruri] = ci;
            int currentclusterid = m_dn_info[fromuris[0]].clusterid;
            if (fromuris.size() == 1) {
                //original data not a patrtial coded block
                targetstpid_beforepart = stpids[0];
                targetblkid_beforepart = blkids[0];
            }
            std::cout << "cluster " << currentclusterid << "perform before partial coding task\n";
            std::cout << "  worker " << workeruri << std::endl;
            //fromuris handledownload from worker
            grpc::ClientContext dopartctx;
            datanode::NodesLocation partnodesloc;
            partnodesloc.set_aspart(true);
            deletepartdir_records.emplace(workeruri);
            for (int i = 0; i < fromuris.size(); ++i) {
                if (!askDNhandling(fromuris[i], stpids[i], false, false)) {
                    std::cout << "source node " << fromuris[i] << " can not handle block" << stpids[i] << std::endl;
                    return ret;
                }
                partnodesloc.add_nodesuri(fromuris[i]);
                partnodesloc.add_nodesstripeid(stpids[i]);
                partnodesloc.add_nodesblkid(blkids[i]);
                partnodesloc.add_ispart(false);
            }
            partnodesloc.set_targetstripeid(targetstpid_beforepart);
            partnodesloc.set_targetblks(targetblkid_beforepart);
            //worker perform dopartialcoding
            datanode::RequestResult dopartres;
            m_dn_ptrs[workeruri]->dopartialcoding(&dopartctx, partnodesloc, &dopartres);


        }

        //for each new gp node
        for (int i = 0; i < globalpartialplan.size(); ++i) {
            const auto &[stpids, blkids, fromuris, workeruri] = globalpartialplan[i];
            std::cout << "  worker " << workeruri << " perfrom global coding task " << std::endl;
            grpc::ClientContext dopartctx;
            datanode::NodesLocation partnodesloc;
            partnodesloc.set_aspart(false);
            for (int j = 0; j < fromuris.size(); ++j) {
                if (!askDNhandling(fromuris[j], stpids[j], false, true)) {
                    std::cout << "source node " << fromuris[j] << " can not handle block" << stpids[j] << std::endl;
                    return ret;
                }
                partnodesloc.add_nodesuri(fromuris[j]);
                partnodesloc.add_nodesstripeid(stpids[j]);
                if (blkids[j] <= 0) partnodesloc.add_nodesblkid(-blkids[j]);
                else partnodesloc.add_nodesblkid(blkids[j]);
                partnodesloc.add_ispart(true);
            }
            partnodesloc.set_targetstripeid(after);
            partnodesloc.set_targetblks(1);
            //worker perform dopartialcoding
            datanode::RequestResult dopartres;
            m_dn_ptrs[workeruri]->dopartialcoding(&dopartctx, partnodesloc, &dopartres);
            image_map_gp.emplace(workeruri, idx_gp++);
        }

        //migration
        //r<=g: delete corresponding lp node
        const auto &[stpids, fromuris, touris] = migrationplan;
        for (int i = 0; i < fromuris.size(); ++i) {
            if (!askDNhandling(fromuris[i], stpids[i], false, false)) {
                std::cout << "source node " << fromuris[i] << " can not handle block" << stpids[i] << std::endl;
                return ret;
            }

//            ask touri perform downloading
            grpc::ClientContext dodownloadctx;
            datanode::DodownloadCMD dodownloadCmd;
            datanode::RequestResult dodownloadres;
            dodownloadCmd.set_aspart(false);
            dodownloadCmd.set_ispart(false);
            dodownloadCmd.set_nodesuri(fromuris[i]);
            dodownloadCmd.set_nodesstripeid(stpids[i]);
            dodownloadCmd.set_targetstripeid(after);
            dodownloadCmd.set_targetblks(1);
            grpc::Status status;
            status = m_dn_ptrs[touris[i]]->dodownload(&dodownloadctx, dodownloadCmd, &dodownloadres);
            deleteblk_records.emplace(fromuris[i], stpids[i]);
            if (image_map_data.contains(fromuris[i])) {
                auto idx = image_map_data[fromuris[i]];
                image_map_data.emplace(touris[i], idx);
                image_map_data.erase(fromuris[i]);

            }
            if (image_map_lp.contains(fromuris[i])) {
                auto idx = image_map_lp[fromuris[i]];
                image_map_lp.emplace(touris[i], idx);
                image_map_lp.erase(fromuris[i]);
            }
        }

        //perform partial_after
        for (int i = 0; i < localpartialplan_after.size(); ++i) {
            //each partial coding cluster
            const auto &[stpids, blkids, fromuris, workeruri] = localpartialplan_after[i];
            int currentclusterid = m_dn_info[fromuris[0]].clusterid;
            std::cout << "cluster " << currentclusterid << "perform after partial coding task\n";
            std::cout << "  worker " << workeruri << std::endl;

            //fromuris handledownload from worker
            grpc::ClientContext dopartctx;
            datanode::NodesLocation partnodesloc;
            partnodesloc.set_aspart(false);
            int after_fromdataidx = -1;
            for (int j = 0; j < fromuris.size(); ++j) {
                if (!askDNhandling(fromuris[j], stpids[j], false, false)) {
                    std::cout << "source node " << fromuris[j] << " can not handle block" << stpids[j] << std::endl;
                    return ret;
                }
                if (after_fromdataidx == -1) after_fromdataidx = image_map_data[fromuris[j]];
                partnodesloc.add_nodesuri(fromuris[j]);
                partnodesloc.add_nodesstripeid(after);
                partnodesloc.add_nodesblkid(blkids[j]);
                partnodesloc.add_ispart(false);
            }
            partnodesloc.set_targetstripeid(after);
            partnodesloc.set_targetblks(1);
            //worker perform dopartialcoding
            datanode::RequestResult dopartres;
            m_dn_ptrs[workeruri]->dopartialcoding(&dopartctx, partnodesloc, &dopartres);
            int after_tolpidx = after_fromdataidx /
                                r;// r<=g : used to distingush after partial lp node from before p4 partial lp node ...
        }

        //delete[clear] migrated blocks
        for (const auto &del: deleteblk_records) {
            delete_block_of_node(del.first, del.second, TYPE::DATA);//any type is ok
        }
        //delete[clear] partial coded dir
        for (const auto &del: deletepartdir_records) {
            grpc::ClientContext delpartdirctx;
            datanode::StripeId delstpid;
            delstpid.set_stripeid(-1);
            datanode::RequestResult deldirres;
            auto status = m_dn_ptrs[del]->clearstripe(&delpartdirctx, delstpid, &deldirres);
        }
        //rename all involved blocks
        for (const auto &ren: rename_records) {
            rename_block_to_node(ren.first, ren.second, after);
        }
        //construct and return new image vector
        auto &image_vector = ret.second;
        for (const auto &d: image_map_data) {
            const auto &[uri, idx] = d;
            image_vector[idx] = uri;
        }
        image_vector[_k] = "d";
        for (const auto &l: image_map_lp) {
            const auto &[uri, idx] = l;
            image_vector[_k + 1 + idx] = uri;
        }
        image_vector[_k + _l + 1] = "l";
        for (const auto &g: image_map_gp) {
            const auto &[uri, idx] = g;
            image_vector[_k + _l + 2 + idx] = uri;
        }
        image_vector[_k + _l + _g + 2] = "g";
        return ret;
    }

    bool FileSystemCN::FileSystemImpl::delete_block_of_node(const std::string &uri, int stripeid,
                                                            FileSystemCN::FileSystemImpl::TYPE tp) {
        grpc::ClientContext deletectx;
        datanode::RequestResult deleteres;
        datanode::StripeId stripeId;
        grpc::Status status;
        stripeId.set_stripeid(stripeid);
        std::cout << "delete " << uri << " blocks of stripe : " << stripeid << std::endl;
        status = m_dn_ptrs[uri]->clearstripe(&deletectx, stripeId, &deleteres);
        return true;
    }

    bool FileSystemCN::FileSystemImpl::rename_block_to_node(const std::string &node, int oldstripeid, int newstripeid) {
        std::cout << "rename " << node << " old stripe : " << oldstripeid << " to new stripe : " << newstripeid
                  << std::endl;
        grpc::ClientContext renamectx;
        datanode::RenameCMD renameCMD;
        renameCMD.set_oldid(oldstripeid);
        renameCMD.set_newid(newstripeid);
        datanode::RequestResult renameres;
        m_dn_ptrs[node]->renameblock(&renamectx, renameCMD, &renameres);
        return true;
    }

    bool FileSystemCN::FileSystemImpl::delete_block_of(int stripeid, FileSystemCN::FileSystemImpl::TYPE tp) {
        auto it_data = std::find(m_fs_image[stripeid].cbegin(), m_fs_image[stripeid].cend(), "d") + 1;
        auto it_lp = std::find(m_fs_image[stripeid].cbegin(), m_fs_image[stripeid].cend(), "l") + 1;
        auto it = m_fs_image[stripeid].cbegin();
        grpc::Status status;
        if (tp == TYPE::DATA) {
            for (it; *it != "d"; ++it) {
                delete_block_of_node(*it, stripeid, tp);
            }
        } else if (tp == TYPE::LP) {
            for (it = it_data; *it != "l"; ++it) {
                delete_block_of_node(*it, stripeid, tp);
            }
        } else {
            for (it = it_lp; *it != "g"; ++it) {
                delete_block_of_node(*it, stripeid, tp);
            }
        }
        return true;
    }


//CN in DN implementation
/*
grpc::Status FileSystemCN::CoordinatorImpl::reportblocktransfer(::grpc::ServerContext *context,
                                                                const ::coordinator::StripeId *request,
                                                                ::coordinator::RequestResult *response) {
    m_fsimpl_ptr->getMCnLogger()->info("datanode {} receive block of stripe {} from client!",context->peer(),request->stripeid());
    m_fsimpl_ptr->updatestripeupdatingcounter(request->stripeid(),context->peer());//should be a synchronous method or atomic int;
    response->set_trueorfalse(true);
    return grpc::Status::OK;
}*/
    FileSystemCN::CoordinatorImpl::CoordinatorImpl() {}

    const std::shared_ptr<FileSystemCN::FileSystemImpl> &FileSystemCN::CoordinatorImpl::getMFsimplPtr() const {
        return m_fsimpl_ptr;
    }

    void FileSystemCN::CoordinatorImpl::setMFsimplPtr(const std::shared_ptr<FileSystemImpl> &mFsimplPtr) {
        m_fsimpl_ptr = mFsimplPtr;
    }

}