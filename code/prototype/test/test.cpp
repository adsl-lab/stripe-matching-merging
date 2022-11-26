//
// Created by 杜清鹏 on 2021/4/8.
//

#include <random>
#include <set>
#include <bitset>
#include "combination_generator.h"
#include "devcommon.h"
#include "MetaInfo.h"
#include "erasurecoding/LRCCoder.h"

using namespace std;

std::pair<std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string >>>,
        std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>>
generate_basic_transition_plan(std::unordered_map<int, lrc::ClusterInfo> m_cluster_info,
                               std::unordered_map<std::string, lrc::DataNodeInfo> m_dn_info,
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
        auto[excluded, _ignore1, candg]=extractor(fsimage[i - 1]);
        auto[currentk, _ignore2, currentg]=extractor(fsimage[i]);
        std::vector<int> candcluster;//cand data cluster
        std::vector<int> overlap;
        std::vector<std::string> overlap_d_nodes;
        std::vector<std::string> overlap_l_nodes;
        std::vector<std::string> to_d_nodes;
        int u = 0;
        std::unordered_set<int> consideronce;
        for (; u < k; ++u) {
            int c = m_dn_info[fsimage[i][u]].clusterid;
            if (excluded.contains(c)) {
                if(!consideronce.contains(c))
                {
                    overlap.push_back(c);
                    consideronce.insert(c);
                }
                overlap_d_nodes.push_back(fsimage[i][u]);
            }
        }
        ++u;
        for (; fsimage[i][u] != "l"; ++u) {
            int c = m_dn_info[fsimage[i][u]].clusterid;
            if (excluded.contains(c)) {
                if(!consideronce.contains(c)){
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
        std::vector<std::string> to_g_nodes(target_g_cands.cbegin() + 1, target_g_cands.cbegin()+g);
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
                              thiscluster.cbegin() + k+1 );//at lease k+1 nodes !
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
generate_designed_transition_plan(std::unordered_map<int, lrc::ClusterInfo> m_cluster_info,
                                  std::unordered_map<std::string, lrc::DataNodeInfo> m_dn_info,
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

    for (int i = 1; i < fsimage.size(); i+=2) {
        auto kpos = std::find(fsimage[i - 1].cbegin(), fsimage[i - 1].cend(), "d");
        auto lpos = std::find(kpos, fsimage[i - 1].cend(), "l");
        auto gpos = std::find(lpos, fsimage[i - 1].cend(), "g");

        int k = kpos - fsimage[i - 1].cbegin();
        int l = lpos - kpos - 1;
        int g = gpos - lpos - 1;
        int shift = l * ceil(log2(g + 1));

        auto[_ignore1, _ignore2, candg] =extractor(fsimage[i - 1]);
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
                std::make_tuple(i - 1, shift, std::move(from_g_nodes), target_coding_node, std::move(to_g_nodes)));
    }

    return ret;
}

std::vector<std::unordered_map<std::string, std::pair<int, bool>>>
placement_resolve(std::unordered_map<int, lrc::ClusterInfo> m_cluster_info, int k, int l, int g,
                  bool designed_placement = false) {

    static std::vector<std::unordered_set<int>> stripe_local_group_history;
    static int stripe_global_history = -1;

    std::vector<std::unordered_map<std::string, std::pair<int, bool>>> ret(3,
                                                                           std::unordered_map<std::string, std::pair<int, bool>>());
    std::vector<int> seq(m_cluster_info.size());

    if (stripe_local_group_history.empty()) {
        std::set<int> current_stripe_clusters;
        allrandom:
        //random
        //only maintain one cluster one local group
        int total_cluster = m_cluster_info.size();
        int local_group = l;

        if (total_cluster < local_group) {
            return ret;
        }
        //pick l cluster to hold
        std::iota(seq.begin(), seq.end(), 0);
        std::shuffle(seq.begin(), seq.end(), std::mt19937(std::random_device()()));
    } else {
        std::unordered_set<int> black_list;
        for (auto s : stripe_local_group_history) {
            black_list.merge(s);
        }
        black_list.insert(stripe_global_history);

        seq.clear();
        int candidate = 0;
        while (candidate < m_cluster_info.size() && seq.size() < l) {
            while (black_list.contains(candidate) && candidate++);//short circuit evaluation
            seq.push_back(candidate);
        }


        std::shuffle(seq.begin(), seq.end(), std::mt19937(std::random_device()()));

        seq.push_back(stripe_global_history);
    }

    for (int i = 0; i < l; ++i) {
        int cluster_dn_num = m_cluster_info[seq[i]].datanodesuri.size();
        assert(cluster_dn_num > g);
        std::vector<int> dns(cluster_dn_num);
        std::iota(dns.begin(), dns.end(), 0);
        std::shuffle(dns.begin(), dns.end(), std::mt19937(std::random_device()()));
        for (int j = 0; j < g; ++j) {
            ret[0].insert({m_cluster_info[seq[i]].datanodesuri[dns[j]], {0, false}});
        }
        ret[1].insert({m_cluster_info[seq[i]].datanodesuri[dns[g]], {1, false}});
    }
    std::vector<int> global_parities(m_cluster_info[seq[l]].datanodesuri.size());
    std::iota(global_parities.begin(), global_parities.end(), 0);
    std::shuffle(global_parities.begin(), global_parities.end(), std::mt19937(std::random_device()()));
    assert(global_parities.size() >= g);
    for (int j = 0; j < g; ++j) {
        ret[2].insert(
                {m_cluster_info[seq[l]].datanodesuri[global_parities[j]], {2, false}});
    }

    if (stripe_local_group_history.empty()) {
        stripe_local_group_history.emplace_back(seq.begin(), seq.begin() + l);
        stripe_global_history = seq[l];
    } else {
        // if 2-merge pattern , then stripe history size up to 1
        stripe_local_group_history.clear();
        stripe_global_history = -1;
    }

    return ret;
}


std::unordered_map<int, std::vector<std::string>> m_fs_image{
        {0,{"192.168.1.3:10002","192.168.1.3:10003","d","192.168.1.3:10001","l","192.168.0.22:10002","192.168.0.22:10001","g"}},
        {1,{"192.168.0.11:10003","192.168.0.11:10001","d","192.168.0.11:10002","l","192.168.0.21:10001","192.168.0.21:10003","g"}},
        {2,{"192.168.0.11:10002","192.168.0.11:10001","d","192.168.0.11:10003","l","192.168.0.22:10002","192.168.0.22:10003","g"}},
        {3,{"192.168.0.22:10001","192.168.0.22:10003","d","192.168.0.22:10001","l","192.168.0.11:10001","192.168.0.11:10003","g"}},
        {4,{"192.168.0.21:10001","192.168.0.21:10002","d","192.168.0.21:10003","l","192.168.1.3:10001","192.168.1.3:10002","g"}},
        {5,{"192.168.0.21:10001","192.168.0.21:10002","d","192.168.0.21:10003","l","192.168.1.3:10001","192.168.1.3:10002","g"}},
        {6,{"192.168.1.3:10003","192.168.1.3:10002","d","192.168.1.3:10001","l","192.168.0.21:10001","192.168.0.21:10002","g"}},
        {7,{"192.168.0.22:10001","192.168.0.22:10003","d","192.168.0.22:10002","l","192.168.0.11:10002","192.168.0.11:10001","g"}}
};

bool refreshfilesystemimagebasic(
        const std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string >> &codingplan,
        const std::tuple<int, std::vector<std::string>, std::vector<std::string >> &migrationplan) {
    //copy stripe datanodes and put stripe+1 datanodes into a set
    //replace stripe+1 nodes set ∩ migration src nodes with migration dst nodes
    //merge stripe+1 set with stripe set
    //set gp nodes with worker node and forwarding dst nodes
    int modified_stripe = std::get<0>(codingplan)+1;
    int k = std::get<1>(codingplan).size();//new k
    int g = std::get<3>(codingplan).size() + 1;
    int l = k / g;
    const auto & be_migrated = std::get<1>(migrationplan);
    const auto & dst_migrated = std::get<2>(migrationplan);
    const auto & total_node = std::get<1>(codingplan);
    const auto & new_gp_node = std::get<3>(codingplan);
    std::vector<std::string> new_stripelocation(k+1+l+1+g+1,"");
    std::unordered_map<std::string,std::string> be_migratedset_to_dst;
    int j=0;
    for(int i=0;i<be_migrated.size();++i)
    {
        be_migratedset_to_dst[be_migrated[i]]=dst_migrated[i];
    }
    std::vector<std::string> total_datanodeset;
    for(const auto & node:total_node){
        //all datanodes in both stripe
        if(be_migratedset_to_dst.contains(node)){
            new_stripelocation[j++]=be_migratedset_to_dst[node];
        }else{
            new_stripelocation[j++]=node;
        }
    }
    new_stripelocation[j++]="d";
    std::vector<std::string> total_lpnodeset;
    for(int i=k/2+1;"l"!=m_fs_image[modified_stripe-1][i];++i){
        //stayed stripe lp
        new_stripelocation[j++]=m_fs_image[modified_stripe-1][i];
    }
    for(int i=k/2+1;"l"!=m_fs_image[modified_stripe][i];++i){
        //stayed stripe lp
        if(!be_migratedset_to_dst.contains(m_fs_image[modified_stripe][i])) new_stripelocation[j++]=m_fs_image[modified_stripe][i];
        else new_stripelocation[j++]=m_fs_image[modified_stripe][i];
    }
    new_stripelocation[j++]="l";
    new_stripelocation[j++]=std::get<2>(codingplan);
    for(const auto & gp:new_gp_node)
    {
        new_stripelocation[j++]=gp;
    }
    new_stripelocation[j++]="g";
    m_fs_image.erase(modified_stripe-1);
    m_fs_image.erase(modified_stripe);
    m_fs_image.insert({modified_stripe-1,std::move(new_stripelocation)});
    return true;
}




int main() {

    std::vector<std::string> v1{"192.168.1.3:10001", "192.168.1.3:10002", "192.168.1.3:10003", "192.168.1.3:10004"};
    lrc::ClusterInfo c1(v1, std::string{"192.168.1.3:10001"}, 0, 0);
    std::vector<std::string> v2{"192.168.0.11:10001", "192.168.0.11:10002", "192.168.0.11:10003", "192.168.0.11:10004"};
    lrc::ClusterInfo c2(v2, std::string{"192.168.0.11:10001"}, 1, 0);
    std::vector<std::string> v3{"192.168.0.21:10001", "192.168.0.21:10002", "192.168.0.21:10003", "192.168.0.21:10004"};
    lrc::ClusterInfo c3(v3, std::string{"192.168.0.21:10001"}, 2, 0);
    std::vector<std::string> v4{"192.168.0.22:10001", "192.168.0.22:10002", "192.168.0.22:10003", "192.168.0.22:10004"};
    lrc::ClusterInfo c4(v4, std::string{"192.168.0.22:10001"}, 3, 0);


    std::unordered_map<int, lrc::ClusterInfo> m_cluster_info;
    m_cluster_info.insert({0, c1});
    m_cluster_info.insert({1, c2});
    m_cluster_info.insert({2, c3});
    m_cluster_info.insert({3, c4});

    std::unordered_map<std::string, lrc::DataNodeInfo> m_dn_info;
    auto dninfo1 = lrc::DataNodeInfo{};dninfo1.clusterid=0;
    auto dn1 = std::make_pair("192.168.1.3:10001",dninfo1);
    auto dninfo2 = lrc::DataNodeInfo{};dninfo2.clusterid=0;
    auto dn2 = std::make_pair("192.168.1.3:10002",dninfo2);
    auto dninfo3 = lrc::DataNodeInfo{};dninfo3.clusterid=0;
    auto dn3 = std::make_pair("192.168.1.3:10003",dninfo3);
    auto dninfo4 = lrc::DataNodeInfo{};dninfo4.clusterid=1;
    auto dn4 = std::make_pair("192.168.0.11:10001",dninfo4);
    auto dninfo5 = lrc::DataNodeInfo{};dninfo5.clusterid=1;
    auto dn5 = std::make_pair("192.168.0.11:10002",dninfo5);
    auto dninfo6 = lrc::DataNodeInfo{};dninfo6.clusterid=1;
    auto dn6 = std::make_pair("192.168.0.11:10003",dninfo6);
    auto dninfo7 = lrc::DataNodeInfo{};dninfo7.clusterid=2;
    auto dn7 = std::make_pair("192.168.0.21:10001",dninfo7);
    auto dninfo8 = lrc::DataNodeInfo{};dninfo8.clusterid=2;
    auto dn8 = std::make_pair("192.168.0.21:10002",dninfo8);
    auto dninfo9 = lrc::DataNodeInfo{};dninfo9.clusterid=2;
    auto dn9 = std::make_pair("192.168.0.21:10003",dninfo9);
    auto dninfo10 = lrc::DataNodeInfo{};dninfo10.clusterid=3;
    auto dn10 = std::make_pair("192.168.0.22:10001",dninfo10);
    auto dninfo11 = lrc::DataNodeInfo{};dninfo11.clusterid=3;
    auto dn11 = std::make_pair("192.168.0.22:10002",dninfo11);
    auto dninfo12 = lrc::DataNodeInfo{};dninfo12.clusterid=3;
    auto dn12 = std::make_pair("192.168.0.22:10003",dninfo12);

    m_dn_info.insert(dn1);
    m_dn_info.insert(dn2);
    m_dn_info.insert(dn3);
    m_dn_info.insert(dn4);
    m_dn_info.insert(dn5);
    m_dn_info.insert(dn6);
    m_dn_info.insert(dn7);
    m_dn_info.insert(dn8);
    m_dn_info.insert(dn9);
    m_dn_info.insert(dn10);
    m_dn_info.insert(dn11);
    m_dn_info.insert(dn12);

    std::unordered_map<int, std::vector<std::string>> fsimage{
            {0,{"192.168.1.3:10002","192.168.1.3:10003","d","192.168.1.3:10001","l","192.168.0.22:10002","192.168.0.22:10001","g"}},
            {1,{"192.168.0.11:10003","192.168.0.11:10001","d","192.168.0.11:10002","l","192.168.0.21:10001","192.168.0.21:10003","g"}},
            {2,{"192.168.0.11:10002","192.168.0.11:10001","d","192.168.0.11:10003","l","192.168.0.22:10002","192.168.0.22:10003","g"}},
            {3,{"192.168.0.22:10001","192.168.0.22:10003","d","192.168.0.22:10001","l","192.168.0.11:10001","192.168.0.11:10003","g"}},
            {4,{"192.168.0.21:10001","192.168.0.21:10002","d","192.168.0.21:10003","l","192.168.1.3:10001","192.168.1.3:10002","g"}},
            {5,{"192.168.0.21:10001","192.168.0.21:10002","d","192.168.0.21:10003","l","192.168.1.3:10001","192.168.1.3:10002","g"}},
            {6,{"192.168.1.3:10003","192.168.1.3:10002","d","192.168.1.3:10001","l","192.168.0.21:10001","192.168.0.21:10002","g"}},
            {7,{"192.168.0.22:10001","192.168.0.22:10003","d","192.168.0.22:10002","l","192.168.0.11:10002","192.168.0.11:10001","g"}}
    };

    std::unordered_map<int, std::vector<std::string>> fsimage2{
            {0,{"192.168.1.3:10002","192.168.1.3:10003","d","192.168.1.3:10001","l","192.168.0.22:10002","192.168.0.22:10001","g"}},
            {1,{"192.168.0.11:10002","192.168.0.11:10001","d","192.168.0.11:10003","l","192.168.0.22:10002","192.168.0.22:10003","g"}},
            {2,{"192.168.0.22:10001","192.168.0.22:10003","d","192.168.0.22:10001","l","192.168.0.11:10001","192.168.0.11:10003","g"}},
            {3,{"192.168.0.21:10001","192.168.0.21:10002","d","192.168.0.21:10003","l","192.168.0.11:10001","192.168.0.11:10002","g"}}
    };
/*
    {
        auto ret = placement_resolve(m_cluster_info, 2, 1, 2);

        for (auto d:ret[0]) {
            std::cout << "datanodes : " << d.first;

        }
        std::cout << "\n";

        for (auto lp:ret[1]) {
            std::cout << "lps : " << lp.first;

        }

        std::cout << "\n";

        for (auto gp:ret[2]) {
            std::cout << "gps : " << gp.first;

        }
        std::cout << "\n";

        ret = placement_resolve(m_cluster_info, 2, 1, 2);

        for (auto d:ret[0]) {
            std::cout << "datanodes : " << d.first;

        }
        std::cout << "\n";

        for (auto lp:ret[1]) {
            std::cout << "lps : " << lp.first;

        }

        std::cout << "\n";

        for (auto gp:ret[2]) {
            std::cout << "gps : " << gp.first;

        }
        std::cout << "\n";
    }
*/
/*

    {
        //test spdlog

        //test LRCCoder
        auto encoder = lrc::LRCCoder({8, 2, 4});
//        encoder.display_matrix();
        auto encoder1 = lrc::LRCCoder({6, 2, 3});
        encoder1.display_matrix();
        auto encoder3 = lrc::LRCCoder({12, 4, 3});
        encoder3.display_matrix();
        auto encoder2 = lrc::LRCCoder({4, 2,2});
        encoder2.display_matrix();
        auto encoder4 = lrc::LRCCoder({8, 4, 4});
        encoder4.display_matrix();
        int  matrix[4*4] = {1,2,3,1,4,5,1,16,17};
        std::cout << jerasure_invertible_matrix(matrix,3,8
                )<<std::endl;

        //test data
        auto data_k_ptrs = new char *[8];
        for(int i=0;i<8;++i)
        {
            data_k_ptrs[i]=new char[32*6];
            std::fill(data_k_ptrs[i],data_k_ptrs[i]+32*6,i<3?'a': (i % 2 ? 'b' : 'd'));
        }

        for(int i = 0 ; i < 8 ;i++)
        {
            for(int j = 0;j<32;j++)
            {
                std::cout << data_k_ptrs[i][j] <<(j<31?"\t":"\n");
            }
        }

        auto globalparity_g_ptrs = new char *[4];
        auto localparity_l_ptrs = new char*[2];

        for(int i=0;i<4;++i)
        {
            globalparity_g_ptrs[i]=new char[32*6];
            std::fill(globalparity_g_ptrs[i],globalparity_g_ptrs[i]+32*6,'1');
        }

        for(int i=0;i<2;++i)
        {
            localparity_l_ptrs[i]=new char[32*6];
            std::copy(data_k_ptrs[i*4],data_k_ptrs[i*4]+32*6,localparity_l_ptrs[i]);
        }

        for(int i =0;i<2;++i)
        {
            for(int j = 0;j<32;j++)
            {
                std::cout  << localparity_l_ptrs[i][j] <<(j<31?"\t":"\n");
            }
        }

        std::cout << "encoding generator matrix :\n";
        encoder.display_matrix();
        encoder.encode(data_k_ptrs, localparity_l_ptrs,globalparity_g_ptrs,12,32*6);

        //display local and global parity
        std::cout << "local parity :\n";
        for(int i =0;i<2;++i)
        {
            for(int j = 0;j<32;j++)
            {
                std::cout  << bitset<8>(localparity_l_ptrs[i][j]) <<(j<31?"\t":"\n");
            }
        }

        std::cout << "global parity :\n";
        for(int i = 0 ; i < 4 ;i++)
        {
            for(int j = 0;j<32;j++)
            {
                std::cout <<bitset<8>(globalparity_g_ptrs[i][j]) <<(j<31?"\t":"\n");
            }
        }



        //erasure 3 blocks

        std::fill(data_k_ptrs[0],data_k_ptrs[0]+32*6,'0');
        std::fill(data_k_ptrs[1],data_k_ptrs[1]+32*6,'0');
        std::fill(data_k_ptrs[2],data_k_ptrs[2]+32*6,'0');
//        std::fill(globalparity_g_ptrs[3],globalparity_g_ptrs[3]+32*6,'0');

        int erasures[5] = {0,1,2,-1,-1};

        //prints
        encoder.decode(8,4,12,0,erasures,data_k_ptrs,globalparity_g_ptrs,32*6);
        std::cout << " after decode : \n" ;
        for(int i = 0 ; i < 8 ;i++)
        {
            for(int j = 0;j<32;j++)
            {
                std::cout <<data_k_ptrs[i][j]<<(j<31?"\t":"\n");
            }
        }
    }
    */

/*
 * test for transition plan
 */
   auto res = generate_basic_transition_plan(m_cluster_info,m_dn_info,fsimage);
   auto res2 = generate_designed_transition_plan(m_cluster_info,m_dn_info,fsimage2);



   /*
    *test for refreshfsimage
   */
    refreshfilesystemimagebasic(res.second[0],res.first[0]);
    refreshfilesystemimagebasic(res.second[1],res.first[1]);
    refreshfilesystemimagebasic(res.second[2],res.first[2]);
    refreshfilesystemimagebasic(res.second[3],res.first[3]);

    for(auto stripe:m_fs_image)
    {
        for(const auto node:stripe.second)
        {
            std::cout << node << "\t";
        }
        std::cout<<std::endl;
    }
    int i=0;//only breakpoint use
   return 0;
}
