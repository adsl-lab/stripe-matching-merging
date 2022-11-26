#include <iostream>
#include <vector>
#include <algorithm>
#include <set>
#include <cassert>
#include <numeric>
#include <map>
#include <fstream>

using namespace std;
enum class block_type {
    data_blk,
    lp_blk,
    gp_blk
};
struct block {
    int stripe_id;
    int block_id;
    block_type block_type;
};

struct host {
    int cluster;
    int hostid;
    int traffic;
    int compute_count;

    vector<block> blocks;
    int data_usage;
    int parity_usage;
};


static bool next_combination(vector<int> &perm, int N, int K) {
    assert(perm.size() == K);
    int rightmost_incr = K - 1;
    while (rightmost_incr >= 0 && perm[rightmost_incr] == (N - (K - 1 - rightmost_incr))) {
        rightmost_incr--;
    }
    if (rightmost_incr >= 0) {
        perm[rightmost_incr++]++;
        while (rightmost_incr < K) {
            perm[rightmost_incr] = perm[rightmost_incr - 1] + 1;
            rightmost_incr++;
        }
        std::copy(perm.cbegin(), perm.cend(), std::ostream_iterator<int>(cout));
        return true;
    }

    std::iota(perm.begin(), perm.end(), 1);
    std::copy(perm.cbegin(), perm.cend(), std::ostream_iterator<int>(cout));
    return false;
}

static void generate_cluster(vector<vector<host>> &cluster_view,
                             int cluster_number, int host_per_cluster) {
    cluster_view.assign(cluster_number, vector<host>(host_per_cluster));
    for (int c = 0; c < cluster_number; ++c) {
        for (int h = 0; h < host_per_cluster; ++h) {
            cluster_view[c][h].cluster = c;
            cluster_view[c][h].hostid = h;
        }
    }
}

using placement_policy = tuple<vector<int>, vector<int>, vector<int>>;

static placement_policy generate_stripe(vector<int> &data_cluster,
                                        vector<int> &lp_cluster,
                                        vector<int> &gp_cluster,
                                        const vector<int> &cluster_set) {
    int k = data_cluster.size();
    int ki = 0;
    int l = lp_cluster.size();
    int li = 0;
    int g = gp_cluster.size();
    int gi = 0;

    auto it = cluster_set.cbegin();
    if (l == 0) {
        // rs
    }
    // lrc
    int r = k / l;
    if (r < g) {
        // 12 4 4
        // 3 1 3 1
        // 15 5 4
        int packed = r;
        int capacity = g;
        for (int i = 0; i < l; ++i) {
            if (capacity < packed) {
                ++it;
                capacity = g;
            }
            int cluster_id = *it;
            for (int j = 0; j < packed; ++j) {
                data_cluster[ki++] = cluster_id;
            }
            lp_cluster[li++] = cluster_id;
            capacity -= packed;
        }
        ++it;
        gp_cluster.assign(g, *it);
    } else if (r == g) {
        for (int i = 0; i < l; ++i) {
            int cluster_id = *it;
            for (int j = 0; j < r; ++j) {
                data_cluster[ki++] = cluster_id;
            }
            lp_cluster[li++] = cluster_id;
            ++it;
        }
        gp_cluster.assign(g, *it);
    } else if (r % (g + 1) == 0) {
        int l_a = k / (g + 1);
        for (int i = 0; i < l_a; ++i) {
            int cluster_id = *it;
            for (int j = 0; j < (g + 1); ++j) {
                data_cluster[ki++] = cluster_id;
            }
            ++it;
        }
        for (int i = 0; i < l; ++i) lp_cluster[li++] = *it;
        for (int i = 0; i < g; ++i) gp_cluster[gi++] = *it;
    } else {
        int remain = r % (g + 1);
        int packed = g + 1;
        int l_a = (r / (g + 1)) * l;
        for (int i = 0; i < l_a; ++i) {
            int cluster_id = *it;
            for (int j = 0; j < (packed); ++j) {
                data_cluster[i * r + j] = cluster_id;
            }
            ++it;
        }
        int capacity = g;
        int hybrid_cluster_id = *it;
        for (int i = 0; i < l; ++i) {
            if (capacity < remain) {
                ++it;
                hybrid_cluster_id = *it;
                capacity = g;
            }
            for (int j = 0; j < remain; ++j) {
                data_cluster[i * r + (g + 1) + j] = hybrid_cluster_id;
            }
            lp_cluster[li++] = hybrid_cluster_id;
            capacity -= remain;
        }
        ++it;
        for (int i = 0; i < g; ++i) gp_cluster[gi++] = *it;
    }
    return make_tuple(data_cluster, lp_cluster, gp_cluster);
}


using stripe_location = tuple<vector<pair<int, int>>, vector<pair<int, int>>, vector<pair<int, int>>>;

static stripe_location fill_cluster(placement_policy &stripe,
                                    vector<vector<host>> &cluster_view,
                                    int stripe_id) {
    auto min_data_policy = [](const host &l, const host &r) { return l.data_usage < r.data_usage; };
    auto min_cpu_policy = [](const host &l, const host &r) { return l.parity_usage < r.parity_usage; };
    vector<pair<int, int>> data_loc, lp_loc, gp_loc;
    const auto &[data_cluster, lp_cluster, gp_cluster] = stripe;
    int k = data_cluster.size();
    int l = lp_cluster.size();
    int g = gp_cluster.size();
    int ki = 0;
    int li = 0;
    int gi = 0;
    unordered_map<int, int> cluster_hostnumber;
    while (ki < k) {
        cluster_hostnumber[data_cluster[ki++]]++;
    }
    while (li < l) {
        cluster_hostnumber[lp_cluster[li++]]++;
    }
    while (gi < g) {
        cluster_hostnumber[gp_cluster[gi++]]++;
    }
    unordered_map<int, int> cluster_cursor;
    for (auto [c_id, cnt]: cluster_hostnumber) {
        cluster_cursor[c_id] = 0;
        sort(cluster_view[c_id].begin(), cluster_view[c_id].end(), min_data_policy);
    }

    for (int id = 0; id < k; ++id) {
        int cursor = cluster_cursor[data_cluster[id]]++;
        cluster_view[data_cluster[id]][cursor].data_usage++;
        cluster_view[data_cluster[id]][cursor].blocks.emplace_back(block{stripe_id, id, block_type::data_blk});
        data_loc.push_back(pair<int, int>(data_cluster[id], cluster_view[data_cluster[id]][cursor].hostid));
    }
    for (int id = 0; id < l; ++id) {
        int cursor = cluster_cursor[lp_cluster[id]]++;
        cluster_view[lp_cluster[id]][cursor].parity_usage++;
        cluster_view[lp_cluster[id]][cursor].blocks.emplace_back(block{stripe_id, id, block_type::lp_blk});
        lp_loc.push_back(pair<int, int>(lp_cluster[id], cluster_view[lp_cluster[id]][cursor].hostid));
    }
    for (int id = 0; id < g; ++id) {
        int cursor = cluster_cursor[gp_cluster[id]]++;
        cluster_view[gp_cluster[id]][cursor].parity_usage++;
        cluster_view[gp_cluster[id]][cursor].blocks.emplace_back(block{stripe_id, id, block_type::gp_blk});
        gp_loc.push_back(pair<int, int>(gp_cluster[id], cluster_view[gp_cluster[id]][cursor].hostid));
    }

    stripe_location stripe_loc = make_tuple(data_loc, lp_loc, gp_loc);
    return stripe_loc;

}

static set<int> resolve_clusterid(const stripe_location &s) {
    const auto &[data_clusters, lp_clusters, _ignore] = s;
    set<int> ret;
    for (auto c: data_clusters) {
        ret.insert(c.first);
    }
    return ret;
    // gp no need
}

static int compute_overlap(const stripe_location &s1,
                           const stripe_location &s2,
                           int upcodeordowncode,
                           int mode) {
    if (upcodeordowncode < 0) {
        const auto &[dc1, lc1, gc1] = s1;
        const auto &[dc2, lc2, gc2] = s2;
        int k = dc1.size();
        int l = lc1.size();
        int g = gc1.size();
        int r = k / l;
        int cost = 0;
        map<int, int> cluster_cnt;
        for (auto [cluster, host]: dc1) {
            cluster_cnt[cluster]++;
        }

        for (auto [cluster, host]: dc2) {
            cluster_cnt[cluster]++;
        }

        for (auto [cluster, cnt]: cluster_cnt) {
            cost += (mode==1?min(2 * g, cnt):cnt);
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
            map<int, int> cluster_cnt;
            for (auto [cluster, host]: dc1) {
                cluster_cnt[cluster]++;
            }

            for (auto [cluster, host]: dc2) {
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
                cost += (mode==1?min(g, cnt):cnt);
                if (cnt > theta) cost += cnt - theta;
            }

            return cost;
        } else if (r % (g + 1) == 0) {
            // 16 2 3
            int type1_cnt = 2 * (g + 1);
            int type2_cnt = (g + 1);
            int weight1 = 1;
            int weight2 = 2;
            map<int, int> cluster_cnt;
            for (auto [cluster, host]: dc1) {
                cluster_cnt[cluster]++;
            }

            for (auto [cluster, host]: dc2) {
                cluster_cnt[cluster]++;
            }

            for (auto [cluster, cnt]: cluster_cnt) {
//            if (cnt == type1_cnt) {
//                cost += weight1;
//            } else {
//                cost += weight2;
//            }
                cost += (mode==1?g:cnt);
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
            int weight_ab2 = min(g+1,theta/m*(m+1));
            map<int, int> cluster_cnt;
            for (auto [cluster, host]: dc1) {
                cluster_cnt[cluster]++;
            }

            for (auto [cluster, host]: dc2) {
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
                cost += (mode==1?min(cnt, g):cnt);
            }

            return cost;
        }
    }
}


using graph = vector<vector<int>>;

static graph build_graph(const vector<stripe_location> &stripes, int upcodeordowncode,int mode) {
    int N = stripes.size();
    graph g(N, vector<int>(N, -1));
    for (int i = 0; i < N; ++i) {
        for (int j = i; j < N; ++j) {
            if (i == j) {
                g[i][i] = 0;
            } else {
                g[i][j] = compute_overlap(stripes[i], stripes[j], upcodeordowncode,mode);
                g[j][i] = g[i][j];
            }
        }
    }
    return g;
}


int main() {
    const int cluster_number = 8;
    const int host_per_cluster_number = 10;
    const int stripe_number = 1000;
    const int k = 6;
    const int l = 2;
    const int g = 2;
    int upcodeordowncode = -1;//-1 for g double  1 for g same
    int mode = 1;// -1 for non-partial 1 for partial
    vector<vector<host>> cluster_view;
    vector<stripe_location> stripes;
    vector<placement_policy> stripes_placement;
    generate_cluster(cluster_view, cluster_number, host_per_cluster_number);

    vector<int> candidate_cluster(cluster_number, 0);
    iota(candidate_cluster.begin(), candidate_cluster.end(), 0);
    vector<int> data_cluster(k, -1);
    vector<int> lp_cluster(l, -1);
    vector<int> gp_cluster(g, -1);
    for (int i = 0; i < stripe_number; ++i) {
        data_cluster.assign(k, -1);
        lp_cluster.assign(l, -1);
        gp_cluster.assign(g, -1);
        random_shuffle(candidate_cluster.begin(), candidate_cluster.end());
        auto stripe = generate_stripe(data_cluster, lp_cluster, gp_cluster, candidate_cluster);
        stripes_placement.push_back(stripe);
        copy(data_cluster.cbegin(), data_cluster.cend(), ostream_iterator<int>(cout, ","));
        cout << "\n";
        copy(lp_cluster.cbegin(), lp_cluster.cend(), ostream_iterator<int>(cout, ","));
        cout << "\n";
        copy(gp_cluster.cbegin(), gp_cluster.cend(), ostream_iterator<int>(cout, ","));
        cout << "\n";
        stripes.emplace_back(fill_cluster(stripe, cluster_view, i));
    }
    auto graph_builded = build_graph(stripes, upcodeordowncode,mode);
    ofstream fs("case-best");
    fs << stripe_number << " " << (stripe_number * (stripe_number - 1)) / 2 << "\n";
    for (int i = 0; i < stripe_number; ++i) {
        for (int j = i + 1; j < stripe_number; ++j) {
            fs << i << " " << j << " " << graph_builded[i][j] << "\n";
        }
    }
    fs.flush();
    fs.close();
    fs.open("case-seq");
    fs << stripe_number << " " << stripe_number / 2 << "\n";
    for (int i = 0; i < stripe_number; i += 2) {
        fs << i << " " << i + 1 << " " << graph_builded[i][i + 1] << "\n";
    }
    fs.flush();
    fs.close();
    fs.open("case-random");
    vector<int> rank(stripe_number, 0);
    iota(rank.begin(), rank.end(), 0);
    random_shuffle(rank.begin(), rank.end());
    fs << stripe_number << " " << stripe_number / 2 << "\n";
    for (int i = 0; i < stripe_number; i += 2) {
        fs << rank[i] << " " << rank[i + 1] << " " << graph_builded[rank[i]][rank[i + 1]] << "\n";
    }
    fs.flush();
    fs.close();
    return 0;
}
