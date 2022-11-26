#ifndef LRC_LRCCODER_H
#define LRC_LRCCODER_H

#include "devcommon.h"
#include "Jerasure/include/jerasure.h"

namespace lrc {
    static void GenLRCEncodingMatrix(std::tuple<int, int, int> stripeinfo,
                                     int *matrix, bool trivial = true) {
        // param : in:{k,l,g} matrix[g*k]
        int k = std::get<0>(stripeinfo);
        int l = std::get<1>(stripeinfo);
        int g = std::get<2>(stripeinfo);
        if (g >= 32) {
            assert("g must smaller than 32");
            return;
        }
        int r = g; //  assume g = k/l
        int width = ceil(log2(r + 1));
        int w = l * width;

        // start at 1 , consider first local group
        int cursor = 0;
        for (int i = 0; i < r; ++i) {
            matrix[0 * k + cursor] = (i + 1);
            cursor++;
        }

        for (int j = 1; j < l; ++j) {
            for (int i = 0; i < r; ++i) {
                matrix[0 * k + cursor] = matrix[(0 * k + cursor) - r] << (width);
                cursor++;
            }
        }

        //next row
        for (int j = 1; j < g; ++j) {
            for (int i = 0; i < k; ++i)
                matrix[j * k + i] = galois_single_multiply(matrix[(0) * k + i], matrix[(j - 1) * k + i], w);
        }

    }


    struct LRCCoder {
        // a light weight one shot encoder , ie : LRCCoder{}({k,l,g},const std::vector<char *>,
        // std::vector<char *> ,std::vector<char *> ,wordlength)


        //cache the matrix
        LRCCoder(std::tuple<int, int, int> stripeinfo, bool trivial = true) : k(std::get<0>(stripeinfo)),
                                                                              l(std::get<1>(stripeinfo)),
                                                                              g(std::get<2>(stripeinfo)) {
            generator_matrix = new int[k * g];
            GenLRCEncodingMatrix(stripeinfo, generator_matrix, trivial);
            w = l * ceil(log2(g + 1));
        }

        ~LRCCoder() {
            delete generator_matrix;
        }

        LRCCoder(const LRCCoder &) = delete;

        LRCCoder(LRCCoder &&) = delete;

        void encode(char **data_words,
                    char **target_local_parities, char **target_global_parities,
                    int cellsize /*to set 64 KB*/, bool trivial = true) {
            //do encode

            //global
            if (target_global_parities) {
                if (trivial) {
                    //just xor
                    for (int i = 0; i < g; ++i) {
                        for (int j = 0; j < k; ++j) {
                            galois_region_xor(data_words[j], target_global_parities[i], cellsize);
                        }
                    }
                } else {
                    int *bitmatrix = jerasure_matrix_to_bitmatrix(k, g, w, generator_matrix);
                    jerasure_bitmatrix_encode(k, g, w, bitmatrix, data_words, target_global_parities, cellsize,
                                              sizeof(long));
                }
            }
            //local
            if (target_local_parities) {
                for (int j = 0; j < l; ++j) {
                    for (int i = 1; i < g; ++i) {
                        galois_region_xor(data_words[j * g + i], target_local_parities[j], cellsize);
                    }
                }
            }
//
        }

        void decode(int p_k, int p_m, int w,
                    int row_k_ones, int *erasures,
                    char **data_ptrs, char **coding_ptrs, int cellsize) {
            jerasure_bitmatrix_decode(p_k, p_m, w,
                                      jerasure_matrix_to_bitmatrix(p_k, p_m, w, generator_matrix), 0, erasures,
                                      data_ptrs, coding_ptrs, cellsize, sizeof(long));
//            jerasure_matrix_decode(p_k,p_m,w,generator_matrix,0,erasures,data_ptrs,coding_ptrs,cellsize);
        }

        void display_matrix() {
            if (nullptr == generator_matrix) {
                return;
            }
            jerasure_print_matrix(generator_matrix, g, k, ceil(log2(g + 1)) * l * 2);
        }

        int *getGeneratorMatrix() const {
            return generator_matrix;
        }

    private:
        int *generator_matrix; // default non-defined value
        int k;
        int l;
        int g;
        int w;
    };
}


#endif //LRC_LRCCODER_H
