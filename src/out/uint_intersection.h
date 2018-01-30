

#ifndef SIMDCompressionAndIntersection_INTERSECTION_H_
#define SIMDCompressionAndIntersection_INTERSECTION_H_

#include <common.h>

using namespace std;
/*
 * Given two arrays, this writes the intersection to out. Returns the
 * cardinality of the intersection.
 */

#ifdef __AVX2__
#include <immintrin.h>

/*
 * Straight port of SIMDintersection to AVX2.
 */
size_t SIMDintersection_avx2(const uint32_t *set1, const size_t length1,
                        const uint32_t *set2, const size_t length2,
                        uint32_t *out);

#endif
/*
 * Given two arrays, this writes the intersection to out. Returns the
 * cardinality of the intersection.
 *
 * This applies a state-of-the-art algorithm. First coded by O. Kaser, adapted
 * by D. Lemire.
 */
size_t onesidedgallopingintersection(const uint32_t *smallset,
                                     const size_t smalllength,
                                     const uint32_t *largeset,
                                     const size_t largelength, uint32_t *out);



#endif /* SIMDCompressionAndIntersection_INTERSECTION_H_ */