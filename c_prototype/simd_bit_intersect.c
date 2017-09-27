#include "stdio.h"
#include "stdlib.h"
#include <immintrin.h>
#include <assert.h>

uint64_t encode(uint64_t* bitmap, uint64_t bitmap_size, uint64_t* vec, uint64_t len);
uint64_t decode(uint64_t* vec, uint64_t vec_size, uint64_t *bitmap, uint64_t len);
void print_vec(uint64_t* vec, uint64_t vec_size);
uint64_t simd_bitmap_intersection(uint64_t* output, uint64_t *a, uint64_t *b, uint64_t size);

int main () {
	uint64_t vec_a [] = {1, 2, 5, 7, 10, 12, 15, 256+1, 256+2, 512+5, 512+7, 1024+10, 1024+12, 1024+15};
	uint64_t vec_b [] = {2, 3, 5, 7, 11, 13, 15, 256+1, 256+3, 256+5, 512+7, 1024+10, 1024+13, 1024+15};
	// intersection: 2 5 7 15 257 519 1034 1039 
	uint64_t * vec_c1 = (uint64_t*)malloc(20 * sizeof(uint64_t));
	
	uint64_t j = 0;
	uint64_t k = 0;
	uint64_t count = 0;
	while(1) {
		if (vec_a[j] == vec_b[k]) {
			*(vec_c1 + count++) = vec_a[j];
			if (++j == sizeof(vec_a) / sizeof(uint64_t) || ++k == sizeof(vec_b) / sizeof(uint64_t)) break;
		} else {
			while (vec_a[j] < vec_b[k]) {
				if(++j == sizeof(vec_a) / sizeof(uint64_t)) break;
			}
			while (vec_a[j] > vec_b[k]) {
				if(++k == sizeof(vec_b) / sizeof(uint64_t)) break;
			}
		}
	}
	printf("match_scalar: \n");
	for(int i = 0; i < count; ++i) printf("%llu ", *(vec_c1+i));
	printf("\n\n");

	printf("simd bitmap intersection: \n");
	uint64_t * bitmap_a = (uint64_t *)(malloc(((1024/64)+2) * sizeof(uint64_t)));
	uint64_t * bitmap_b = (uint64_t *)(malloc(((1024/64)+2) * sizeof(uint64_t)));
	uint64_t size;
	size = encode(bitmap_a, 18, vec_a, sizeof(vec_a) / sizeof(uint64_t));
	encode(bitmap_b, 18, vec_b, sizeof(vec_b) / sizeof(uint64_t));
	uint64_t * output = (uint64_t*)malloc(20 * sizeof(uint64_t));
	uint64_t result_size;
	result_size = simd_bitmap_intersection(output, bitmap_a, bitmap_b, size);
	uint64_t * vec_c2 = (uint64_t *)(malloc(20 * sizeof(uint64_t)));
	uint64_t c_count = decode(vec_c2, 20, output, result_size);
	for(uint64_t i = 0; i < c_count; ++i) printf("%llu ", *(vec_c2+i));
	printf("\n");
/*
	uint64_t * vec_a2 = (uint64_t *)(malloc(20 * sizeof(uint64_t)));
	uint64_t * vec_b2 = (uint64_t *)(malloc(20 * sizeof(uint64_t)));	
	uint64_t vec_a_size = decode(vec_a2, 20, bitmap_a, size);
	uint64_t vec_b_size = decode(vec_b2, 20, bitmap_b, size);
	print_vec(vec_a2, vec_a_size);
	print_vec(vec_b2, vec_b_size);	
*/
}

uint64_t encode(uint64_t* bitmap, uint64_t bitmap_size, uint64_t* vec, uint64_t len) {
	uint64_t i = 0;
	uint64_t min = vec[0] & ~63l;
	uint64_t max = (vec[len-1] + 63) & ~63l;
	uint64_t size = (max-min) / 64;
	bitmap[0] = min;
	uint64_t *map_start = &bitmap[1];
	while (i < len) {
		uint64_t value = vec[i++];
		uint64_t loc = value - min;
		assert(loc/64 < bitmap_size-1);
		map_start[loc/64] |= 1l << (63 - loc + loc / 64 * 64);
	}
	return size+1;
}

uint64_t decode(uint64_t* vec, uint64_t vec_size, uint64_t *bitmap, uint64_t len) {
	uint64_t i = 0;
	uint64_t count = 0;
	uint64_t min = bitmap[0];
	uint64_t *map_start = &bitmap[1];
	while (i < len-1) {
		uint64_t num = __builtin_popcountll(map_start[i]);
		uint64_t bitval = map_start[i];
		count += num;
		assert(count <= vec_size);
		for(int j = 0; j < num; ++j) {
			uint64_t pos = __builtin_ctzll(bitval);
			bitval ^= 1l << pos;
			vec[count-j-1] = min+i*64+63-pos;
		}
		i += 1;
	}
	return count;
}

void print_vec(uint64_t* vec, uint64_t vec_size) {
	printf("size of vec = %llu\n", vec_size);
	for(int i = 0; i < vec_size; ++i) printf("%llu ", vec[i]);
	printf("\n");
	return;
}

uint64_t simd_bitmap_intersection(uint64_t* output, uint64_t *a, uint64_t *b, uint64_t bitmap_size) {
	//we assume equal size here:
	uint64_t min_a = a[0];
	uint64_t min_b = b[0];
	uint64_t max_of_min = min_a < min_b ? min_b : min_a;
	uint64_t min_of_max = min_a < min_b ? min_a+(bitmap_size-1)*64 : min_b+(bitmap_size-1)*64;
	uint64_t size = (min_of_max - max_of_min) / 64;

	uint64_t *a_start = a + (max_of_min - min_a) / 64 + 1;
	uint64_t *b_start = b + (max_of_min - min_b) / 64 + 1;

	uint64_t i = 0;
	output[0] = max_of_min;
	uint64_t *output_start = &output[1];
	while ((i+4) < size) {
		const __m256 m_a = _mm256_loadu_ps((float*) &a_start[i]);
		const __m256 m_b = _mm256_loadu_ps((float*) &b_start[i]);
		const __m256 r = _mm256_and_ps(m_a, m_b);
		for(int index = 0; index < 4; ++index) {
			output_start[i+index] = _mm256_extract_epi64(r, index);
			//			printf("%llu\n", output_start[i+index]);
		}
		//separate r into 4 uint64_t
		i += 4;
	}
	while (i < size) {
		output_start[i] = a_start[i] & b_start[i];
		i += 1;
	}
	// less than 4 uint64_t left.
	return size+1;
}
