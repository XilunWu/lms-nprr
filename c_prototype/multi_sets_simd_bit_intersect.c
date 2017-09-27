uint64_t decode(uint64_t* vec, uint64_t *bitmap, uint64_t len, uint64_t min);

uint64_t simd_bitmap_intersection_helper(uint64_t *const output, uint64_t *const *bit, uint64_t num_of_maps, uint64_t bitmap_size, uint64_t min) {	
	//we assume equal size and bitmaps all are already aligned here:
	uint64_t i = 0;
	uint64_t count = 0;

	uint64_t *bitmap_256 = (uint64_t*)malloc(4 * sizeof(uint64_t));
	while ((i+4) < bitmap_size) {
		__m256 m_a = _mm256_loadu_ps((float*) &(bit[0][i]));
		for(int j = 1; j < num_of_maps; ++j) {
			const __m256 m_b = _mm256_loadu_ps((float*) &(bit[j][i]));
			m_a = _mm256_and_ps(m_a, m_b);
		}
		//separate r into 4 uint64_t
		for(int index = 0; index < 4; ++index) {
			bitmap_256[index] = _mm256_extract_epi64(m_a, index);
		}
		count += decode(&output[count], bitmap_256, 4, min+i*64);
		i += 4;
	}
	uint64_t i_tmp = i;
	while (i < bitmap_size) {
		uint64_t a = bit[0][i];
		for(int j = 1; j < num_of_maps; ++j) a &= bit[j][i];
		bitmap_256[i-i_tmp] = a;
		i += 1;
	}
	count += decode(&output[count], bitmap_256, i-i_tmp, min+i_tmp*64);
	free(bitmap_256);
	return count;
}

uint64_t simd_bitmap_intersection(uint64_t *const output, uint64_t *const *bit, uint64_t *start, uint64_t num_of_maps, uint64_t bitmap_size, uint64_t min) {
	uint64_t **bitmap_start = (uint64_t**)malloc(num_of_maps * sizeof(uint64_t *));
	
	for(int i = 0; i < num_of_maps; ++i) {
		bitmap_start[i] = bit[i]+start[i];
	}
	return simd_bitmap_intersection_helper(output, bitmap_start, num_of_maps, bitmap_size, min);
}

uint64_t decode(uint64_t* vec, uint64_t *bitmap, uint64_t len, uint64_t min) {
	uint64_t i = 0;
	uint64_t count = 0;
	while (i < len) {
		uint64_t num = __builtin_popcountll(bitmap[i]);
		uint64_t bitval = bitmap[i];
		count += num;
		for(int j = 0; j < num; ++j) {
			uint64_t pos = __builtin_ctzll(bitval);
			bitval ^= 1l << pos;
			vec[count-j-1] = min+i*64+63-pos;
		}
		i += 1;
	}
	return count;
}
