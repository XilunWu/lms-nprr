package scala.lms.nprr

import scala.lms.common._

trait SetIntersection extends Set {
	this: Dsl =>

	object tmp {
		val mem_size = 1 << 22
		val mem = NewArray[Int] (tmp_mem_size)  // may need be larger
		var curr_set_head = 0
		var curr_set_type = 0
	}

	object intersection {
		import set_const._
		import tmp._

		def clearMem = { curr_set_head = 0; curr_set_type = 0 }

		def getCurrSetSize = {
			if (getCurrSetType == type_uint_set) 
				size_uint_set_head + mem(curr_set_head + loc_uint_set_len)
			else {  //type_bit_set
				size_bit_set_head + mem(curr_set_head + loc_bit_set_len)
			}
		}

		def getCurrSetType = curr_set_type

		// a: uint set, b: uint set
		def uint_inter (a: UintSet, b: UintSet): Set = {
			// if sparse, return BitSet
			// else UintSet
			Set(mem, curr_set_head)
		}
		def uint_inter_helper (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			a_len: Rep[Int], b_len: Rep[Int],
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  // return cardinality
			0
		}

		def bit_inter (a: BitSet, b: BitSet): BitSet = {
			val a_len = a.getLen
			val b_len = b.getLen
			val a_min = a.getMin
			val b_min = b.getMin
			val a_start = a_min >> BITS_PER_INT_SHIFT
			val b_start = b_min >> BITS_PER_INT_SHIFT
			val c_start = max (a_start, b_start)
			val c_len = min (a_start+a_len, b_start+b_len) - c_start
			val c_min = c_start << BITS_PER_INT_SHIFT
			val a_arr = a.getMem
			val a_arr_start = a.getData + c_start - a_start
			val b_arr = b.getMem
			val b_arr_start = b.getData + c_start - b_start

			// the new set will overwrite the old set in tmp memory
			// the struct returned by inter_helper is: head, data. 
			// so we need allocate space in front of data.
			// inter_helper returns the type of set.
			val next_set_start = curr_set_head + getCurrSetSize + size_bit_set_head
			val c_set_len = bit_inter_helper (
				a_arr, b_arr, a_arr_start, b_arr_start, c_len, mem, next_set_start)

			val c_min_offset = mem (next_set_start+loc_bit_set_min-size_bit_set_head)
			val c_real_min = (c_start + c_min_offset) << BITS_PER_INT_SHIFT
			curr_set_head = next_set_start - size_bit_set_head
			curr_set_type = type_bit_set
			val c_set = BitSet (mem, next_set_start - size_bit_set_head)
			c_set
		}
		// a: bit set, b: bit set --> c
		// a and b are aligned before hand
		def bit_inter_helper (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			len: Rep[Int],  
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  // return cardinality

			// simd_bitset_intersection returns the len of the bit array
			// it discards the beginning and ending 0s
			// set builder will count the total 1s
			val set_type = uncheckedPure[Int](
          "simd_bitset_intersection((uint64_t *)", c, 
          ", ", c_start,
          ", (uint64_t *)", a, 
          ", (uint64_t *)", a_start,
          ", (uint64_t *)", b, 
          ", (uint64_t *)", b_start,
          ", (uint64_t)", len,
          ")"
        )
			set_type
		}

		// a: uint set, b: bit set --> c
		def uint_bit_inter (a: UintSet, b: BitSet): UintSet = {
			UintSet(mem, curr_set_head)
		}
		def uint_bit_inter_helper (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			a_len: Rep[Int], b_len: Rep[Int],
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  // return cardinality
			0
		}

		// result set is stored in
		// returns the result set type 
		def setIntersection (a: BitSet, b: BitSet): BitSet = {
			bit_inter(a, b)
		}

		def setIntersection (a: UintSet, b: BitSet): UintSet = {
			uint_bit_inter(a, b)
		}

		def setIntersection (a: UintSet, b: UintSet): Set = {
			uint_inter(a, b)
		}

		// returns the result set type
		def setIntersection (a: BitSet): Rep[Int] = { 
			if (getCurrSetType == type_uint_set) 
				setIntersection (UintSet(mem, curr_set_head), a)
			else // type_bit_set
				setIntersection (BitSet(mem, curr_set_head), a)
		}
		def setIntersection (a: UintSet): Rep[Int] = { 
			if (getCurrSetType == type_uint_set) 
				setIntersection (UintSet(mem, curr_set_head), a)
			else // type_bit_set
				setIntersection (a, BitSet(mem, curr_set_head))
		}
		
	}
}