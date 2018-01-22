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

		def getCurrSetSize = {
			if (getCurrSetType == type_uint_set) 
				size_uint_set_head + mem(curr_set_head + loc_uint_set_len)
			else {  //type_bit_set
				size_bit_set_head + mem(curr_set_head + loc_bit_set_len)
			}
		}

		def getCurrSetType = curr_set_type

		// a: uint set, b: uint set
		def uint_inter (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			a_len: Rep[Int], b_len: Rep[Int],
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  // return cardinality

		}

		def bit_inter (a: BitSet, b: BitSet): Set = {
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
			// the struct is: head, data
			// returns the type of set
			val next_set_start = curr_set_head + getCurrSetSize
			val new_set_type = bit_inter_helper (
				a_arr, b_arr, a_arr_start, b_arr_start, c_len, mem, next_set_start)
			
			val c_min_offset = mem (bitarr_len+loc_bit_array_min_offset)
			val c_real_min = (c_start + c_min_offset) << BITS_PER_INT_SHIFT

			val c_array = new SimpleSet (tmp.mem, 0, type_bit_set, bitarr_len, c_real_min)
			c_array
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
			val bitarr_len = uncheckedPure[Int](
          "simd_bitset_intersection((uint64_t *)", c, 
          ", ", c_start,
          ", (uint64_t *)", a, 
          ", (uint64_t *)", a_start,
          ", (uint64_t *)", b, 
          ", (uint64_t *)", b_start,
          ", (uint64_t)", len,
          ")"
        )
			bitarr_len
		}

		// a: uint set, b: bit set --> c
		def uint_bit_inter (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			a_len: Rep[Int], b_len: Rep[Int],
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  // return cardinality
			
		}

		// result set is stored in
		// returns the result set type 
		def setIntersection (a: SimpleSet, b: SimpleSet): SimpleSet = {
			if (a.typ == type_bit_set) {
				if (s2.getType == type_bit_set) 
					build(new BitSet(s1.mem, s1.data), new BitSet(s2.mem, s2.data))
				else // type_uint_set
					build(new UIntSet(s2.mem, s2.data), new BitSet(s1.mem, s1.data))
			}
			else { // type_uint_set 
				if (s2.getType == type_bit_set) 
					build(new UIntSet(s1.mem, s1.data), new BitSet(s2.mem, s2.data))
				else
					build(new UIntSet(s1.mem, s1.data), new UIntSet(s2.mem, s2.data))
			}
			// curr_simple_set points to the new simple set
			tmp.curr_simple_set = 
			SimpleSet(tmp.mem, tmp.curr_simple_set)
		}

		// returns the result set type
		def setIntersection (a: SimpleSet): Rep[Int] = { 
			def currSet = SimpleSet (tmp.mem, tmp.curr_simple_set)
			setIntersection(a, currSet)
		}
		
	}

	 
	/*
	sealed abstract SimpleArray
	case class BitArray (
		val array: Rep[Array[Int]],
		val start: Rep[Int],
		val len: Rep[Int],
		val min: Rep[Int]
	) extends SimpleArray
	case class UIntArray (
		val array: Rep[Array[Int]],
		val start: Rep[Int],
		val len: Rep[Int],
		val min: Rep[Int]
	) extends SimpleArray
	*/
}