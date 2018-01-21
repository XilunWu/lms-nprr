package scala.lms.nprr

import scala.lms.common._

trait SetIntersection {
	this: Dsl =>

	object tmp {
		val mem_size = 1 << 22
		val mem = NewArray[Int] (tmp_mem_size)  // may need be larger
		var curr_simple_set = 0
	}

	object inter_const {
		// data type
		val BITS_PER_INT = 64
		val BITS_PER_INT_SHIFT = 6

		// Intermediate array struct
		val loc_simple_set_type = 0

		// bit set
		val loc_simple_bit_set_len = 1
		val loc_simple_bit_set_min = 2
		val size_simple_bit_set_head = 3

		// uint set
		val loc_simple_uint_set_card = 1
		val size_simple_uint_set_head = 2

		// types
		val type_simple_uint_set = 0
		val type_simple_bit_set = 1
	}

	object intersection {
		import inter_const._

		// a: uint set, b: uint set
		def uint_inter (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			a_len: Rep[Int], b_len: Rep[Int],
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  // return cardinality

		}

		def bit_inter (a: SimpleSet, b: SimpleSet): SimpleSet = {
			val a_len = a.len
			val b_len = b.len
			val a_min = a.min
			val b_min = b.min
			val a_start = a_min >> BITS_PER_INT_SHIFT
			val b_start = b_min >> BITS_PER_INT_SHIFT
			val c_start = max (a_start, b_start)
			val c_len = min (a_start+a_len, b_start+b_len) - c_start
			val c_min = c_start << BITS_PER_INT_SHIFT
			val a_arr = a.array
			val a_arr_start = a.start + c_start - a_start
			val b_arr = b.array
			val b_arr_start = b.start + c_start - b_start

			// the new set will overwrite the old set in tmp memory
			// the struct is: data, card, min_offset 
			val bitarr_len = bit_inter_helper (
				a_arr, b_arr, a_arr_start, b_arr_start, c_len, tmp.mem, 0)

			val c_min_offset = tmp.mem (bitarr_len+loc_bit_array_min_offset)
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
		def setIntersection (a: SimpleSet, b: SimpleSet): SimpleSet = {

			// curr_simple_set points to the new simple set
			tmp.curr_simple_set = 
			SimpleSet(tmp.mem, tmp.curr_simple_set)
		}

		def setIntersection (a: SimpleSet): SimpleSet = {
			def currSet = SimpleSet (tmp.mem, tmp.curr_simple_set)
			setIntersection(a, currSet)
		}
		
	}

	class SimpleSet (
		val array: Rep[Array[Int]],
		val start: Rep[Int]) {
		def typ = array(start + loc_simple_set_type)
	}

	class SimpleUintSet (
		a: Rep[Array[Int]], 
		s: Rep[Int]
	) extends SimpleSet (a, s) {
		override def typ = type_simple_uint_set
		def card = array(start + loc_simple_uint_set_card)
	}

	class SimpleBitSet (
		a: Rep[Array[Int]], 
		s: Rep[Int]
	) extends SimpleSet (a, s) {
		override def typ = type_simple_bit_set
		def len = array(start + loc_simple_bit_set_len)
		def min = array(start + loc_simple_bit_set_min)
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