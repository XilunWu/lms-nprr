package scala.lms.nprr

import scala.lms.common._

trait SetIntersection extends Set {
	this: Dsl =>

  // have to make them all case class to avoid violating 
  // ordering of effect. (not sure if case necessary but ....)
  // Can we make them object without violating effect order???

	import set_const._

	object intersection {

		// Here starts UintSet * UintSet Intersection
		// a: uint set, b: uint set
		def uint_inter (a: UintSet, b: UintSet): Rep[Long] = {
			0l
		}

		def uint_inter (a: UintSet, b: UintSet, c_arr: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {
			// if sparse, return BitSet
			// else UintSet
			val a_len = a.getLen
			val b_len = b.getLen
			val a_start = a.getData
			val b_start = b.getData
			val a_arr = a.getMem
			val b_arr = b.getMem
			val c_set_size = uint_inter_helper (
				a_arr, b_arr, a_start, b_start, a_len, b_len, c_arr, c_start)
			c_set_size
		}
		def uint_inter_helper (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			a_len: Rep[Int], b_len: Rep[Int],
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  // return cardinality
			// find the rare array and the freq array?
			// No, it's done in SIMDIntersection.
			/*
			val c_len = uncheckedPure[Int]("SIMDIntersection(",
				"(const uint32_t *) ", a, ", ",
				"(size_t) ", a_start, ", ",
				"(const uint32_t *) ", b, ", ",
				"(size_t) ", b_start, ", ",
				"(size_t) ", a_len, ", ",
				"(size_t) ", b_len, ", ",
				"(const uint32_t *) ", c, ", ",
				"(size_t) ", c_start,
				")"
			)
			c_len
			*/
			0
			/*
				TODO: We do this step after testing all intersection parts

			val range = ( c( c_start + c_len - 1 ) - c( c_start ) )
			val sparse = 
				if ( range > set_const.BITS_IN_AVX_REG * c_len ) true
			  else false
			if (sparse) {  // build uint set 

			} else {  // build bit set 
				// for test, we don't build bit set at this monent. 
			}
			*/
		}

		// Here starts BitSet * BitSet Intersection

		def bit_inter (a: BitSet, b: BitSet): Rep[Long] = {
			// basically the same code but call different c routine. 
			// same c routine can be used
			val a_len = a.getLen
			val b_len = b.getLen
			val a_min = a.getMin
			val b_min = b.getMin
			val a_start = a_min >>> BITS_PER_INT_SHIFT
			val b_start = b_min >>> BITS_PER_INT_SHIFT
			
			val c_start = if ( a_start > b_start ) a_start else b_start
			val c_len = 
				if ( a_start + a_len > b_start + b_len ) 
					b_start + b_len - c_start
				else 
					a_start + a_len - c_start
			val c_min = c_start << BITS_PER_INT_SHIFT
			val a_arr = a.getMem
			val a_arr_start = a.getData + c_start - a_start
			val b_arr = b.getMem
			val b_arr_start = b.getData + c_start - b_start

			val c_set_card = bit_inter_aggregate_helper (
				a_arr, b_arr, a_arr_start, b_arr_start, c_len)
			c_set_card
		}

		def bit_inter (a: BitSet, b: BitSet, c_arr: Rep[Array[Int]], c_addr: Rep[Int]): Rep[Int] = {
			val a_len = a.getLen
			val b_len = b.getLen
			val a_min = a.getMin
			val b_min = b.getMin
			val a_start = a_min >>> BITS_PER_INT_SHIFT
			val b_start = b_min >>> BITS_PER_INT_SHIFT
			
			val c_start = if ( a_start > b_start ) a_start else b_start
			val c_len = 
				if ( a_start + a_len > b_start + b_len ) 
					b_start + b_len - c_start
				else 
					a_start + a_len - c_start
			val c_min = c_start << BITS_PER_INT_SHIFT
			val a_arr = a.getMem
			val a_arr_start = a.getData + c_start - a_start
			val b_arr = b.getMem
			val b_arr_start = b.getData + c_start - b_start

			// the new set will overwrite the old set in tmp memory
			// the struct returned by inter_helper is: head, data. 
			// so we need allocate space in front of data.
			// inter_helper returns the type of set.

			// error: violating ordering of effect 
			// Resolved: Can't define variable in object. 
			// Ask: Why?
			// Answer: It's about scoping. We must make sure the variable doesn't escape the scope.
			val c_set_size = bit_inter_helper (
				a_arr, b_arr, a_arr_start, b_arr_start, c_len, c_arr, c_addr)

			val c_min_offset = BitSet(c_arr, c_addr).getMin
			val c_real_min = (c_start + c_min_offset) << BITS_PER_INT_SHIFT
			c_set_size
		}
		// a: bit set, b: bit set --> c: bit set 
		// a and b are aligned before hand
		def bit_inter_helper (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			len: Rep[Int],  
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  // return cardinality

			// simd_bitset_intersection returns the len of the bit array
			// it discards the beginning and ending 0s
			// set builder will count the total 1s
			
			val set_size = uncheckedPure[Int](
          "simd_bitset_intersection(", 
          "(uint32_t *)", c, 
          ", ", c_start,
          ", (uint32_t *)", a, 
          ", (size_t)", a_start,
          ", (uint32_t *)", b, 
          ", (size_t)", b_start,
          ", (size_t)", len,
          ")"
        )
			set_size
		}

		def bit_inter_aggregate_helper (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			len: Rep[Int]): Rep[Long] = {  
			// we only test count: return cardinality

			// simd_bitset_intersection returns the len of the bit array
			// it discards the beginning and ending 0s
			// set builder will count the total 1s
			
			val set_card = uncheckedPure[Long](
          "simd_bitset_intersection_count(", 
          "(uint32_t *)", a, 
          ", (size_t)", a_start,
          ", (uint32_t *)", b, 
          ", (size_t)", b_start,
          ", (size_t)", len,
          ")"
        )
			set_card
		}

		// Here starts UintSet * BitSet Intersection
		// a: uint set, b: bit set --> c
		def uint_bit_inter (a: UintSet, b: BitSet): Rep[Long] = {
			0l
		}
		def uint_bit_inter (a: UintSet, b: BitSet, c_arr: Rep[Array[Int]], c_addr: Rep[Int]): Rep[Int] = {
			0
		}

		def uint_bit_inter_helper (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			a_len: Rep[Int], b_len: Rep[Int],
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  
			0
		}

		// result set is stored in
		// BitSet X BitSet = BitSet
		def setIntersection (a: BitSet, b: BitSet): Rep[Long] = {
			bit_inter(a, b)
		}

		// UintSet X BitSet = UintSet
		def setIntersection (a: UintSet, b: BitSet): Rep[Long] = {
			uint_bit_inter(a, b)
		}

		// UintSet X UintSet = BitSet or UintSet
		def setIntersection (a: UintSet, b: UintSet): Rep[Long] = {
			uint_inter(a, b)
		}

		def setIntersection (a: BitSet, b: BitSet, mem: Rep[Array[Int]], start: Rep[Int]): Rep[Int] = {
			bit_inter(a, b, mem, start)
		}

		// UintSet X BitSet = UintSet
		def setIntersection (a: UintSet, b: BitSet, mem: Rep[Array[Int]], start: Rep[Int]): Rep[Int] = {
			uint_bit_inter(a, b, mem, start)
		}

		// UintSet X UintSet = BitSet or UintSet
		def setIntersection (a: UintSet, b: UintSet, mem: Rep[Array[Int]], start: Rep[Int]): Rep[Int] = {
			uint_inter(a, b, mem, start)
		}
/*
		def setIntersection (a: BitSet): Rep[Long] = { 
			if (getCurrSetType == type_uint_set) 
				setIntersection (UintSet(tmp.mem, tmp.get_curr_set_head), a)
			else // type_bit_set
				setIntersection (BitSet(tmp.mem, tmp.get_curr_set_head), a)
		}
		def setIntersection (a: UintSet): Rep[Long] = { 
			if (getCurrSetType == type_uint_set) 
				setIntersection (UintSet(tmp.mem, tmp.get_curr_set_head), a)
			else // type_bit_set
				setIntersection (a, BitSet(tmp.mem, tmp.get_curr_set_head))
		}
*/		
	}
}