package scala.lms.nprr

import scala.lms.common._

trait SetIntersection extends Set {
	this: Dsl =>

  // have to make them all case class to avoid violating 
  // ordering of effect. (not sure if case necessary but ....)
  // Can we make them object without violating effect order???
	case class TmpMem () {
		val mem_size = 1 << 22
		val mem = NewArray[Int](mem_size)  // may need be larger
		val curr_set_info = NewArray[Int](2)
		val loc_curr_set_head = 0
		val loc_curr_set_type = 1
		def set_curr_set_head (head: Rep[Int]) = {
			uncheckedPure[Unit] (curr_set_info, "[ ", loc_curr_set_head, " ]",
				" = ", head
			)
		}
		def set_curr_set_type (typ: Rep[Int]) = {
			uncheckedPure[Unit] (curr_set_info, "[ ", loc_curr_set_type, " ]",
				" = ", typ
			)
		}
		def get_curr_set_head = curr_set_info (loc_curr_set_head )
		def get_curr_set_type = curr_set_info (loc_curr_set_type )
	}

	case class Intersection () {
		import set_const._

		val tmp = TmpMem()

		def clearMem = { 
			// free mem and curr_set_info
		}

		def getMem = tmp.mem

		def getCurrSet = tmp.get_curr_set_head

		def getCurrSetSize = {
			if (getCurrSetType == type_uint_set) 
				size_uint_set_head + tmp.mem( tmp.get_curr_set_head + loc_uint_set_len )
			else {  //type_bit_set
				size_bit_set_head + tmp.mem( tmp.get_curr_set_head + loc_bit_set_len )
			}
		}

		def getCurrSetType = tmp.get_curr_set_type

		// a: uint set, b: uint set
		def uint_inter (a: UintSet, b: UintSet): Rep[Unit] = {
			// if sparse, return BitSet
			// else UintSet
		}
		def uint_inter_helper (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			a_len: Rep[Int], b_len: Rep[Int],
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  // return cardinality
			0
		}

		def bit_inter (a: BitSet, b: BitSet): Rep[Unit] = {
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
			val next_set_start = 
				tmp.get_curr_set_head + getCurrSetSize + size_bit_set_head
			val c_set_len = bit_inter_helper (
				a_arr, b_arr, a_arr_start, b_arr_start, c_len, tmp.mem, next_set_start)

			val c_min_offset = tmp.mem (next_set_start+loc_bit_set_min-size_bit_set_head)
			val c_real_min = (c_start + c_min_offset) << BITS_PER_INT_SHIFT
			tmp.set_curr_set_head( next_set_start - size_bit_set_head )
			tmp.set_curr_set_type( type_bit_set )
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
			
			val set_len = uncheckedPure[Int](
          "simd_bitset_intersection((uint64_t *)", c, 
          ", ", c_start,
          ", (uint64_t *)", a, 
          ", (uint64_t)", a_start,
          ", (uint64_t *)", b, 
          ", (uint64_t)", b_start,
          ", (uint64_t)", len,
          ")"
        )
			set_len
		}

		// a: uint set, b: bit set --> c
		def uint_bit_inter (a: UintSet, b: BitSet): Rep[Unit] = {
		}
		def uint_bit_inter_helper (a: Rep[Array[Int]], b: Rep[Array[Int]], 
			a_start: Rep[Int], b_start: Rep[Int], 
			a_len: Rep[Int], b_len: Rep[Int],
			c: Rep[Array[Int]], c_start: Rep[Int]): Rep[Int] = {  // return cardinality
			0
		}

		// result set is stored in
		// BitSet X BitSet = BitSet
		def setIntersection (a: BitSet, b: BitSet): Rep[Unit] = {
			bit_inter(a, b)
		}

		// UintSet X BitSet = UintSet
		def setIntersection (a: UintSet, b: BitSet): Rep[Unit] = {
			uint_bit_inter(a, b)
		}

		// UintSet X UintSet = BitSet or UintSet
		def setIntersection (a: UintSet, b: UintSet): Rep[Unit] = {
			uint_inter(a, b)
		}

		def setIntersection (a: BitSet): Rep[Unit] = { 
			if (getCurrSetType == type_uint_set) 
				setIntersection (UintSet(tmp.mem, tmp.get_curr_set_head), a)
			else // type_bit_set
				setIntersection (BitSet(tmp.mem, tmp.get_curr_set_head), a)
		}
		def setIntersection (a: UintSet): Rep[Unit] = { 
			if (getCurrSetType == type_uint_set) 
				setIntersection (UintSet(tmp.mem, tmp.get_curr_set_head), a)
			else // type_bit_set
				setIntersection (a, BitSet(tmp.mem, tmp.get_curr_set_head))
		}
		
	}
}