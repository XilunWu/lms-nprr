package scala.lms.nprr

import scala.lms.common._

object trieblock_const {
	val size_of_trie_block_head = 0
}

trait TrieBlock extends Set with SetIntersection {
	this: Dsl => 

	import trieblock_const._

	case class TrieBlock (
		val mem: Rep[Array[Int]],
		val data: Rep[Int]
	) {
		val set_data = data+size_of_trie_block_head

		def getSetType = set.getSetType (mem, set_data)
		def getSetSize = {
			if (getSetType == SetType.UintSet) {
				val uintset = UintSet(mem, set_data)
				uintset.getSize
			} else { // type_bit_set
				val bitset = BitSet(mem, set_data)
				bitset.getSize
			}
		}
		def getBlockSize = {
			if (getSetType == SetType.UintSet) {
				val uintset = UintSet(mem, set_data)
				size_of_trie_block_head + uintset.getSize + uintset.getCard
			} else { // type_bit_set
				val bitset = BitSet(mem, set_data)
				size_of_trie_block_head + bitset.getSize + bitset.getIndexSize 
			}
		}
		def getCard = {
			if (getSetType == SetType.UintSet) {
				val uintset = UintSet(mem, set_data)
				uintset.getCard
			} else { // type_bit_set
				val bitset = BitSet(mem, set_data)
				bitset.getCard
			}
		}
		def getSetAddr = set_data
		def getBitSet = BitSet (mem, set_data)
		def getUintSet = UintSet (mem, set_data)
		def getChildBlock (key: Rep[Int]) = {
			if (getSetType == SetType.UintSet) {
				val uintset = UintSet(mem, set_data)
				val index = uintset getIndexByKey key
				mem(set_data+uintset.getSize+index)
			} else { // type_bit_set
				val bitset = BitSet(mem, set_data)
				val index = bitset getIndexByKey key
				mem(set_data+bitset.getSize+index)
			}
		}
		def getChildBlockByIndex (index: Rep[Int]) =
			if (getSetType == SetType.UintSet) {
				val uintset = UintSet(mem, set_data)
				mem(set_data+uintset.getSize+index)
			} else { // type_bit_set
				val bitset = BitSet(mem, set_data)
				mem(set_data+bitset.getSize+index)
			}

		def refineIndex (index: Rep[Int], value: Rep[Int], c_addr: Rep[Int]) = {
			if (getSetType == SetType.UintSet) refineIndexByIndex (index, c_addr)
			else refineIndexByValue (value, c_addr)
		}
		def refineIndexByIndex (index: Rep[Int], c_addr: Rep[Int]) = {
			val uintset = UintSet(mem, set_data)
			val start = uintset.getSize
			mem(set_data+start+index) = c_addr
		}
		def refineIndexByValue (value: Rep[Int], c_addr: Rep[Int]) = {
			val bitset = BitSet(mem, set_data)
			val min = bitset.getMin
			val start = bitset.getSize
			mem(set_data+start+value-min) = c_addr
		}
		def buildFromRawData (
			arr: Rep[Array[Int]], begin: Rep[Int], end: Rep[Int]) = {
			// build TrieBlock head
			val sparse = 
				if ( arr(end-1) - arr(begin) < set_const.BITS_IN_AVX_REG * (end - begin) ) false
				else true
			val sparse_test = false
		  if (sparse_test) {
				val set = UintSet(mem, set_data)
				set.buildUintSet (arr, begin, end)
			}
			else {
				val set = BitSet(mem, set_data)
				set.buildBitSet (arr, begin, end)
			}
		}
/*
		def build (s: List[TrieBlock]): Rep[Int] = { 
			val intersection = Intersection()
			// if s.length == 1 ... else ...
			// we don't support 1 relation join yet.
			val a_set_type = s(0).getSetType
			val b_set_type = s(1).getSetType
			if (a_set_type == Set.BitSet) { 
				if (b_set_type == Set.BitSet) {
					intersection.setIntersection (s(0) getBitSet, s(1) getBitSet)
				} else {
					intersection.setIntersection (s(1) getUintSet, s(0) getUintSet)
				}
			} else { // uintset
				if (b_set_type == Set.BitSet) {
					intersection.setIntersection (s(0) getUintSet, s(1) getBitSet)
				} else {
					intersection.setIntersection (s(0) getUintSet, s(1) getUintSet)
				}
			}
			// the result is stored in the tmp memory of object "intersection"
			
			s.tail.tail.foreach { tb =>
				val next_set_type = tb.getSetType
				if (next_set_type == Set.UintSet)
					intersection.setIntersection(tb getUintSet)
				else 
					intersection.setIntersection(tb getBitSet)
			}
			// copy the tmp set into mem
			mem(data+loc_trie_block_type) = intersection.getCurrSetType
			memcpy (
				mem, data+size_of_trie_block_head, 
				intersection.getMem, intersection.getCurrSet, 
				intersection.getCurrSetSize
			)
			// clear memory after building
			intersection.clearMem
			
			getSize
		}
*/
		// no furthur operation on this result so we only count.
		// for nonleaf set we still to do foreach so we put them
		// in an uint array
		// It returns the size of result set
		def build_aggregate_nonleaf (s: List[TrieBlock]): Rep[Int] = { 
			// put intersection in query_optc

			// if s.length == 1 ... else ...
			// we don't support 1 relation join yet.
			// Hint: when to output the result trie???
			// 			 can they be done in place???

			// we only do 2 set intersection here
			val a_set_type = s(0).getSetType
			val b_set_type = s(1).getSetType
			// intersection returns the size of the result set
			val size = 
				if (a_set_type == SetType.BitSet) { 
					if (b_set_type == SetType.BitSet) {
						// this gives a bitset
						intersection.setIntersection (s(0) getBitSet, s(1) getBitSet, mem, data)
					} else {
						// this gives a uint set
						intersection.setIntersection (s(1) getUintSet, s(0) getUintSet, mem, data)
					}
				} else { // uintset
					if (b_set_type == SetType.BitSet) {
						// this gives a uint set
						intersection.setIntersection (s(0) getUintSet, s(1) getBitSet, mem, data)
					} else {
						intersection.setIntersection (s(0) getUintSet, s(1) getUintSet, mem, data)
					}
				}
			size
		}
		// It returns the cardinality of result set.
		def build_aggregate_leaf (s: List[TrieBlock]): Rep[Long] = { 
			// put intersection in query_optc

			// if s.length == 1 ... else ...
			// we don't support 1 relation join yet.
			// Hint: when to output the result trie???
			// 			 can they be done in place???

			// we only do 2 set intersection here
			val a_set_type = s(0).getSetType
			val b_set_type = s(1).getSetType
			val count = 
				if (a_set_type == SetType.BitSet) { 
					if (b_set_type == SetType.BitSet) {
						intersection.setIntersection (s(0) getBitSet, s(1) getBitSet)
					} else {
						intersection.setIntersection (s(1) getUintSet, s(0) getUintSet)
					}
				} else { // uintset
					if (b_set_type == SetType.BitSet) {
						intersection.setIntersection (s(0) getUintSet, s(1) getBitSet)
					} else {
						intersection.setIntersection (s(0) getUintSet, s(1) getUintSet)
					}
				}
			count
		}
	}
}

