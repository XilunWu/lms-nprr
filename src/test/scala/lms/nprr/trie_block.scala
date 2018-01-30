package scala.lms.nprr

import scala.lms.common._

object trie_block_const {
	// Trie block head
	// type, size, 
	val loc_trie_block_type = 0
	val size_of_trie_block_head = 1

	// Set type
	val type_uint_set = 0
	val type_bit_set = 1
}

trait TrieBlock extends Set with SetIntersection{
	this: Dsl => 

	import trie_block_const._

	case class TrieBlock (
		val mem: Rep[Array[Int]],
		val data: Rep[Int]
	) {
		val set_data = data+size_of_trie_block_head

		def getType = mem(data+loc_trie_block_type)
		def getSize = {
			if (getType == type_uint_set) {
				val uintset = UintSet(mem, set_data)
				size_of_trie_block_head + uintset.getSize + uintset.getCard
			} else { // type_bit_set
				val bitset = BitSet(mem, set_data)
				size_of_trie_block_head + bitset.getSize + bitset.getIndexSize 
			}
		}
		def getCard = {
			if (getType == type_uint_set) {
				val uintset = UintSet(mem, set_data)
				uintset.getCard
			} else { // type_bit_set
				val bitset = BitSet(mem, set_data)
				bitset.getCard
			}
		}
		def getSet = set_data
		def getBitSet = BitSet (mem, set_data)
		def getUintSet = UintSet (mem, set_data)
		def getChildBlock (key: Rep[Int]) = {
			if (getType == type_uint_set) {
				val uintset = UintSet(mem, set_data)
				val index = uintset getIndexByKey key
				mem(data+size_of_trie_block_head+uintset.getSize+index)
			} else { // type_bit_set
				val bitset = BitSet(mem, set_data)
				val index = bitset getIndexByKey key
				mem(data+size_of_trie_block_head+bitset.getSize+index)
			}
		}
		def getChildBlockByIndex (index: Rep[Int]) =
			if (getType == type_uint_set) {
				val uintset = UintSet(mem, set_data)
				mem(data+size_of_trie_block_head+uintset.getSize+index)
			} else { // type_bit_set
				val bitset = BitSet(mem, set_data)
				mem(data+size_of_trie_block_head+bitset.getSize+index)
			}

		def refineIndex (index: Rep[Int], value: Rep[Int], c_addr: Rep[Int]) = {
			if (getType == type_uint_set) refineIndexByIndex (index, c_addr)
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
			val sparse_test = true
		  if (sparse_test) {
				mem (data+loc_trie_block_type) = type_uint_set
				val set = UintSet(mem, set_data)
				set.buildUintSet (arr, begin, end)
			}
			else {
				mem (data+loc_trie_block_type) = type_bit_set
				val set = BitSet(mem, set_data)
				set.buildBitSet (arr, begin, end)
			}
		}

		def build (s: List[TrieBlock]): Rep[Int] = { 
			val intersection = Intersection()
			// if s.length == 1 ... else ...
			// we don't support 1 relation join yet.
			val a_set_type = s(0).getType
			val b_set_type = s(1).getType
			if (a_set_type == type_bit_set) { 
				if (b_set_type == type_bit_set) {
					intersection.setIntersection (s(0) getBitSet, s(1) getBitSet)
				} else {
					intersection.setIntersection (s(1) getUintSet, s(0) getUintSet)
				}
			} else { // uintset
				if (b_set_type == type_bit_set) {
					intersection.setIntersection (s(0) getUintSet, s(1) getBitSet)
				} else {
					intersection.setIntersection (s(0) getUintSet, s(1) getUintSet)
				}
			}
			// the result is stored in the tmp memory of object "intersection"
			
			s.tail.tail.foreach { tb =>
				val next_set_type = tb.getType
				if (next_set_type == type_uint_set)
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
	}
}

