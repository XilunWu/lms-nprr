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
	import trie_block_const._

	case class TrieBlock (
		val mem: Rep[Array[Int]],
		val data: Rep[Int]
	) {
		val set = Set (mem, data+size_of_trie_block_head)

		def getType = mem(data+loc_trie_block_type)
		def getSize = {
			if (getType == type_uint_set) {
				val uintset = UintSet(set.mem, set.data)
				size_of_trie_block_head + uintset.getSize + uintset.getCard
			} else { // type_bit_set
				val bitset = BitSet(set.mem, set.data)
				size_of_trie_block_head + bitset.getSize + bitset.getIndexSize 
			}
		}
		def getCard = {
			if (getType == type_uint_set) {
				val uintset = UintSet(set.mem, set.data)
				uintset.getCard
			} else { // type_bit_set
				val bitset = BitSet(set.mem, set.data)
				bitset.getCard
			}
		}
		def getSet = set
		def getBitSet = BitSet (mem, data+size_of_trie_block_head)
		def getUintSet = UintSet (mem, data+size_of_trie_block_head)
		def getChildBlock (key: Rep[Int]) = {
			if (getType == type_uint_set) {
				val uintset = UintSet(set.mem, set.data)
				val index = uintset getIndexByKey key
				mem(data+size_of_trie_block_head+uintset.getSize+index)
			} else { // type_bit_set
				val bitset = BitSet(set.mem, set.data)
				val index = bitset getIndexByKey key
				mem(data+size_of_trie_block_head+bitset.getSize+index)
			}
		}
		def refineIndex (index: Rep[Int], value: Rep[Int], c_addr: Rep[Int]) = {
			getType match {
				case type_uint_set => 
					refineIndexByIndex (index, c_addr)
				case type_bit_set =>
					refineIndexByValue (value, c_addr)
				case _ => 
			}
		}
		def refineIndexByIndex (index: Rep[Int], c_addr: Rep[Int]) = {
			val uintset = UintSet(set.mem, set.data)
			val start = uintset.getSize
			mem(set.data+start+index) = c_addr
		}
		def refineIndexByValue (value: Rep[Int], c_addr: Rep[Int]) = {
			val bitset = BitSet(set.mem, set.data)
			val min = bitset.getMin
			val start = bitset.getSize
			mem(set.data+start+value-min) = c_addr
		}
		def buildFromRawData (
			arr: Rep[Array[Int]], begin: Rep[Int], end: Rep[Int]) = {
			// build TrieBlock head
			val sparse = 
				if ( arr(end-1) - arr(begin) < set_const.BITS_IN_AVX_REG * (end - begin) ) false
				else true
			if (sparse) {
				mem (data+loc_trie_block_type) = type_uint_set
				set.buildUintSet (arr, begin, end)
			}
			else {
				mem (data+loc_trie_block_type) = type_bit_set
				set.buildBitSet (arr, begin, end)
			}
		}

		def build (List[TrieBlock]): Rep[Int] = { 
			var result_set_type = -1
			// if s.length == 1 ... else ...
			// we don't support 1 relation join yet.
			val a_set = s(0) getSet
			val b_set = s(1) getSet
			// the result is stored in the tmp memory of object "intersection"
			result_set_type = intersection.setIntersection(a_set, b_set)
			var i = 2
			while (i < s.length) { 
				// the result is stored in the tmp memory of object "intersection"
				result_set_type = intersection.setIntersection(s(i) getSet)
				i += 1 
			} 
			// copy the tmp set into mem
			mem(data+loc_trie_block_type) = result_set_type
			memcpy (
				mem, data+size_of_trie_block_head, 
				intersection.getMem, intersection.getSet, 
				intersection.getSetSize
			)
			// clear memory after building
			intersection.clearMem
			getSize
		}
	}

