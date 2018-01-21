package scala.lms.nprr

import scala.lms.common._

object set_const {
	// Set head
	// UintSet: len
	// BitSet: len, card, min
	val loc_uint_set_len = 0
	val loc_bit_set_len = 0
	val loc_bit_set_card = 1
	val loc_bit_set_min = 2

	val size_uint_set_head = 1
	val size_bit_set_head = 3

	// Set type
	val type_uint_set = 0
	val type_bit_set = 1

	// Set specific const
	val BITS_PER_INT = 64
	val BITS_PER_INT_SHIFT = 6	
}
// Index is the absolute addr of child set in memory pool

trait Set extends UncheckedOps with SetIntersection {
	this: Dsl =>

	import set_const._

	class SetBuilder (
		val mem: Rep[Array[Int]],
		val data: Rep[Int]) {

		def build (
			arr: Rep[Array[Int]], 
			begin: Rep[Int], 
			end: Rep[Int]): Set = {

			def buildUintSet = {
				var i = begin
				while (i < end) {
					// TODO: replace with memcpy 
					mem(data+size_set_head+i-begin) = arr(i)
					i += 1
				}
			}

			def buildBitSet = {
				// Build Bitset from Int Array
				val min = (arr(begin) >> BITS_PER_INT_SHIFT) << BITS_PER_INT_SHIFT
				var i = begin
				var curr_int = 0
				var bit_int = 0
				var min_in_curr_int = min
				while (i < end) {
					val value = arr(i)
					// print("value = "); println(value)
					while (value - min_in_curr_int >= BITS_PER_INT) {
						// unchecked[Unit]("printf(\"%x\\n\", ", readVar(bit_int), ")")
						mem(data+size_set_head+curr_int) = bit_int
						min_in_curr_int += BITS_PER_INT
						curr_int += 1
						bit_int = 0
					}
					val diff = value - min_in_curr_int
					val last_bit = uncheckedPure[Int]("1l << ", (BITS_PER_INT - diff - 1))
					bit_int = bit_int | last_bit
					i += 1
				}
				if (bit_int != 0) mem(data+size_set_head+curr_int) = bit_int
			}

			if ((arr(end-1) - arr(begin)) < 32 * (end-begin)) {  // build dense set
				// Build set head
				mem(data+loc_set_type) = type_bit_set
				mem(data+loc_set_card) = end - begin
				val min = (arr(begin) >> BITS_PER_INT_SHIFT) << BITS_PER_INT_SHIFT
				val max = arr(end - 1)
				val body_size = ((max - min) >> BITS_PER_INT_SHIFT) + 1
				val index_size = body_size << BITS_PER_INT_SHIFT
				mem(data+loc_set_min) = min
				mem(data+loc_set_range) = body_size << BITS_PER_INT_SHIFT
				mem(data+loc_set_size) = size_set_head + body_size + index_size
				mem(data+loc_set_body_size) = body_size
				// set head is done building

				// Build bit set body
				buildBitSet

				// Build set index? We reserve space for index
				// but refine them when child sets are built
			} 
			else {  // build sparse set
				// Build set head
				mem(data+loc_set_type) = type_uint_set
				mem(data+loc_set_card) = end - begin
				mem(data+loc_set_range) = arr(end-1) - arr(begin)
				mem(data+loc_set_min) = arr(begin)
				mem(data+loc_set_size) = size_set_head + 2 * (end - begin)
				mem(data+loc_set_body_size) = mem(data+loc_set_card)

				// Build bit set body
				buildUintSet

				// Build set index? We reserve space for index
				// but refine them when child sets are built
			}
			new BaseSet (mem, data)
		}
		def refineIndex (index: Rep[Int], value: Rep[Int], c_addr: Rep[Int]) = {
			mem(data+loc_set_type) match {
				case type_uint_set => 
					refineIndexByIndex (index, c_addr)
				case type_bit_set =>
					refineIndexByValue (value, c_addr)
				case _ => 
			}
		}
		def refineIndexByIndex (index: Rep[Int], c_addr: Rep[Int]) = {
			val start = mem(data+loc_set_body_size) + size_set_head
			mem(data+start+index) = c_addr
		}
		def refineIndexByValue (value: Rep[Int], c_addr: Rep[Int]) = {
			val min = mem(data+loc_set_min)
			val start = mem(data+loc_set_body_size) + size_set_head
			mem(data+start+value-min) = c_addr
		}
		def build (s1: BaseSet) = s1
		def build (s1: BaseSet, s2: BaseSet): BaseSet = {
			
		}
		private def build (s1: UIntSet, s2: UIntSet): BaseSet = {
			s2
		}
		private def build (s1: UIntSet, s2: BitSet): BaseSet = {
			s2
		}
		private def build (s1: BitSet, s2: BitSet): BaseSet = {
			s2
		}
		private def find_min_in_bitset (bit_arr: Rep[Int]) = {
			uncheckedPure[Int](
				"__builtin_clzll(",
				"(uint64_t) ", bit_arr,
				")"
			)
		}
		private def find_max_in_bitset (bit_arr: Rep[Int]) = {
			val tzeros = uncheckedPure[Int](
				"__builtin_ctzll(",
				"(uint64_t) ", bit_arr,
				")"
			)
			val max = BITS_PER_INT - tzeros - 1
			max
		}
		/*
		private def buildBitsetHeadFromBitArray (
			bit_arr: Rep[Int], bit_arr_start: Rep[Int], bit_arr_len: Rep[Int],  
			bit_arr_min: Rep[Int], bit_arr_card: Rep[Int],
			target_mem: Rep[Int], target_mem_start: Rep[Int]
		) = {
			// Build set head
			target_mem(target_mem_start+loc_set_type) = type_bit_set
			target_mem(target_mem_start+loc_set_card) = bit_arr_card
			val body_size = bit_arr_len
			val index_size = body_size << BITS_PER_INT_SHIFT
			target_mem(target_mem_start+loc_set_min) = bit_arr_min
			target_mem(target_mem_start+loc_set_range) = body_size << BITS_PER_INT_SHIFT
			target_mem(target_mem_start+loc_set_size) = size_set_head + body_size + index_size
			target_mem(target_mem_start+loc_set_body_size) = body_size
		}
		*/
		def build (s: List[BaseSet]): BaseSet = {
			// if s.length == 1 ... else ...
			// we don't support 1 attribute yet.
			val a_set = s(0) getSimpleSet
			val b_set = s(1) getSimpleSet
			intersection.setIntersection(a_set, b_set)
			var i = 2
			while (i < s.length) { //
				intersection.setIntersection(s(i) getSimpleSet)
				i += 1 
			} 
			// copy the tmp set into mem
			// ...

			// clear memory after building
			intersection.clearMem

		}
	}
/*
	abstract class Set {
		val mem: Rep[Array[Int]]
		val data: Rep[Int]

		def getType: Rep[Int] = mem(data+loc_set_type) // Enum type?
		def getCardinality: Rep[Int] = mem(data+loc_set_card) // cardinality of set
		def getSize: Rep[Int] = mem(data+loc_set_size) // in unit sizeof(Int)
		def getKey(offset: Rep[Int]) = mem(data+offset)
		def getRange = mem(data+loc_set_range)
		def getMin = mem(data+loc_set_min)
		def getBodySize = mem(data+loc_set_body_size)
		def getMem = mem
		def getBody = data + size_set_head
		def getChild(key: Rep[Int]): Rep[Int]    // return head of the child set
	}
*/
	class Set (
		val mem: Rep[Array[Int]], 
		val data: Rep[Int]) extends Set {

		def buildBitSet (arr: Rep[Array[Int]],
		 begin: Rep[Int], end: Rep[Int]) = {
			mem(data+loc_bit_set_card) = end - begin
			val min = (arr(begin) >> BITS_PER_INT_SHIFT) << BITS_PER_INT_SHIFT
			val max = arr(end - 1)
			mem(data+loc_bit_set_len) = ((max - min) >> BITS_PER_INT_SHIFT) + 1
			mem(data+loc_bit_set_min) = min

			var i = begin
			var curr_int = 0
			var bit_int = 0
			var min_in_curr_int = min
			while (i < end) {
				val value = arr(i)
				// print("value = "); println(value)
				while (value - min_in_curr_int >= BITS_PER_INT) {
					// unchecked[Unit]("printf(\"%x\\n\", ", readVar(bit_int), ")")
					mem(data+size_bit_set_head+curr_int) = bit_int
					min_in_curr_int += BITS_PER_INT
					curr_int += 1
					bit_int = 0
				}
				val diff = value - min_in_curr_int
				val last_bit = uncheckedPure[Int]("1l << ", (BITS_PER_INT - diff - 1))
				bit_int = bit_int | last_bit
				i += 1
			}
			if (bit_int != 0) mem(data+size_bit_set_head+curr_int) = bit_int
		}

		def buildUintSet (arr: Rep[Array[Int]],
		 begin: Rep[Int], end: Rep[Int]) = {
			mem(data+loc_uint_set_len) = end - begin			
		 	// build uint set head
			var i = begin
			while (i < end) {
				// TODO: replace with memcpy 
				mem(data+size_uint_set_head+i-begin) = arr(i)
				i += 1
			}
		}



		def getChild(key: Rep[Int]): Rep[Int] = {
			val typ = getType
			if (typ == type_uint_set) {
				val concrete_set = new UIntSet (mem, data)
				concrete_set getChild key
			}	else if (typ == type_bit_set) {
				val concrete_set = new BitSet (mem, data)
				concrete_set getChild key
			} else 0
		}

		//	 Need modify data structure
		def getSimpleSet: SimpleSet = 
			SimpleSet(mem, data+size_set_head)
	}

	class BitSet (
		m: Rep[Array[Int]], 
		d: Rep[Int]) extends BaseSet (m, d) {

		def getLen = mem(data+loc_bit_set_len)
		def getCard = mem(data+loc_bit_set_card)
		def getMin = mem(data+loc_bit_set_min)
		def getSize = size_bit_set_head + getLen
		override def getChild(key: Rep[Int]): Rep[Int] = {
			val index = key - getMin
			val start = data+size_set_head+mem(data+loc_set_body_size)
			mem(start+index)
		}

		def foreach (f: Rep[Int] => Rep[Unit]): Rep[Unit] = {
			var i = 0
			var j = 0
			val arr = NewArray[Int](getCardinality)
			val num = uncheckedPure[Int] (
				"decode ((uint64_t *) ", arr, 
				", (uint64_t *) ", mem, 
				", (uint64_t) ", data+size_set_head, 
				", (uint64_t) ", mem(data+loc_set_body_size),
				", (uint64_t) ", getMin,
				")"
			)
			if (num != getCardinality) {
				println("error in bitset foreach (): decode!")
				print("card = "); println(getCardinality)
				print("return val = "); println(num)
			}

			while (i < getCardinality) {
				f (arr(i))
				i += 1
			}
		}
	}

	class UintSet (
		m: Rep[Array[Int]], 
		d: Rep[Int]) extends BaseSet (m, d){

		def getLen = mem(data+loc_uint_set_len)
		def getCard = getLen
		def getSize = size_uint_set_head + getCard
		def getKeyByIndex(index: Rep[Int]): Rep[Int] = 
			mem(data+size_set_head+index)
		def getChildByIndex(index: Rep[Int]): Rep[Int] = {
			val start = data+size_set_head+mem(data+loc_set_body_size)
			mem(start+index)
		}
		override def getChild(key: Rep[Int]): Rep[Int] = {
			val index = getIndexByKey (key)
			val start = data+size_set_head+mem(data+loc_set_body_size)
			mem(start+index)
		}

		def getIndexByKey (key: Rep[Int]): Rep[Int] = {
			val addr = data+size_set_head

			// Can LMS handle this recursion?
			/*
			def searchInRange (begin: Rep[Int], end: Rep[Int]): Rep[Int] = {
				if (end - begin <= 5) {
					var i = 0
					while (begin + i < end && mem(addr+begin+i) < key) i += 1
					begin+i
				} else {
					val pivot = (begin + end) / 2
					if (mem(addr+pivot) == key) pivot
					else if (mem(addr+pivot) < key) searchInRange (pivot, end)
					else searchInRange (begin, pivot)
				}	
			}
			*/
			def searchInRange (begin: Rep[Int], end: Rep[Int]): Rep[Int] = {
				var b = begin
				var e = end
				while (e - b >= 5) {	
					val pivot = (b + e) / 2
					if (mem(addr+pivot) == key) {
						b = pivot
						e = pivot + 1
					}
					else if (mem(addr+pivot) < key) {
						b = pivot
					}
					else  {
						e = pivot  // pivot + 1 in seek()
					}
				}
				var res = b
				while (mem(addr+res) != key) res += 1
				res
			}
			searchInRange (0, mem(data+loc_set_card))
		}

		def foreach (f: Rep[Int] => Rep[Unit]): Rep[Unit] = {
			var i = 0
			while ( i < getCardinality ) {
				f (mem(data+size_set_head+i))
				i += 1
			}
		}
		def foreach_index (f: Rep[Int] => Rep[Unit]): Rep[Unit] = {
			var i = 0
			while ( i < getCardinality ) {
				f (i)
				i += 1
			}
		}
	}
}


