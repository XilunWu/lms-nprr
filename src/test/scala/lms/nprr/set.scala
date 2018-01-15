package scala.lms.nprr

import scala.lms.common._

object set_const {
	// Set head
	val loc_set_size = 0  // the size of set. In sizeof(Int)
	val loc_set_type = 1
	val loc_set_card = 2  // the cardinality of set
	val loc_set_range = 3  // range = max - min
	val loc_set_min = 4
	val loc_set_body_size = 5

	val size_set_head = 6
	// Set body

	// Set type
	val type_uint_set = 0
	val type_bit_set = 1

	// Set specific const
	val BITS_PER_INT = 64
	val BITS_PER_INT_SHIFT = 6
}
// Index is the absolute addr of child set in memory pool

trait Set extends UncheckedOps {
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
		def build (s1: BaseSet) = {}
		def build (s1: BaseSet, s2: BaseSet) = {}
		def build (s: List[BaseSet]) = {

		}
	}

	abstract class Set {
		val mem: Rep[Array[Int]]
		val data: Rep[Int]

		def getType: Rep[Int] = mem(data+loc_set_type) // Enum type?
		def getCardinality: Rep[Int] = mem(data+loc_set_card) // cardinality of set
		def getSize: Rep[Int] = mem(data+loc_set_size) // in unit sizeof(Int)
		def getKey(offset: Rep[Int]) = mem(data+offset)
		def getRange = mem(data+loc_set_range)
		def getMin = mem(data+loc_set_min)
		def getNextKey(key: Rep[Int]): Rep[Int]  // return offset of the appropriate key
		def getChild(key: Rep[Int]): Rep[Int]    // return head of the child set
		def getKeyGTE(key: Rep[Int]): Rep[Int]  // return addr of the appropriate key
		def foreach(f: Rep[Int] => Rep[Unit]): Rep[Unit]
	}

	class BaseSet (
		val mem: Rep[Array[Int]], 
		val data: Rep[Int]) extends Set {

		def getNextKey(key: Rep[Int]): Rep[Int] = {
			0
		}
		def getChild(key: Rep[Int]): Rep[Int] = {
			0
		}
		def getKeyGTE(key: Rep[Int]): Rep[Int] = {
			0
		}
		def foreach (f: Rep[Int] => Rep[Unit]): Rep[Unit] = {

		}
	}

	class BitSet (
		m: Rep[Array[Int]], 
		d: Rep[Int]) extends BaseSet (m, d) {

		override def getChild(key: Rep[Int]): Rep[Int] = {
			val index = key - getMin
			val start = data+size_set_head+mem(data+loc_set_body_size)
			mem(start+index)
		}

		override def foreach (f: Rep[Int] => Rep[Unit]): Rep[Unit] = {
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

	class UIntSet (
		m: Rep[Array[Int]], 
		d: Rep[Int]) extends BaseSet (m, d){

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

		override def foreach (f: Rep[Int] => Rep[Unit]): Rep[Unit] = {
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


