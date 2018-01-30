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
	val BITS_PER_INT = 32
	val BITS_PER_WORD = 64
	val BITS_IN_AVX_REG = 256 // or 128 for SSE2. It's also defined as macro in c header
	val BITS_PER_INT_SHIFT = 5
	val BITS_PER_WORD_SHIFT = 6
	val BYTES_PER_INT = 4
	val BYTES_PER_WORD = 8
}
// Index is the absolute addr of child set in memory pool

trait Set extends UncheckedOps {
	this: Dsl =>

	import set_const._

	def memcpy (dest: Rep[Array[Int]], dest_start:Rep[Int],
		src: Rep[Array[Int]], src_start: Rep[Int], len: Rep[Int]) = {
		unchecked[Unit] ( "memcpy((char *)", dest,
			" + ", BYTES_PER_INT*dest_start, ", ",
			"(char *)", src, " + ", BYTES_PER_INT*src_start, ", ", 
			"(size_t) ", BYTES_PER_INT*len, ")"
		)
	}

	case class BitSet (
		mem: Rep[Array[Int]], 
		data: Rep[Int]) {

		def getMem = mem
		def getData = data+size_bit_set_head
		def getLen = mem(data+loc_bit_set_len)
		def getCard = mem(data+loc_bit_set_card)
		def getMin = mem(data+loc_bit_set_min)
		def getSize = size_bit_set_head + getLen
		def getIndexSize = getLen << BITS_PER_INT_SHIFT
		def getIndexByKey(key: Rep[Int]): Rep[Int] = {
			val index = key - getMin
			index
		}

		def buildBitSet (arr: Rep[Array[Int]],
		 begin: Rep[Int], end: Rep[Int]) = {
			mem(data+loc_bit_set_card) = end - begin
			val min = (arr(begin) >>> BITS_PER_INT_SHIFT) << BITS_PER_INT_SHIFT
			val max = arr(end - 1)
			mem(data+loc_bit_set_len) = ((max - min) >>> BITS_PER_INT_SHIFT) + 1
			mem(data+loc_bit_set_min) = min

			var i = begin
			var curr_int = 0
			var bit_int = 0
			var min_in_curr_int = min
			while (i < end) {
				val value = arr(i)
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
			if (bit_int != 0) {
				mem(data+size_bit_set_head+curr_int) = bit_int
			}
			// print("build set head = "); println(data+size_bit_set_head)
		}

		def foreach (f: Rep[Int] => Rep[Unit]): Rep[Unit] = {
			var i = 0
			val arr = NewArray[Int](getCard)
			val num = unchecked[Int] (
				"decode ((uint32_t *) ", arr, 
				", (uint32_t *) ", mem, 
				", (size_t) ", getData, 
				", (size_t) ", getLen,
				", (uint32_t) ", getMin,
				")"
			)
			/*
			print("card = "); println(getCard)
			print("return val = "); println(num)
			print("min = "); println(getMin)
			print("len = "); println(getLen)
			print("data+head = "); println(getData)
			*/
			/*
			if (num != getCard) {
				println("error in bitset foreach (): decode!")
				print("card = "); println(getCard)
				print("return val = "); println(num)
				print("min = "); println(getMin)
				print("len = "); println(getLen)
				print("data+head = "); println(getData)
				println("dump the set:")
				while (i < getLen) {
					print("int getData+ i = "); print(getData + i)
					print(", bit = "); println(mem(getData + i))
					i += 1
				}
				i = 0
				while (i < getCard) {
					println(arr(i))
					i += 1
				}
			}
			*/
			while (i < getCard) {
				f (arr(i))
				i += 1
			}
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
	}

	case class UintSet (
		mem: Rep[Array[Int]], 
		data: Rep[Int]) {

		def getMem = mem
		def getData = data+size_uint_set_head
		def getLen = mem(data+loc_uint_set_len)
		def getCard = getLen
		def getSize = size_uint_set_head + getCard
		def getKeyByIndex(index: Rep[Int]): Rep[Int] = 
			mem(data+size_uint_set_head+index)
		def getIndexByKey (key: Rep[Int]): Rep[Int] = {
			val addr = data+size_uint_set_head

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
			searchInRange (0, getCard)
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

		def foreach (f: Rep[Int] => Rep[Unit]): Rep[Unit] = {
			var i = 0
			while ( i < getCard ) {
				f (mem(data+size_uint_set_head+i))
				i += 1
			}
		}

		def foreach (f: Rep[Int => Unit]): Rep[Unit] = {
			var i = 0
			while ( i < getCard ) {
				f.apply(mem(data+size_uint_set_head+i))
				i += 1
			}
		}

		def foreach_index (f: Rep[Int] => Rep[Unit]): Rep[Unit] = {
			var i = 0
			while ( i < getCard ) {
				f (i)
				i += 1
			}
		}

		def foreach_index (f: Rep[Int => Unit]): Rep[Unit] = {
			var i = 0
			while ( i < getCard ) {
				f.apply(i)
				i += 1
			}
		}
	}
}


