package scala.lms.nprr

import scala.lms.common._

trait Set extends Dsl with StagedQueryProcessor with UncheckedOps {
	object set_const {
    val loc_type = 0
    val loc_cardinality = 1
    // bitset:  size = header + range + 64*range
    // uintvec: szie = header + 2 * cardinality
    val loc_range = 2  // range = (max - min)/64
    val loc_min = 3

    val sizeof_bitset_header = 4
    val sizeof_uintvec_header = 4

    val type_uintvec = 1
    val type_bitmap = 2
    val type_hybrid = 3

    val bytes_per_int = 8
    val bits_per_int = 8 * bytes_per_int
	}
	case class Set (mem: Rep[Array[Int]], head: Rep[Int]) {
		def get_cardinality = mem(head)
		def get_sibling_set = { // calculate set size and index size
			val size = 4 + 2 * mem(head)
			head + size
		} 
		def set_index (index_in_set: Rep[Int], index: Rep[Int]) = {
			mem(head+4+mem(head)+index_in_set) = index
		}
	}
	/*
	case class Set (val addr: Rep[Array[Int]], val head: Rep[Int]) {
		import set_const._

		// val set_type = addr(head+loc_type)
		val set_type = type_bitmap
		val set_cardinality = addr(head+loc_cardinality)
		val set_range = addr(head+loc_range)
		val set_min = addr(head+loc_min)

		def getSize = {
			if (set_type == type_bitmap) {
				val size = sizeof_bitset_header + (bits_per_int + 1) * set_range 
				size
			} else {
				val size = sizeof_uintvec_header + 2 * set_cardinality
				size
			}
		}
		def getCardinality = set_cardinality
		def getChild(x: Rep[Int]): Rep[Int] = {
      val index = x-set_min
      return addr(head+sizeof_bitset_header+set_range+index)
		}
		def getMin = set_min
		def getMax = set_min + bits_per_int * set_range
		def findByValue(x: Rep[Int]): Rep[Int] = {
			val index = x-set_min
			return head + sizeof_bitset_header + index / 64
		}
		def getUintSet (dest: Rep[Array[Int]]): Rep[Unit] = {
			/*
				if (set_type == type_bitmap) 
				TODO: make it hybrid
			*/
			val num = uncheckedPure[Int](
				"decode ((uint64_t *)",
				dest,
				", (uint64_t *)",
				addr,
				", ",
				head+sizeof_bitset_header,
				", ",
				set_range,
				", ",
				set_min,
				")"
			)
		}
		def foreach (f: Rep[Int=>Unit]): Rep[Unit] = {
			/*
				if (set_type == type_bitmap) 
				TODO: make it hybrid
			*/
			bitset_foreach(f)
		}

		def bitset_foreach (f: Rep[Int=>Unit]): Rep[Unit] = {
			val values = NewArray[Int](set_cardinality)
			val num = uncheckedPure[Int](
				"decode ((uint64_t *)",
				values,
				", (uint64_t *)",
				addr,
				", ",
				head+sizeof_bitset_header,
				", ",
				set_range,
				", ",
				set_min,
				")"
			)
			/*
			println(set_cardinality)
			var i = 0
			while (i < set_cardinality) {println(values(i)); i += 1}
			*/
			var i = 0
			while (i < num) { 
				/*
				print("parent: = ")
				println(values(i))
				*/
				f(values(i)); 
				i += 1
			}
		}
	}
	*/
}
