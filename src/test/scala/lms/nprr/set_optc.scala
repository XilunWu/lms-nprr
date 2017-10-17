package scala.lms.nprr

import scala.lms.common._

object set_const {
	object trie_const{

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
}
class Set (val addr: Rep[Array[Int]], val head: Rep[Int]) {
	import set_const._

	val set_type = addr(head+loc_type)
	val set_cardinality = addr(head+loc_cardinality)
	val set_range = addr(head+loc_range)
	val set_min = addr(head+loc_min)

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
			set_range/bits_per_int,
			", ",
			set_min
		)
		var i = 0
		while (i < num) {
			f values(i)
		}
	}
}
