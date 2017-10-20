package scala.lms.nprr

import scala.lms.common._

trait Intersection extends Dsl with UncheckedOps {
	def simd_bitmap_intersection(res: Rep[Array[Int]], set_head: Rep[Int], n_rel: Rep[Int], arr: Rep[Array[Array[Int]]], start: Rep[Array[Int]], end: Rep[Array[Int]], min: Rep[Int]) = {
		val length = end(0)-start(0)
		/*
		println("intersection length = ")
		println(length)
		*/
		val num_of_ints = uncheckedPure[Int](
          "simd_bitmap_intersection((uint64_t *)", 
          res, 
          ", ",
          set_head,
          ", (uint64_t **)",
          arr, 
          ", (uint64_t *)",
          start,
          ", (uint64_t)",
          n_rel,
          ", (uint64_t)",
          length,
          ", (uint64_t)",
          min,
          ")"
        )
		num_of_ints
	}
}

trait ParIntersection extends Dsl with StagedQueryProcessor with UncheckedOps{

}

trait NprrJoinImp extends Trie with Intersection {
  // Option: Inline / not Inline
  // 1. Have an expanded if-then-else branch for each level
  // 2. Not expand at all
  
  def nprr_iterative (tries: List[BitTrie], schema: Schema): Rep[Unit] = {
  	var count = 0l
		val result = new ArrayBuffer (1 << 28)
		val tmp_store_length = 1 << 10
		// iterator(tid)(trie)
		// just 1 thread
		val iterator = tries map {t => 
			new BitTrieIterator(t)}
		val builder = new BitTrieBuilder(new BitTrie(result, schema))
		// Trie is now stored in prefix order instead
		var i = 0
		val data = tries(0).getData

  	var level = 0
  	join_on_level(0)

		println(count)

		def join_on_level(lv: Int): Rep[Unit] = {
			val tmp_store = NewArray[Int](tmp_store_length)
			val iterator_i = iterator.filter( t => t.getSchema.contains(schema(lv)))
			val result_set = builder.build_set(lv, iterator_i)  // we decode it here. 
																													// Use the decoded int to set iterator
			val len = result_set.getCardinality
			if (len >= tmp_store_length) {
				println("tmp store is not large enough!")
				// exit
			}
			result_set getUintSet tmp_store
			var i = 0 
			while (i < len) {
				if (lv == schema.length-1) {
					count += 1l
				} else {
					iterator_i foreach { t => 
						t.getChild(lv, tmp_store(i))
						join_on_level(lv+1)
					}
				}
				i += 1			
			}
		}
		// return len of intersection set
		/*
		def leapfrog_on_level(level: Int, 
			arr: Rep[Array[Array[Int]]], 
			head: Rep[Array[Int]]) = {
			var len = 0
			val number_of_relations = tries.filter( t => t.getSchema.contains(schema(level))).length
			val arr_pos = NewArray[Int](number_of_relations)
			// One round of leapfrog will make arr(i, set, pos) as max
			// and arr(i+1, set, pos) as min
			var curr_num = get_uint_trie_elem(arr(0), head(0), 0)            
			var i = 1
			while ( i < number_of_relations ) {
				arr_pos(i) = uint_trie_geq(arr(i), head(i), curr_num, 0)
				curr_num = get_uint_trie_elem(arr(i), head(i), arr_pos(i))
				//print("curr_num = "); print(curr_num)
				i += 1
			}

			i = 0
			while ( i >= 0 ) {
				// if we found an intersection
				val min = get_uint_trie_elem(arr(i), head(i), arr_pos(i))
				val max = 
					if (i == 0) get_uint_trie_elem(arr(number_of_relations - 1), 
						head(number_of_relations - 1), 
						arr_pos(number_of_relations - 1))
				else get_uint_trie_elem(arr(i - 1), head(i - 1), arr_pos(i - 1))
				//print("max = "); print(max); print("; min = "); println(min)
				if (max == min) {
					inter_data update (level, len, max)
					len += 1
					arr_pos(i) = arr_pos(i) + 1
				} else {
					arr_pos(i) = uint_trie_geq(arr(i), head(i), max, arr_pos(i))
				}
				if (uint_trie_reach_end(arr(i), head(i), arr_pos(i))) i = -1
				else if (i == number_of_relations - 1) i = 0
				else i += 1
			}
			len
		}
		*/
	}

	def nprr_lambda (tries: List[BitTrie], schema: Schema): Rep[Unit] = {
		var count = 0l
		val result = new ArrayBuffer (1 << 28)
		// iterator(tid)(trie)
		// just 1 thread
		val iterator = tries map {t => 
			new BitTrieIterator(t)}
		val builder = new BitTrieBuilder(new BitTrie(result, schema))
		// Trie is now stored in prefix order instead
		var i = 0
		val data = tries(0).getData

		nprr_subtrie(0).apply(0)
		print("count = ")
		println(count)

		def getResultTrie = builder.getResultTrie
		// write func (Rep[A] => Rep[B]) and pass it as lambda
		// by def fundef[A,B](f: Rep[A] => Rep[B]): Rep[A] => Rep[B] =
			// 			(x: Rep[A]) => lambda(f).apply(x)
		// Rep[A] => Rep[B] <-> Rep[A => B] (apply / fun)
		// Note: foreach can be done differently for uint/bitset
		def nprr_subtrie (level: Int): Rep[Int=>Unit] = fun { x: Rep[Int] => // x is the parent of set on level
			val is_last_attr = (level == schema.length-1)
			// find children sets on level of value x.
			// intersect those sets.

			// open on level-1 for x
			if (level > 0) {
				val iterator_x = iterator.filter( t => t.getSchema.contains(schema(level-1)))
				(iterator_x.map{ t => 
					t.getSchema indexOf schema(level-1)}, 
					iterator_x).zipped.foreach { (lv, t) =>
					if (lv != t.getSchema.length-1)
						t.getChild(lv, x)
				}
			}

			val iterator_i = iterator.filter( t => t.getSchema.contains(schema(level)))
			// Don't forget to build the trie
			val result_set = builder.build_set(level, iterator_i)
			// println(builder.next_set_to_build)
			// println(result_set getCardinality)

			if (is_last_attr) 
			{
				count = count + (result_set getCardinality).toLong
				unit()  // necessary???
			}
			else {
				result_set foreach nprr_subtrie(level+1) // foreach will setup iterator
				// don't forget to set up index
			}
		}
	}


  // search functions for UInteTrie
  /*
  def get_uint_trie_elem(arr: Rep[Array[Int]], head: Rep[Int], index: Rep[Int]) = {
    val start_of_elem = trie_const.sizeof_uint_set_header
    arr(head + start_of_elem + index)
  }
  def uint_trie_reach_end(arr: Rep[Array[Int]], head: Rep[Int], index: Rep[Int]) = {
    val card = arr(head + trie_const.loc_of_cardinality)
    index >= card
  }
  // function uint_trie_geq returns the index of first element in array
  // which is no less than value. 
  def uint_trie_geq(arr: Rep[Array[Int]], head: Rep[Int], value: Rep[Int], init_start: Rep[Int]) = {
    val card = arr(head + trie_const.loc_of_cardinality)
    //print("card = "); println(card)
    var start = head + trie_const.sizeof_uint_set_header + init_start
    var end = head + trie_const.sizeof_uint_set_header + card
    // search among arr(start) and arr(end)
    var size = end - start
    while (size > 5) {
      val mid_point = (start + end) / 2
      val pivot = arr(mid_point)
      if (pivot < value) start = mid_point + 1
      else if (pivot > value) end = mid_point + 1
      else {
        start = mid_point
        end = mid_point + 1
      }
      size = end - start
    }
    var i = 0
    while (i < size && arr(start + i) < value) {
      //print("curr_v = "); print(arr(start+i)); print(" < value = "); println(value)
      i += 1
    }
    (start + i) - (head + trie_const.sizeof_uint_set_header)
  }
  */
}
