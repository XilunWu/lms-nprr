package scala.lms.nprr

import scala.lms.common._

trait Intersection extends Dsl with StagedQueryProcessor with UncheckedOps{
	def simd_bitmap_intersection(res: Rep[Array[Int]], n_rel: Rep[Int], arr: Rep[Array[Array[Int]]], start: Rep[Array[Int]], end: Rep[Array[Int]], min: Rep[Int]) = {
		val length = end(0)-start(0)
		val num_of_ints = uncheckedPure[Int](
          "simd_bitmap_intersection((uint64_t *)", 
          res, 
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
	def nprr_recursive = {}
	def nprr_iterative (tries: List[BitTrie], schema: Schema): Rep[Unit] = {
			val count = var_new[Long](0l)
      val curr_set = new Matrix(schema.length, tries.length)
      val inter_data = new Matrix( schema.length, 1 << 18 )
      val curr_inter_data_index = NewArray[Int](schema.length)
      val inter_data_len = NewArray[Int](schema.length)

      var level = 0
      // init on curr_set(0), curr_inter_data_index(0), inter_data(0), and inter_data_len(0)
      init
      // println(inter_data_len(0))
      while (level >= 0) {
        // print("curr level: ")
        // println(level)
        if (level == schema.length - 1) {
          // yld each result because we've found them (stored in inter_data)
          intersect_on_level(schema.length - 1)
          // TODO: yld(tuple)
          var row = 0
          var i = 0
          // yld -> a lambda expression. A foreach function 
          // on the result of join/intersection
          while (i < inter_data_len(schema.length - 1)) {
            count += 1l
            i += 1
          }
          // next(): then find next in set on level
          if (schema.length > 1)
            curr_inter_data_index( schema.length - 2 )  =  
              curr_inter_data_index( schema.length - 2 ) + 1
          // up(): 1 level up
          level -= 1
        } 
        else if ( level == 0 ) { 
          if (schema.length > 1)
            level = join_on_level( 0 )
          unit() 
        }
        else if ( level == 1 ) { 
          if (schema.length > 2)
            level = join_on_level( 1 ) 
          unit()
        }
        else {} //Empty
      }

      println(count)

      def join_on_level(level: Int): Rep[Int] = {
        if ( atEnd( level )) {
          // up() 
          val new_level = level - 1
          // next() if not the first attribute
          if ( level != 0 ) 
            curr_inter_data_index( level - 1 ) = 
              curr_inter_data_index( level - 1 ) + 1
          // level -= 1
          new_level
        } else {
          // println(inter_data(level, curr_inter_data_index(level)))
          val new_level = level + 1
          // open(): update curr_set ( new_level )
          tries.filter( t => t.getSchema.contains(schema(level + 1))).foreach { it =>
            // col in Matrix
            val relation = tries indexOf it
            // row in Matrix
            val s = it.getSchema
            val attr_index = s indexOf ( schema ( level + 1))
            // if it's the first attr. don't update curr_set
            // otherwise, assign curr_set to the child set 
            // of the curr set on the previous level
            if (attr_index != 0) {
              val prev_attr = s( attr_index - 1 )
              val prev_attr_index = schema indexOf prev_attr              
              val curr_int = inter_data ( 
                prev_attr_index, 
                curr_inter_data_index( prev_attr_index ))    
              val child = it.findChildSetByValue( 
                curr_set (prev_attr_index, relation),
                curr_int
                )
              if (child <= 0) println("wrong child offset!")
              curr_set update ( level + 1, relation, child )
            }
          }
          val len = intersect_on_level( level + 1 )
          // modify curr_set (= child), curr_inter_data_index (= 0), and inter_data_len
          curr_inter_data_index( level + 1 ) = 0
          inter_data_len( level + 1 ) = len
          // level += 1
          new_level
        }
      }
      def atEnd (level : Int) : Rep[Boolean] = 
        curr_inter_data_index(level) >= inter_data_len(level)
      def intersect_on_level (level : Int) : Rep[Int] = 
        // intersect_on_level_leapfrog(level)
        // We switch to bit intersection for now
        intersect_on_level_bitmap(level)

      def intersect_on_level_bitmap (level : Int) : Rep[Int] = {
        // return the cardinality of result set
        // intersect
        // and put result into inter_data ( level )
        val it = tries.filter( t => t.getSchema.contains(schema(level)))
        val arr = NewArray[Array[Int]]( it.length )
        it foreach { t =>
            arr(it indexOf t) = t.getTrie
        }
        val head = NewArray[Int]( it.length )
        it foreach { t =>
            // print("set: "); println(curr_set(level, tries indexOf t))
            head(it indexOf t) = curr_set(level, tries indexOf t)
        }

        // Step1: align all bitmaps
        val start = NewArray[Int]( it.length )
        val end = NewArray[Int]( it.length )
        var min = it.head.getMinInBitset(head(0)) // max of min's
        var max = it.head.getMaxInBitset(head(0)) // min of max's
        // Step1.1: find the overlap of bitmaps
        it foreach { t =>
            val set = head(it indexOf t)
            val min_tmp = t.getMinInBitset(set)
            val max_tmp = t.getMaxInBitset(set)
            if (min_tmp > min) min = min_tmp
            if (max_tmp < max) max = max_tmp
        }
        if (min >= max) 0
        else {
          // Step 1.2: pass min, max as "start" and "end" into func bit_intersection.
          it foreach {t =>
            val i = it indexOf t
            start(i) = t.findElemInSetByValue(head(i), min)
            end(i) = t.findElemInSetByValue(head(i), max)
          }
          simd_bitmap_intersection(inter_data(level), it.length, arr, start, end, min)
        }
      }

      def intersect_on_level_leapfrog (level : Int) : Rep[Int] = {
        // intersect
        // and put result into inter_data ( level )
        val it = tries.filter( t => t.getSchema.contains(schema(level)))
        val arr = NewArray[Array[Int]]( it.length )
        it foreach { t =>
            arr(it indexOf t) = t.getTrie
        }
        val head = NewArray[Int]( it.length )
        it foreach { t =>
            // print("set: "); println(curr_set(level, tries indexOf t))
            head(it indexOf t) = curr_set(level, tries indexOf t)
        }
        leapfrog_on_level( level, arr, head )
      }

      def init = {
        curr_inter_data_index( 0 ) = 0
        tries.foreach { it => 
          val attr = it.getSchema ( 0 )
          val attr_index = schema indexOf attr
          val relation = tries indexOf it
          val head = it.findFirstSet
          curr_set update ( attr_index, relation, head )
        }
        // conduct intersection on level 0
        inter_data_len(0) = intersect_on_level(0)
      }

      // return len of intersection set
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
    }

    // search functions for UInteTrie
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
}
