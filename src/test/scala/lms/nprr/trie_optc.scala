package scala.lms.nprr

import scala.lms.common._

trait Trie extends Dsl with StagedQueryProcessor {

	
  object trie_const{
    val initRawDataLen  = (1 << 16)
    val loc_of_type = 0
    val loc_of_cardinality = 1
    val sizeof_uint_set_header = 2
    val type_uintvec = 1
  }

  class Matrix (row: Rep[Int], col: Rep[Int]) {
    val rows = NewArray[Array[Int]](row)
    val lens = NewArray[Int](row)

    var i = 0
    while (i < row) {
      val new_row = NewArray[Int](col)
      rows(i) = new_row
      i += 1
    }

    def apply(row_num: Rep[Int]) = rows(row_num)
    def apply(row_num: Rep[Int], col_num: Rep[Int]): Rep[Int] = {
      val row = rows(row_num)
      row(col_num)
    }/*
    def update(row_num: Rep[Int], new_row: Rep[Array[Int]]) = {
    	rows update (row_num, new_row)
    }*/
    def update(row_num: Rep[Int], col_num: Rep[Int], x: Rep[Int]) = {
    	if (col_num >= lens(row_num)) {
    		var new_len = 2 * lens(row_num)
    		while(col_num >= new_len) new_len *= 2
    		val new_arr = NewArray[Int](new_len)
    		row_dup(row_num, new_arr)
    		lens(row_num) = new_len
    		rows update (row_num, new_arr)
    	}
      val row = rows(row_num)
      row update (col_num, x)
    }
    def row_dup(row: Rep[Int], new_arr: Rep[Array[Int]]) = {
    	val src = rows(row)
    	val count = lens(row)
    	var i = 0
    	while (i < count) {
    		new_arr(i) = src(i)
    		i += 1
    	}
    }
  }

  class ArrayBuffer (init_len: Rep[Int]) {
  	var arr = NewArray[Int](init_len)
  	var len = init_len
  	def update(index: Rep[Int], value: Rep[Int]) = {
  		if (index >= len) {
  			var new_len = 2 * len
  			while (index >= new_len) new_len *= 2
  			val new_arr = NewArray[Int](new_len)
  			dup2(new_arr)
  			len = new_len
  			arr = new_arr
  		}
  		arr(index) = value
  	}

  	def apply(index: Rep[Int]) = arr(index)

  	def dup2(new_arr: Rep[Array[Int]]) = {
    	var i = 0
    	val count = len
    	while (i < count) {
    		new_arr(i) = arr(i)
    		i += 1
    	}
    }
  }

  class Trie (schema: Schema) {
    import trie_const._
		
  }

  class IntTrie (schema: Schema) {
    import trie_const._
    //index(i) is the start of child of value(i)
    //the intermediate datum used while generating uintTrie
    val indexArray = new Matrix (schema.length, initRawDataLen)
    val valueArray = new Matrix (schema.length, initRawDataLen)
    val lenArray = NewArray[Int](schema.length)

    //trie in linear array
    val uintTrie = new ArrayBuffer(initRawDataLen * schema.length * 2)

    def getTrie = uintTrie.arr
    def getSchema = schema

    def +=(fields: Vector[Rep[Int]]):Rep[Unit] = {
      var diff = false
      fields foreach { x =>
        val i = fields indexOf x
        if (lenArray(i) == 0) diff = true
        else 
          if (!diff) diff = !(valueArray(i, lenArray(i)-1) == x)
        if (diff) {
          valueArray update (i, lenArray(i), x)
          if (i != schema.length - 1) {
            indexArray update (i, lenArray(i), lenArray(i+1))
          }
          lenArray(i) = lenArray(i) + 1
        }
      }
    }

    def buildIntSet(level: Rep[Int], start: Rep[Int], end: Rep[Int], addr: Rep[Int], addr_index: Rep[Int]) = {
      val v = valueArray(level)
      val num = end - start
      var addr_new = addr
      var addr_index_new = addr_index

      uintTrie update (addr, type_uintvec)
      uintTrie update (addr+1, num)      

      var i = start
      while (i < end) {
        //value
        uintTrie update(addr + sizeof_uint_set_header + i - start, v(i))
        //index. Except for the last column
        if (level != schema.length - 1) {
        	uintTrie update(addr + sizeof_uint_set_header + num + i - start, addr_index_new)
          val num_of_children = indexArray(level, i+1) - indexArray(level, i)
          //update the location of its child set 
          addr_index_new = addr_index_new + sizeof_uint_set_header + 2 * num_of_children
        }
        i += 1
      }
      addr_new = addr + sizeof_uint_set_header + 2 * num
      (addr_new, addr_index_new)
    }

    def buildIntTrie = {
      var level = 0
      var set_number = 0
      var addr_new_set = 0
      var addr_new_set_index = sizeof_uint_set_header + 2 * lenArray(0)
      //make sure that indexArray(i)(lenArray(i)) = lenArray(i+1)
      0 until (schema.length - 1) foreach { i =>
        indexArray update (i, lenArray(i), lenArray(i+1))
      }
      while (level < schema.length) {
        val num_of_sets = if (level == 0) 1 else lenArray(level - 1)
        set_number = 0
        while (set_number < num_of_sets) {
          val start = if (level == 0) 0 else indexArray(level - 1, set_number)
          val end = if (level == 0) lenArray(0) else indexArray(level - 1, set_number + 1)
          val (addr, index) = buildIntSet(level, start, end, addr_new_set, addr_new_set_index)
          // print("after buildIntSet, addr = "); print(addr); print(", index = "); println(index)
          addr_new_set = addr
          addr_new_set_index = index
          set_number += 1
        }
        level += 1
      }
      //printTrie
    }
    def findChild(curr_set: Rep[Int], index: Rep[Int]): Rep[Int] = {
      //return -1 if no child found
      val set_size = uintTrie(curr_set + trie_const.loc_of_cardinality)
      val child_index_loc = curr_set + sizeof_uint_set_header + set_size + index
      val child = uintTrie(child_index_loc)
      //uninitialized index for last column
      if (child == 0) -1 else child
    }
    // set-level method 
    def findChildSetByValue ( set : Rep[Int], value : Rep[Int] ) = {
      // This can be done by helper function. Replace it later
      val arr = uintTrie
      val card = arr(set + trie_const.loc_of_cardinality)
      var start = set + trie_const.sizeof_uint_set_header
      var end = set + trie_const.sizeof_uint_set_header + card
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
        i += 1
      }
      val index = start + i - (set + trie_const.sizeof_uint_set_header)
      findChild(set, index)
    }
    def findFirstSet : Rep[Int] = 0

    def printTrie: Rep[Unit] = {
      val curr_index = NewArray[Int](schema.length)
      val curr_set = NewArray[Int](schema.length)
      val tp = uintTrie(0)
      var level = 0 

      if (tp == type_uintvec) {
        level = 0
        curr_set(0) = 0
        curr_index(0) = 0
        while (level >= 0) {
          //print indent
          var i = 0
          while (i < 8 * level) { print(" "); i += 1 }
          print(get_uint_trie_elem(curr_set(level), curr_index(level)))
          //if it has child (findChild returns positive), go 1 level down
          val child_set = findChild(curr_set(level), curr_index(level))
          if (child_set >= 0) {
            println(" --> ")
            level += 1
            curr_set(level) = child_set
            curr_index(level) = 0
          }
          //if it has no child (findChild returns -1) 
          else {
            println("")
            curr_index(level) = curr_index(level) + 1
            // or go 1 level up when reaching end. 
            // Note that we may need go multiple levels up 
            while (level >= 0 && atEnd(curr_set(level), curr_index(level))) {
              level -= 1
              if (level >= 0) curr_index(level) = curr_index(level) + 1
            }
          }
        }
      }
    }
    def get_uint_trie_elem(set: Rep[Int], index: Rep[Int]) = {
      uintTrie(set + trie_const.sizeof_uint_set_header + index)
    }
    def atEnd(set: Rep[Int], index: Rep[Int]) = {
      val card = uintTrie(set + trie_const.loc_of_cardinality)
      index >= card
    }
  }
}