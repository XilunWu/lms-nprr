package scala.lms.nprr

import scala.lms.common._
/*
trait Trie extends Set with Intersection with Dsl with StagedQueryProcessor with UncheckedOps {

  object trie_const {
    val initRawDataLen  = (1 << 16)

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

  case class Matrix (row: Rep[Int], col: Rep[Int]) {
    val rows = NewArray[Array[Int]](row)
    val lens = NewArray[Int](row)

    var i = 0
    while (i < row) {
      val new_row = NewArray[Int](col)
      rows(i) = new_row
      lens(i) = col
      i += 1
    }

    def apply(row_num: Rep[Int]) = rows(row_num)
    def apply(row_num: Rep[Int], col_num: Rep[Int]): Rep[Int] = {
      val row = rows(row_num)
      row(col_num)
    }
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

  case class ArrayBuffer (init_len: Rep[Int]) {
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

  class TabularTrie (data: Rep[Array[Int]], head: Rep[Int], schema: Schema){
    
  } 
  class ColumnarTrie (data: Rep[Array[Int]], head: Rep[Int], schema: Schema){
    
  }
  class TabularTrieLoader(schema: Schema) {
    import trie_const._
    // change it back to Vector[Rep[Array[Int]]]
    val indexArray = new Matrix (schema.length, initRawDataLen)
    val valueArray = new Matrix (schema.length, initRawDataLen)
    val lenArray = NewArray[Int](schema.length)
    var offset = 0

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
    def buildTrie(mem: Rep[Array[Int]], start: Rep[Int]) = {
      offset = 0
      //make sure that indexArray(i)(lenArray(i)) = lenArray(i+1)
      0 until (schema.length - 1) foreach { i =>
        indexArray update (i, lenArray(i), lenArray(i+1))
      }
      build_trie(0, mem, start, 0, lenArray(0)) 
      offset  // return the size of trie
    }

    def build_trie(level: Int, mem: Rep[Array[Int]], start: Rep[Int], begin: Rep[Int], end: Rep[Int]): Rep[Unit] = {      
      val set_head = start+offset
      val set_size = build_set(mem, start+offset, valueArray(level), begin, end)  
      offset += set_size

      if (level != schema.length - 1) {
        // Build subtrie for each element in result set if its not the last attribute.
        var i = start
        while ( i < end ) {
          val begin = indexArray(level, i)
          val end = indexArray(level, i+1)
          // TODO: update offset in build_set
          val set = new Set(mem, set_head)
          set set_index (i - start, valueArray(level, i), start+offset)  // index_in_set, value, index_value
          build_trie(mem, start+offset, valueArray(level+1), begin, end)
          i += 1
        }
      }
    }

    def build_set(mem: Rep[Array[Int]], start: Rep[Int], value: Rep[Array[Int]], begin: Rep[Int], end: Rep[Int]): Rep[Int] = {
      val min = value(begin)
      val range = value(end-1) - min
      val cardinality = end - begin
      val index_size = set_index_size(range, cardinality)
      // We build Int set only, for test
      build_Int_set(level, mem, start) + index_size
    }

    def build_Int_set(mem: Rep[Array[Int]], start: Rep[Int], value: Rep[Array[Int]], begin: Rep[Int], end: Rep[Int]): Rep[Int] = {
      // TODO: build set like we did 
      // Structure: Head - Int vector - Index vector
      // Head: card - range - size_in_bytes - type
      val min = value(begin)
      val max = value(end-1)
      val card = end-begin
      val range = max - min
      val set_type = set_const.type_uintvec
      mem(start+0) = card
      mem(start+1) = range
      mem(start+2) = set_const.bytes_per_int * card // in bytes
      mem(start+3) = set_type

      // Int vector
      var offset = 4
      array_copy(mem, start+offset, value, begin, card * bytes_per_int)

      // Index is allocated outside
      4+card
    }
  }
  // ColumnarTrieLoader: Int set is done. Bit set/hybrid need be done.
  class ColumnarTrieLoader(schema: Schema) {
    import trie_const._
    // change it back to Vector[Rep[Array[Int]]]
    val indexArray = new Matrix (schema.length, initRawDataLen)
    val valueArray = new Matrix (schema.length, initRawDataLen)
    val lenArray = NewArray[Int](schema.length)

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

    def buildTrie(mem: Rep[Array[Int]], start: Rep[Int]) = {
      //make sure that indexArray(i)(lenArray(i)) = lenArray(i+1)
      0 until (schema.length - 1) foreach { i =>
        indexArray update (i, lenArray(i), lenArray(i+1))
      }
      val first_set_on_level = NewArray[Int](schema.length)
      var offset = 0
      (0 until schema.length) foreach { lv => 
        val value = valueArray(lv)
        first_set_on_level(lv) = start+offset
        if (lv == 0) {
          val begin = 0
          val end = lenArray(0)
          val size_of_set = buildSet(mem, start+offset, value, begin, end)
          val size_of_index = allocate_index (value, begin, end)
          offset += (size_of_set+size_of_index)
        }
        else {
          val index = indexArray(lv-1)
          var parent_set = first_set_on_level(lv-1)
          var low_bound = 0
          var up_bound = new Set(mem, parent_set) get_cardinality
          var i = 0
          while (i < lenArray(lv-1)) {  // iterate through all elems on upper level, build their child sets
            val begin = index(i)
            val end = index(i+1)
            // make index in parent set
            // if the index shall be in sibling of parent set, update it to its sibling.
            if (i >= up_bound) {
              parent_set = (new Set(mem, parent_set) get_sibling_set)
              low_bound = up_bound
              up_bound += (new Set(mem, parent_set) get_cardinality)
            }
            new Set(mem, parent_set) set_index (i-low_bound, start+offset) // Check out how EH stores index for bitset. Do we need data to index?
            val size_of_set = buildSet(mem, start+offset, value, begin, end)
            val size_of_index = if (lv != schema.length-1) allocate_index (value, begin, end) else 0 // no index for last attribute
            offset += (size_of_set+size_of_index)
            i += 1
          }
        }
      }
      start+offset
    }
    def buildSet(mem: Rep[Array[Int]], start: Rep[Int], value: Rep[Array[Int]], begin: Rep[Int], end: Rep[Int]): Rep[Int] = {
      val min = value(begin)
      val range = value(end-1) - min
      val cardinality = end - begin
      // return the size of set
      /*if (range / cardinality > 32) return buildIntSet(mem, start, value, begin, end)
      else return buildBitSet(mem, start, value, begin, end)*/
      return buildIntSet(mem, start, value, begin, end)
    }
    def buildIntSet(mem: Rep[Array[Int]], start: Rep[Int], value: Rep[Array[Int]], begin: Rep[Int], end: Rep[Int]): Rep[Int] = {
      // Structure: Head - Int vector - Index vector
      // Head: card - range - size_in_bytes - type
      val min = value(begin)
      val max = value(end-1)
      val card = end-begin
      val range = max - min
      val set_type = set_const.type_uintvec
      mem(start+0) = card
      mem(start+1) = range
      mem(start+2) = set_const.bytes_per_int * card // in bytes
      mem(start+3) = set_type

      // Int vector
      var offset = 4
      array_copy(mem, start+offset, value, begin, card * bytes_per_int)

      // Index is allocated outside
      4+card
    }
    def buildBitSet(mem: Rep[Array[Int]], start: Rep[Int], value: Rep[Array[Int]], begin: Rep[Int], end: Rep[Int]): Rep[Int] = {
      // Structure: Head - Bitset - Index (How to store index? if the bitset is relatively sparse?)
      0
    }
    def allocate_index (value: Rep[Array[Int]], begin: Rep[Int], end: Rep[Int]): Rep[Int] = {
      // allocate index based on selectivity
      // for uint vector
      return end-begin
    }
    def array_copy(dest: Rep[Array[Int]], dest_start: Rep[Int], src: Rep[Array[Int]], src_start: Rep[Int], n: Rep[Int]) = {
      // memcpy((void *)(dest_start+dest_start), (void *)(src+src_start), n)
      unchecked[Unit]("memcpy((void *)(", dest, "+", dest_start, "),",
        "(void *)(", src, "+", src_start, "),",
        n, ");")
    }
  }

  class IntTrie (val schema: Schema) {
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

  val size_set_header = 1 // type of set 
  class TrieBuilder() {
    def build_set (data: NewArray[Int], head: Rep[Int], sets: List[Set]) {
      var offset = 0
      // allocate header. We write it back later.
      offset += size_set_header

      // estimate the size of result set and find the smallest set. 
      // get set header of the first set
      val min_set = sets(0)                   // init set be the first set
      var min_set = sets(0) get_cardinality   // the size of the smallest set
      var min_index = 0                       // the index of the smallest set
      var alloc_size = sets(0) get_num_of_bytes   // estimate of alloc_size
      var result_set_type = sets(0) get_type  // the type of the result set.
      

    }
  }

  class BitTrieLoader (val schema: Schema) {
    import trie_const._
    //index(i) is the start of child of value(i)
    //the intermediate datum used while generating uintTrie
    val indexArray = new Matrix (schema.length, initRawDataLen)
    val valueArray = new Matrix (schema.length, initRawDataLen)
    val lenArray = NewArray[Int](schema.length)

    val bitTrie = new ArrayBuffer(initRawDataLen * schema.length * 2)

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
    //get_info_bitset returns (min_in_bitmap, max_in_bitmap, start_of_index_section, size_of_bitset)
    def get_info_bitset(level: Rep[Int], start: Rep[Int], end: Rep[Int]) = {
      val v = valueArray(level)
      val min = v(start)
      val max = v(end-1)
      val card = end-start
      val min_in_bitmap: Rep[Int] = min & (~(bits_per_int-1))
      val max_in_bitmap: Rep[Int] = (max+bits_per_int) & (~(bits_per_int-1))
      val size_of_bitmap = (max_in_bitmap - min_in_bitmap) / bits_per_int
      val start_of_index_section = sizeof_bitset_header + size_of_bitmap
      val size_of_bitset = start_of_index_section + (max_in_bitmap - min_in_bitmap)
      (min_in_bitmap, max_in_bitmap, card, start_of_index_section, size_of_bitset)
    }

    def buildBitSet(level: Rep[Int], start: Rep[Int], end: Rep[Int], addr: Rep[Int], addr_index: Rep[Int]) = {
      val v = valueArray(level)
      var addr_new = addr
      var addr_index_new = addr_index

      val (min_in_bitmap, max_in_bitmap, cardinality, start_of_index_section, size_of_bitset) = get_info_bitset(level, start, end)
      bitTrie update (addr+loc_type, type_bitmap)
      bitTrie update (addr+loc_cardinality, cardinality)
      bitTrie update (addr+loc_range, (max_in_bitmap-min_in_bitmap) / bits_per_int)
      bitTrie update (addr+loc_min, min_in_bitmap)

      var bit_int = 0x0
      var min_in_bit_int = min_in_bitmap
      var i = start
      var pos = 0
      while (i < end) {
        val value = v(i)
        if (value - min_in_bit_int >= bits_per_int) {
          while (value - min_in_bit_int >= bits_per_int) { 
            bitTrie update (addr+sizeof_bitset_header+pos, bit_int)
            bit_int = 0x0
            pos += 1
            min_in_bit_int += bits_per_int 
          }
        }
        else {
          val diff = value - min_in_bit_int
          val last_bit = uncheckedPure[Int]("1l << ", (bits_per_int - diff - 1))
          bit_int = bit_int | last_bit
          // We assume all indices are initialized to 0. 
          if (level != schema.length - 1) {
            val index_in_bitmap = value - min_in_bitmap
            bitTrie update (addr+start_of_index_section+index_in_bitmap, addr_index_new)
            // print("value = "); print(value); print("; next = "); println(addr_index_new)
            // println(bitTrie(addr+start_of_index_section+index_in_bitmap))
            val (_, _, _, _, size_of_child_bitset) = get_info_bitset(level+1, indexArray(level, i), indexArray(level, i+1))

            addr_index_new += size_of_child_bitset
          }
          i += 1
        }
      }
      bitTrie update (addr+sizeof_bitset_header+pos, bit_int)
      addr_new = addr + size_of_bitset
      (addr_new, addr_index_new)
    }

    def buildBitTrie = {
      var level = 0
      var set_number = 0
      var addr_new_set = 0
      var addr_new_set_index = get_info_bitset(0, 0, lenArray(0))._5
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
          val (addr, index) = buildBitSet(level, start, end, addr_new_set, addr_new_set_index)
          // print("after buildIntSet, addr = "); print(addr); print(", index = "); println(index)
          addr_new_set = addr
          addr_new_set_index = index
          set_number += 1
        }
        level += 1
      }
      //printTrie
      new BitTrie(bitTrie, schema)
    }
  }

  case class BitTrie (bitTrie: ArrayBuffer, schema: Schema) {
    import trie_const._
    //trie in linear array

    def getTrie = bitTrie.arr
    def getData = bitTrie.arr
    def getSchema = schema

    def apply(i: Rep[Int]) = bitTrie.arr(i)
  /*
    // set-level method 
    def findChildSetByValue ( set : Rep[Int], value : Rep[Int] ) = {
      // This can be done by helper function. Replace it later
      val arr = bitTrie
      val min_in_bitmap = arr(set + loc_of_bitmap_min)
      val size_of_bitmap = arr(set + loc_of_cardinality)
      val index = value - min_in_bitmap
      val start_of_index_section = sizeof_bit_set_header + size_of_bitmap
      // print("min = "); print(min_in_bitmap); print("; size_of_bitmap = "); print(size_of_bitmap); print("; index = "); println(index)
      arr(set + start_of_index_section + index)
    }
    def findFirstSet : Rep[Int] = 0
    def getSizeOfBitmap(set: Rep[Int]): Rep[Int] = bitTrie(set+loc_of_cardinality)    
    def getMinInBitset(set: Rep[Int]): Rep[Int] = bitTrie(set+loc_of_bitmap_min)
    def getMaxInBitset(set: Rep[Int]): Rep[Int] = bitTrie(set+loc_of_bitmap_max)
    def findElemInSetByValue( set : Rep[Int], value : Rep[Int] ) = {
    	val arr = bitTrie
    	val rounded_value = value & (~(bits_per_int-1))
      val min_in_bitmap = arr(set + loc_of_bitmap_min)
      val index = (rounded_value - min_in_bitmap) / bits_per_int
      set + sizeof_bit_set_header + index
    }
  */
  }

  case class BitTrieIterator (trie: BitTrie) {
    val set_head = NewArray[Int](trie.getSchema.length)
    set_head(0) = 0

    def getSchema = trie.getSchema
    def getData = trie.getData
    def getSetHead(level: Int) = set_head(level)
    def getChild(level: Int, x: Rep[Int]) = { // x is the parent on level
      // set iterator on level to the child set of x
      val data = trie.getData
      val curr_set = set_head(level)
      // find x in curr_set
      val set = new Set(data, curr_set)
      set_head(level+1) = set.getChild(x)
      // print("Child of elem "); print(x); print(" is at addr: ")
      // println(set_head(level+1))
    }
    def getSet(head: Rep[Int]): Set = {
      return new Set(trie.getData, head)
    }
  }

  case class BitTrieBuilder (trie: BitTrie) {
    import trie_const._

    val addr_start_level = NewArray[Int](trie.getSchema.length)
    var next_set_to_build = 0

    def getResultTrie = trie
    def build_set(level: Int, it: List[BitTrieIterator]): Set = {
      val schema = trie.getSchema
      val lv_in_rels = it.map{ t => t.getSchema indexOf schema(level)}
      val arr = NewArray[Array[Int]]( it.length )
      it foreach { t =>
          arr(it indexOf t) = t.getData
      }
      val head = NewArray[Int]( it.length )
      (it, lv_in_rels).zipped.foreach { (t, lv) =>
          head(it indexOf t) = t.getSetHead(lv)
      }
      // Step1: align all bitmaps
      val start = NewArray[Int]( it.length )
      val end = NewArray[Int]( it.length )
      var min = it.head.getSet(head(0)).getMin // max of min's
      var max = it.head.getSet(head(0)).getMax // min of max's
      // Step1.1: find the overlap of bitmaps
      it foreach { t =>
          val set_i = head(it indexOf t)
          val min_tmp = t.getSet(set_i).getMin
          val max_tmp = t.getSet(set_i).getMax
          if (min_tmp > min) min = min_tmp
          if (max_tmp < max) max = max_tmp
      }
      val cardinality:Rep[Int] = 
        if (min >= max) 0
        else {
          // Step 1.2: pass min, max as "start" and "end" into func bit_intersection.
          it foreach {t =>
            val i = it indexOf t
            start(i) = t.getSet(head(i)).findByValue(min)
            end(i) = t.getSet(head(i)).findByValue(max)
          }
          simd_bitmap_intersection(trie.getData, next_set_to_build+sizeof_bitset_header, it.length, arr, start, end, min)
        }
      val set_head = next_set_to_build
      // make header: type | cardinality
      val data = trie.getData
      data(set_head+loc_type) = type_bitmap
      data(set_head+loc_cardinality) = cardinality
      val result_set = new Set(data, set_head)
      // update next_set_to_build
      // we don't build result trie because of the inefficient data structure. 
      // next_set_to_build = next_set_to_build + result_set.getSize
      return result_set
    }
  }
  
}
*/