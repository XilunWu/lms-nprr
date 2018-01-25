package scala.lms.nprr

import scala.lms.common._

trait Trie extends MemPool with TrieBlock {
	this: Dsl =>

	abstract class Trie {
		val mem: MemPool
		val data: Rep[Int]  // the beginning address of data in memory pool
		val schema: Vector[String]
		// tmp_data: of size max_level * MAXLEN
		// don't forget to free these arrays
		val maxlen = (1 << 18)
		val tmp_data = schema.map{_ => NewArray[Int](maxlen)}
		val tmp_index = schema.map{_ => NewArray[Int](maxlen)}
		val tmp_len = NewArray[Int](schema.length)
		// build trie and rollback memory,
		// return the final size of our trie in mem.
		// free tmp_data after building trie
		def buildTrie: Rep[Int] 
		def += (fields: Vector[Rep[Int]]): Rep[Unit] = {
			var diff = false
			fields foreach { x =>
			  val i = fields indexOf x
			  if (tmp_len(i) == 0) diff = true
			  else if (!diff) diff = !(tmp_data(i)(tmp_len(i)-1) == x)

			  if (diff) {
			    tmp_data(i)(tmp_len(i)) = x
			    if (i != schema.length - 1) {
			    	tmp_index(i)(tmp_len(i)) = tmp_len(i+1)
			    }
			    tmp_len(i) = tmp_len(i) + 1
			  }
			}
		}
		def dumpRawData = {
			// finish tmp_index
			tmp_index foreach { i_arr =>
				val i = tmp_index indexOf i_arr
				if (i != tmp_index.length - 1)
					i_arr(tmp_len(i)) = tmp_len(i + 1)
			}
			val cursor = NewArray[Int](schema.length)
			def dumpRawTrie (lv: Int): Rep[Unit] = {
				if ( lv == 0 ) {
					val begin = 0
					val end = tmp_len(0)
					var i = begin
					while ( i < end ) {
						cursor( lv ) = i
						dumpRawTrie(lv + 1)
						i += 1
					}
				}
				else if ( lv != schema.length - 1 ) {
					val parent = cursor( lv - 1 )
					val begin = tmp_index( lv - 1 )( parent )
					val end = tmp_index( lv - 1 )( parent + 1 )
					var i = begin
					while ( i < end ) {
						cursor( lv ) = i
						dumpRawTrie(lv + 1)
						i += 1
					}
				} else {
					val parent = cursor( lv - 1 )
					val begin = tmp_index( lv - 1 )( parent )
					val end = tmp_index( lv - 1 )( parent + 1 )
					var i = begin
					while ( i < end ) {
						tmp_data.dropRight(1).foreach { data =>
							print( data( cursor( tmp_data indexOf data )))
							print("\t")
						}
						println( tmp_data.last( i ))
						i += 1
					}
				}
			}
			dumpRawTrie( 0 )
		}

		def getTrieBlock(offset: Rep[Int]): TrieBlock = {
			TrieBlock (mem.mem, data+offset)  // the func signature may need change
		}
	}

	// Sets are stored in Prefix order as EmptyHeaded did. 
	case class SimpleTrie (
		val mem: MemPool, 
		val data: Rep[Int], 
		val schema: Vector[String]
	) extends Trie {

		def buildTrie: Rep[Int] = {  // return the mem usage
			var offset = 0
			// lv: the level in which the set we're gonna build resides
			// start & end: the begin/end of the set in array.
			def buildSubTrie (lv:Int, begin: Rep[Int], end: Rep[Int]): Rep[Unit] = {
				// build the set block for the attribute at level "lv".
				// for each key in set, build their sub tries.
				val arr = tmp_data(lv)
				// make the judgement in Set.buildSet() method:
				// what kind of set should we build.
				val tb = TrieBlock (mem.mem, data+offset)
				tb.buildFromRawData (arr, begin, end)
				// val t_block = tb.buildTrieBlock (arr, begin, end)
				offset += (tb getSize)
				if (lv != schema.length-1) {
					var index = begin
					while (index < end) {
						val index_arr: Rep[Array[Int]] = tmp_index(lv)
						val c_begin: Rep[Int] = index_arr(index)  // specify the type of "recursive value"
						val c_end = index_arr(index+1)
						// print("build set, offset = "); println(offset)
						tb refineIndex (index - begin, arr(index), data+offset)
						buildSubTrie(lv+1, c_begin, c_end)
						index += 1
					}
				}
			}

			// finish tmp_index
			tmp_index foreach { i_arr =>
				val i = tmp_index indexOf i_arr
				if (i != tmp_index.length - 1)
					i_arr(tmp_len(i)) = tmp_len(i + 1)
			}
			buildSubTrie (0, 0, tmp_len(0))
			// free tmp_mem
			// ....

			offset  // mem usage
		}

		private def dumpUintSet (uintset: UintSet) = {
			print("mem: "); println(uintset.mem)
			print("data: "); println(uintset.data)
			println("")
		}

		private def dumpBitSet (bitset: BitSet) = {
			print("mem: "); println(bitset.mem)
			print("data: "); println(bitset.data)
			println("")
		}

		def dumpTrie = {
			val dump_set_info = false
			val dump_set_elem = false 
			val dump_set_type = true

			val str = NewArray[Int](schema.length)
			def printSubTrie (tb: TrieBlock, lv: Int): Rep[Unit] = {
				if (lv == schema.length-1) {
					val typ = tb.getType
					if (typ == set_const.type_uint_set) { 
						val uintset = tb.getUintSet
						/*
						 * This dump the address of each set
						*/
						if (dump_set_info) dumpUintSet(uintset)
						if (dump_set_elem) {
							uintset foreach { fun { 
								v: Rep[Int] =>
								var i = 0
								while ( i < lv ) {
									print(str(i)); print("\t")
									i += 1
								} 
								println(v)
							}}
						}
						if (dump_set_type) {
							print(" " * lv * 16)
							printSetType (tb getType)
							println("")
						}
						unit()
					} else if (typ == set_const.type_bit_set) {
						val bitset = tb.getBitSet
						/*
						 * This dump the address of each set
						*/
						if (dump_set_info) dumpBitSet(bitset)
						if (dump_set_elem) {
							bitset foreach { v => 
								var i = 0
								while ( i < lv ) {
									print(str(i)); print("\t")
									i += 1
								} 
								println(v)
							}
						}
						if (dump_set_type) {
							print(" " * lv * 16)
							printSetType (tb getType)
							println("")
						}
						unit()
					}
				} else {
					val typ = tb.getType
					if (typ == set_const.type_uint_set) {
						val uintset = tb.getUintSet
						/*
						 * This dump the address of each set
						*/
						if (dump_set_info) dumpUintSet(uintset)
						if (dump_set_type) {
							print(" " * lv * 16)
							printSetType (tb getType)
						}

						uintset foreach_index { fun {
							index: Rep[Int] =>
							val v = uintset getKeyByIndex index
							val c_block = TrieBlock (
								tb.mem, 
								tb getChildBlockByIndex index
							)
							str(lv) = uintset getKeyByIndex index
							printSubTrie (c_block, lv + 1)
						}}
					} else {
						val bitset = tb.getBitSet
						/*
						 * This dump the address of each set
						*/
						if (dump_set_info) dumpBitSet(bitset)
						if (dump_set_type) {
							print(" " * lv * 16)
							printSetType (tb getType)
						}

						bitset foreach { v =>
							val index = bitset getIndexByKey v
							val c_block = TrieBlock (
								tb.mem, 
								tb getChildBlockByIndex index
							)
							str(lv) = v
							printSubTrie (c_block, lv + 1)
						}
					}
				}
			}

			def printSetType (typ: Rep[Int]) = {
				if (typ == set_const.type_uint_set) print("Uint set ")
				else if (typ == set_const.type_bit_set) print("Bit set ")
			}

			val tb = TrieBlock(mem.mem, data)
			printSubTrie (tb, 0)
		}
	}
	/*
	class PrefixTrie extends Trie {}

	class LevelOrderTrie extends Trie {}

	trait ParTrie extends Trie {}
	*/
	class TrieIterator (val trie: Trie) {
		// val trie: Trie
		val cursor: Rep[Array[Int]] = NewArray[Int](trie.schema.length)
		val keys: Rep[Array[Int]] = NewArray[Int](trie.schema.length)
		
		def init = { // need?

		}
		def setChildBlock (lv: Int, value: Rep[Int]) = {  // open(lv, value)
			val tb = TrieBlock (trie.mem.mem, cursor(lv))
			cursor(lv+1) = tb getChildBlock value
		}
		def getCurrBlockOnAttr (attr: String) = {
			val schema = trie.schema
			TrieBlock (trie.mem.mem, cursor(schema indexOf attr))
		}
	}

	// class ParTrieIterator extends TrieIterator {}
	

	abstract class TrieBuilder {
		val tries: List[Trie]
		val schemas: List[Vector[String]]
		val resultSchema: Vector[String]

		def build (mem: Rep[Array[Int]], start: Rep[Int]): Rep[Int]
	}
	class SimpleTrieBuilder (
		val tries: List[Trie],
		val schemas: List[Vector[String]],
		val resultSchema: Vector[String]
	) extends TrieBuilder {

		val iterators = tries.map(new TrieIterator(_))

		override def build (mem: Rep[Array[Int]], start: Rep[Int]): Rep[Int] = {
			var offset = 0

			def buildSubTrie (lv:Int): Rep[Unit] = {
				val it_involved = iterators.filter (_.trie.schema contains resultSchema(lv))
				val block_on_lv = it_involved map (_ getCurrBlockOnAttr resultSchema(lv))
				val tb = TrieBlock (mem, start+offset)
				tb.build (block_on_lv)
				offset += (tb getSize)
				if (lv != resultSchema.length-1) {
					// foreach:
					// 1. set iterators to child
					// 2. build sub tries, 
					// 3. and refine indices in their parent set
					val it_involved_on_next_lv = 
						iterators.filter (_.trie.schema contains resultSchema(lv+1))

					// How to remove the code smell???
					val typ = tb.getType
					if (typ == set_const.type_uint_set) {
						val concrete_set = tb.getUintSet
						concrete_set foreach_index { index =>
							val x = concrete_set getKeyByIndex index
							it_involved foreach { it => it.setChildBlock (lv, x) } // open(lv): to the child of x
							tb refineIndexByIndex (index, start+offset)
							buildSubTrie(lv+1)
						}
					}
					else if (typ == set_const.type_bit_set) {
						val concrete_set = tb.getBitSet
						concrete_set foreach { x =>
							it_involved foreach { it => it.setChildBlock (lv, x) }
							tb refineIndexByValue (x, start+offset)
							buildSubTrie(lv+1)
						}
					}
				}
			}

			// init: put iterators on attribute schema(0) at position
			iterators foreach (_.init)
			buildSubTrie(0)
			offset
		}
	}
}
