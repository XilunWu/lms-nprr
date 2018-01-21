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

		def getTrieBlock(offset: Rep[Int]): TrieBlock = {
			TrieBlock (mem.mem, data+offset)  // the func signature may need change
		}
	}

	// Sets are stored in Prefix order as EmptyHeaded did. 
	class SimpleTrie (
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
					var index = 0
					while (index < end - begin) {
						val index_arr: Rep[Array[Int]] = tmp_index(lv)
						val c_begin: Rep[Int] = index_arr(begin+index)  // specify the type of "recursive value"
						val c_end = index_arr(begin+index+1)
						// print("build set, offset = "); println(offset)
						tb refineIndex (index, arr(begin+index), data+offset)
						buildSubTrie(lv+1, c_begin, c_end)
						index += 1
					}
				}
			}

			buildSubTrie (0, 0, tmp_len(0))
			offset  // mem usage
		}

		def printTrie = {
			def printSubTrie (head: Rep[Int], lv: Int): Rep[Unit] = {
				val tb = TrieBlock (mem.mem, head)
				print(" " * lv * 16)
				printSetType (tb getType)
				if (lv == schema.length-1) println("")
				else {
					// pattern match generates no code:
					/*
					val concrete_set = (set getType) match {
						case set_const.type_uint_set =>
							new UIntSet (mem.mem, head)
						case set_const.type_bit_set =>
							new BitSet (mem.mem, head)
						case _ => set
					}
					*/
					val typ = tb.getType
					if (typ == set_const.type_uint_set) {
						val concrete_set = tb.getUintSet
						concrete_set foreach { x =>
							val c_set = concrete_set getChild(x)
							printSubTrie (c_set, lv+1)
						}
					}
					else if (typ == set_const.type_bit_set) {
						val concrete_set = tb.getBitSet
						concrete_set foreach { x =>
							val c_set = concrete_set getChild(x)
							printSubTrie (c_set, lv+1)
						}
					}
				}
			}
			// Why this pattern match doesn't generate code?
			/*
			def printSetType (typ: Rep[Int]) = typ match {
				case set_const.type_uint_set => 
					print("Uint set ")
				case set_const.type_bit_set =>
					print("Bit set ")
				case _ =>
			}
			*/
			def printSetType (typ: Rep[Int]) = {
				if (typ == set_const.type_uint_set) print("Uint set ")
				else if (typ == set_const.type_bit_set) print("Bit set ")
			}
			printSubTrie(data, 0)
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

/*
		def getNext = {
			val set = trie getSet cursor(curr_lv)
			val key = keys(curr_lv)
			val next = set getNextKey key
			keys(curr_lv) = next
		}
		def up = {
			curr_lv -= 1
		}
		def open = {
			val parent = trie getSet cursor(curr_lv)
			val curr_prefix = keys(curr_lv)
			val child = parent getChild curr_prefix
			curr_lv += 1
			cursor(curr_lv) = child
		}
*/
		/*
		def seek(key: Rep[Int]) = {
			val set = trie getSet cursor(curr_lv)
			val res = set getKeyGTE key
			res
		}
		*/
	}

	// class ParTrieIterator extends TrieIterator {}
	

	abstract class TrieBuilder {
		val tries: List[Trie]
		val schemas: List[Vector[String]]
		val resultSchema: Vector[String]

		def build (mem: Rep[Array[Int]], start: Rep[Int])
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
				val block_on_lv = it_involved map (_ getCurrSetOnAttr resultSchema(lv))
				val tb = TrieBlock (mem, start+offset)
				tb.build (block_on_lv)
				offset += (set getSize)
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
							it_involved setChildSet (lv, x)  // open(lv): to the child of x
							tb refineIndexByIndex (index, start+offset)
							buildSubTrie(lv+1)
						}
					}
					else if (typ == set_const.type_bit_set) {
						val concrete_set = tb.getBitSet
						concrete_set foreach { x =>
							it_involved setChildSet (lv, x) 
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
