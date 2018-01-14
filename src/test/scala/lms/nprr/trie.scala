package scala.lms.nprr

import scala.lms.common._

trait Trie extends MemPool with Set {
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

		def getSet(offset: Rep[Int]): Set = {
			new BaseSet (mem.mem, data+offset)  // the func signature may need change
		}
	}

	// Sets are stored in Prefix order as EmptyHeaded did. 
	class SimpleTrie (
		val mem: MemPool, 
		val data: Rep[Int], 
		val schema: Vector[String]) extends Trie {

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
				val sb = new SetBuilder (mem.mem, data+offset)
				val set = sb.build (arr, begin, end)
				offset += (set getSize)
				if (lv != schema.length-1) {
					var index = 0
					while (index < (set getCardinality)) {
						val index_arr: Rep[Array[Int]] = tmp_index(lv)
						val c_begin: Rep[Int] = index_arr(begin+index)  // specify the type of "recursive value"
						val c_end = index_arr(begin+index+1)
						// print("build set, offset = "); println(offset)
						sb refineIndex (index, arr(begin+index), offset)
						buildSubTrie(lv+1, c_begin, c_end)
						index += 1
					}
				}
			}

			buildSubTrie (0, 0, tmp_len(0))
			offset
		}

		def printTrie = {
			def printSubTrie (head: Rep[Int], lv: Int): Rep[Unit] = {
				val set = new BaseSet (mem.mem, head)
				print(" " * lv * 16)
				printSetType (set getType)
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
					val typ = set.getType
					if (typ == set_const.type_uint_set) {
						val concrete_set = new UIntSet (mem.mem, head)
						concrete_set foreach { x =>
							val c_set = concrete_set getChild(x)
							printSubTrie (data+c_set, lv+1)
						}
					}
					else if (typ == set_const.type_bit_set) {
						val concrete_set = new BitSet (mem.mem, head)
						concrete_set foreach { x =>
							val c_set = concrete_set getChild(x)
							printSubTrie (data+c_set, lv+1)
						}
					}
					else {}
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

	class TrieIterator (val trie: Trie) {
		// val trie: Trie
		val cursor: Rep[Array[Int]] = NewArray[Int](trie.schema.length)
		val keys: Rep[Array[Int]] = NewArray[Int](trie.schema.length)
		var curr_lv = 0

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
		def init // need?
		def seek(key: Rep[Int]) = {
			val set = trie getSet cursor(curr_lv)
			val res = set getKeyGTE key
			res
		}
	}

	trait ParTrieIterator extends TrieIterator {}
	*/
}
