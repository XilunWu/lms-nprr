package scala.lms.nprr

import scala.lms.common._

// How to design this method?
// When to decide type of set to build?
// When to decide type of set to cast?
trait Set {
	this: Dsl =>

	def buildSet(
		mem: Rep[Array[Int]],
		addr: Rep[Int], 
		arr: Rep[Array[Int]], 
		begin: Rep[Int], 
		end: Rep[Int]): BaseSet = {
		val set = new BaseSet(mem, addr)
		set
	}

	abstract class Set  {
		val mem: Rep[Array[Int]]
		val data: Rep[Int]

		def getType: Rep[Int]  // Enum type?
		def getCardinality: Rep[Int]  // cardinality of set
		def getSize: Rep[Int]  // in unit sizeof(Int)
		def getNextKey(key: Rep[Int]): Rep[Int]  // return addr of the appropriate key
		def getChild(key: Rep[Int]): Rep[Int]    // return head of the child set
		def getKeyGTE(key: Rep[Int]): Rep[Int]  // return addr of the appropriate key
		def foreach(f: Rep[Int] => Rep[Unit]): Rep[Unit]
	}

	class BaseSet (
		val mem: Rep[Array[Int]], 
		val data: Rep[Int]) extends Set {

		def getType = 0
		def getCardinality = 2
		def getSize = 8
		def getNextKey(key: Rep[Int]): Rep[Int] = 0
		def getChild(key: Rep[Int]): Rep[Int] = 0
		def getKeyGTE(key: Rep[Int]): Rep[Int] = 0
		def foreach(f: Rep[Int] => Rep[Unit]): Rep[Unit] = {
			var i = 0
			while (i < getCardinality) {
				f(i)
				i += 1
			}
		}
	}

	// class BitSet extends Set {}

	// class UIntSet extends Set {}
}


