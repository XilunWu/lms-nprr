package scala.lms.nprr

import scala.lms.common._

trait MemPool {
	this: Dsl =>
	abstract class MemPool {
		val mem: Rep[Array[Int]]  // a better definition is Rep[Array[Byte]]
		var top = 0
		/*
		def alloc (size: Int)
		def alloc (size: Rep[Int])
		def rollback (size: Int)
		def rollback (size: Rep[Int])
		*/
	}

	class SimpleMemPool (val size: Int) extends MemPool {
		val mem = NewArray[Int](size)
	}

	abstract class ParMemPool extends MemPool {
		val num_threads: Int
	}
}