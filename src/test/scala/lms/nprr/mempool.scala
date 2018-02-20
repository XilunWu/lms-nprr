package scala.lms.nprr

import scala.lms.common._

trait MemPool extends UncheckedOps {
	this: Dsl =>
	abstract class MemPool {
		val mem: Rep[Array[Int]]  // a better definition is Rep[Array[Byte]]
		/*
		def alloc (size: Int)
		def alloc (size: Rep[Int])
		def rollback (size: Int)
		def rollback (size: Rep[Int])
		*/
		def free = {}
	}

	case class SimpleMemPool (val size: Int) extends MemPool {
		// remember to initialize it to 0
		val mem = NewArray[Int](size)
		// uncheckedPure[Unit]("memset(" + mem + ", 0, " + size + " * sizeof(uint64_t));")
	}

	abstract class ParMemPool extends MemPool {
		val num_threads: Int
	}
}