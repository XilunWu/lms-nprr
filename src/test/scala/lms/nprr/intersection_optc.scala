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
