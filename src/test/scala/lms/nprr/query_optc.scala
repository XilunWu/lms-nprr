package scala.lms.nprr

import scala.lms.common._

object query_optc {
trait QueryCompiler extends Dsl with StagedQueryProcessor
with ScannerLowerBase with Trie {
  override def version = "query_optc"
/**
Input File Tokenizer
--------------------
*/
  class Scanner(name: Rep[String]) {
    val fd = open(name)
    val fl = filelen(fd)
    val data = mmap[Char](fd,fl)
    var pos = 0

    def next(d: Rep[Char]) = {
      val start = pos: Rep[Int] // force read
      while (data(pos) != d) pos += 1
      val len = pos - start
      //Special case for tpch dbgen data. Last column contains '|' and '\n'
      pos += 1
      RString(stringFromCharArray(data,start,len), len)
    }

    def nextInt(d: Rep[Char]) = {
      val start = pos: Rep[Int] // force read
      var num = 0
      while (data(pos) != d) {
        //If date format, transfer it to Int
        if (data(pos) != '-')
          num = num * 10 + (data(pos) - '0').toInt
        pos += 1
      }
      pos += 1
      RInt(num)
    }

    def hasNext = pos < fl
    def done = close(fd)
  }

/**
Low-Level Processing Logic
--------------------------
*/
  abstract class RField {
    def print()
    def compare(o: RField): Rep[Boolean]
    def hash: Rep[Long]
    def lessThan(o :RField): Rep[Boolean]
    //def update(o: RField)
    def -(o:RField): Rep[Int]
    def contains(o:RField): Rep[Boolean]
  }
  //make data visilble outside RString.
  case class RString(val data: Rep[String], len: Rep[Int]) extends RField {
    def print() = prints(data)
    def compare(o: RField) = o match { case RString(data2, len2) => if (len != len2) false else {
      // TODO: we may or may not want to inline this (code bloat and icache considerations).
      var i = 0
      while (i < len && data.charAt(i) == data2.charAt(i)) {
        i += 1
      }
      i == len
    }}
    def lessThan(o: RField) = o match { case RString(data2, len2) => 
      var i = 0
      while (i < len && i < len2 && data.charAt(i) == data2.charAt(i)) i += 1
      if (i == len2) false
      else if(i == len) true
      else data.charAt(i) < data2.charAt(i)
    }
    def hash = data.HashCode(len)
    //def update(o: RField) = o match { case RString(data2, len2) => data = data2; len = len2}
    def -(o:RField) = o match { case RString(data2, len2) =>
      var res = 0
      var i = 0
      while (i < len && i < len2) {
        res *= 26
        res = res + (data.charAt(i).toInt - data2.charAt(i).toInt)
        i += 1
      }
      res
    }
    def contains(o:RField) = o match { case RString(data2, len2) =>
      //
      var nmatch = 0
      var i = 0
      //need fix

      while (nmatch != len2 && i < len-len2) {
        while (data.charAt(i+nmatch) == data2.charAt(nmatch)) nmatch += 1
        nmatch = if (nmatch == len2) nmatch else 0
        i += 1
      }
      nmatch == len2
    }
  }
  //make value visilble outside RInt.
  case class RInt(val value: Rep[Int]) extends RField {
    def print() = printf("%lld",value)
    def compare(o: RField) = o match { case RInt(v2) => value == v2 }
    def lessThan(o: RField) = o match { case RInt(v2) =>  value < v2 }
    def hash = value.asInstanceOf[Rep[Long]]
    //def update(o: RField) = o match { case RInt(v2) => value = v2}
    def -(o:RField) = o match { case RInt(v2) => value - v2}
    def contains(o:RField) = o match { case RInt(v2) => //no contains support for Int
      v2 <= value
    }
  }

  type Fields = Vector[RField]

  def isNumericCol(s: String) = s.startsWith("#")

  case class Record(fields: Fields, schema: Schema) {
    def apply(key: String): RField = fields(schema indexOf key)
    def apply(keys: Schema): Fields = keys.map(this apply _)
  }

  def processCSV(filename: Rep[String], schema: Schema, fieldDelimiter: Char, externalSchema: Boolean)(yld: Record => Rep[Unit]): Rep[Unit] = {
    val s = new Scanner(filename)
    val last = schema.last
    def nextField(name: String) = {
      val d = if (name==last) '\n' else fieldDelimiter
      if (isNumericCol(name)) s.nextInt(d) else s.next(d)
    }
    def nextRecord = Record(schema.map(nextField), schema)
    if (!externalSchema) {
      // the right thing would be to dynamically re-check the schema,
      // but it clutters the generated code
      // schema.foreach(f => if (s.next != f) println("ERROR: schema mismatch"))
      nextRecord // ignore csv header
    }
    while (s.hasNext) yld(nextRecord)
    s.done
  }

  def printSchema(schema: Schema) = println(schema.mkString(defaultFieldDelimiter.toString))

  def printFields(fields: Fields) = {
    if (fields.nonEmpty) {
      fields.head.print
      fields.tail.foreach { x  => printf(defaultFieldDelimiter.toString); x.print }
    }
    println("")
  }

  def fieldsEqual(a: Fields, b: Fields) = (a zip b).foldLeft(unit(true)) { (a,b) => b._1 compare b._2 }

  def fieldsHash(a: Fields) = a.foldLeft(unit(0L)) { _ * 41L + _.hash }

/**
Query Interpretation = Compilation
----------------------------------
*/
  def evalPred(p: Predicate)(rec: Record): Rep[Boolean] = p match {
    case Eq(a1, a2) => evalRef(a1)(rec) compare evalRef(a2)(rec)
    case LT(a1, a2) => evalRef(a1)(rec) lessThan evalRef(a2)(rec)
    case GTE(a1, a2) => !(evalRef(a1)(rec) lessThan evalRef(a2)(rec))
    case CON(a1, a2) => evalRef(a1)(rec) contains evalRef(a2)(rec)
  }

  def evalRef(r: Ref)(rec: Record): RField = r match {
    case Field(name) => rec(name)
    case Value(x:Int) => RInt(x)
    case Value(x) => RString(x.toString,x.toString.length)
  }

  def resultSchema(o: Operator): Schema = o match {
    case Scan(_, schema, _, _)   => schema
    case Filter(pred, parent)    => resultSchema(parent)
    case Project(schema, _, _)   => schema
    case Join(left, right)       => resultSchema(left) ++ resultSchema(right)
    case Group(keys, agg, parent)=> keys ++ agg
    case HashJoin(left, right)   => resultSchema(left) ++ resultSchema(right)
    case PrintCSV(parent)        => Schema()
    case NprrJoin(parents, outSchema, num_threads) => outSchema
    case Count(parent)            => Schema("#COUNT")
  }

  def execOp(o: Operator)(yld: Record => Rep[Unit]): Rep[Unit] = o match {
    case Scan(filename, schema, fieldDelimiter, externalSchema) =>
      processCSV(filename, schema, fieldDelimiter, externalSchema)(yld)
    case Filter(pred, parent) =>
      execOp(parent) { rec => if (evalPred(pred)(rec)) yld(rec) }
    case Project(newSchema, parentSchema, parent) =>
      execOp(parent) { rec => yld(Record(rec(parentSchema), newSchema)) }
    case Join(left, right) =>
      execOp(left) { rec1 =>
        execOp(right) { rec2 =>
          val keys = rec1.schema intersect rec2.schema
          if (fieldsEqual(rec1(keys), rec2(keys)))
            yld(Record(rec1.fields ++ rec2.fields, rec1.schema ++ rec2.schema))
        }
      }
    case Group(keys, agg, parent) =>
      val hm = new HashMapAgg(keys, agg)
      execOp(parent) { rec =>
        if (agg(0) == "#COUNT") hm(rec(keys)) += Vector[RField](RInt(1))
        else hm(rec(keys)) += rec(agg)
      }
      hm foreach { (k,a) =>
        yld(Record(k ++ a, keys ++ agg))
      }
    case Count(parent) =>
      var num = 0
      execOp(parent) { rec =>
        num += 1
      }
      yld(Record(Vector[RField](RInt(num)),Schema("#COUNT")))
    case HashJoin(left, right) =>
      val keys = resultSchema(left) intersect resultSchema(right)
      val hm = new HashMapBuffer(keys, resultSchema(left))
      execOp(left) { rec1 =>
        hm(rec1(keys)) += rec1.fields
      }
      execOp(right) { rec2 =>
        hm(rec2(keys)) foreach { rec1 =>
          yld(Record(rec1.fields ++ rec2.fields, rec1.schema ++ rec2.schema))
        }
      }
    case NprrJoin(parents, outSchema, num_threads) =>
      val tries = parents.map { p => 
        val bintrie = new BitTrie(resultSchema(p))
        execOp(p) { rec => bintrie += 
          rec.fields.map {
            case RInt (i: Rep[Int]) => i.AsInstanceOf[Int]
            case RString (str: Rep[String], len: Rep[Int]) => str.toInt
          }
        }
        //bintrie.buildIntTrie 
        bintrie.buildBitTrie         
        bintrie
      }
      val nprr = new NprrJoinAlgo(tries, outSchema)
      //Measure data loading and preprocessing time
      unchecked[Unit]("clock_t begin, end; double time_spent")
      unchecked[Unit]("begin = clock()")
      nprr.run(yld)
      unchecked[Unit]("end = clock(); printf(\"Query execution time: %f\\n\", (double)(end - begin) / CLOCKS_PER_SEC)")

    case PrintCSV(parent) =>
      val schema = resultSchema(parent)
      printSchema(schema)
      execOp(parent) { rec => printFields(rec.fields) }
  }
  def execQuery(q: Operator): Unit = execOp(q) { _ => }


/**
Algorithm Implementations
*/

  // class NprrJoinAlgo( tries : List[IntTrie], schema : Schema) {
  class NprrJoinAlgo( tries : List[BitTrie], schema : Schema) {

    import trie_const._
    // tries is the list of TrieIterators involved in
    // schema is the result schema of join

    //param: yld is the function we'll introduce later, from NprrJoin
    def run(yld: Record => Rep[Unit]): Rep[Unit] = {
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
        // Option:
        // 1. Have an expanded if-then-else branch for each level
        // 2. Not expand at all
        if (level == schema.length - 1) {
          // yld each result because we've found them (stored in inter_data)
          intersect_on_level(schema.length - 1)
          // TODO: yld(tuple)
          var row = 0
          val record = schema.reverse.tail.reverse.map{ attr=>
            val i = schema indexOf attr
            RInt(inter_data(i, curr_inter_data_index(i)))
          }
          var i = 0
          while (i < inter_data_len(schema.length - 1)) {
            yld(Record(
              record ++ Vector[RField](RInt(inter_data(schema.length - 1, i))), 
              schema))
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
          /*
          val (start, end) = it map { t =>
            val set = head(it indexOf t)
            val s = t.findElemInSetByValue(set, min)
            val e = t.findElemInSetByValue(set, max)
            (s, e)
          } unzip
          */
          val start = it map { t =>
            val set = head(it indexOf t)
            val start = t.findElemInSetByValue(set, min)
            start
          }
          val end = it map { t =>
            val set = head(it indexOf t)
            val end = t.findElemInSetByValue(set, max)
            end
          }
          bitmap_intersectioon(level, it, start, end, min)
        }
      }

      def bitmap_intersectioon(level: Int, arr: List[BitTrie], start: List[Rep[Int]], end: List[Rep[Int]], min: Rep[Int]) = {
        val ints_in_bitmap = end.head - start.head
        var i = 0
        var pos = 0
        /*
        print("start.head: ")
        println(start.head)
        print("end.head: ")
        println(end.head)
        print("min: ")
        println(min) 
        */      
        while (i < ints_in_bitmap) {
          var bitmap = -1
          (arr, start, end).zipped.foreach { case (arr, start, end) =>
            bitmap = readVar(bitmap) & arr(start+i)
          }
          i += 1
          // decode bitmap to a set of int's
          val numbers = uncheckedPure[Int]("__builtin_popcountll(", readVar(bitmap), ")")
          // print("popcount = ")
          // println(numbers)
          pos += numbers
          var k = 1
          while (bitmap != 0) {
            val ntz = uncheckedPure[Int]("__builtin_ctzll(", readVar(bitmap), ")")
            // print("ntz = "); print(ntz); print(" ")
            // println(min + 64*i - ntz - 1)
            inter_data update (level, pos-k, min + 64*i - ntz - 1)
            val ops = uncheckedPure[Int]("1l << ", ntz)
            bitmap = readVar(bitmap) ^ ops
            //println(ops); println(bitmap)
            k += 1
          }
        }
        pos
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
/**
Data Structure Implementations
------------------------------
*/
  /*
  def foreach[T] (arr: Rep[Array[T]], func: Rep[T] => Rep[Unit]) {
    var i = 0
    while (i < arr.length) {
      func(arr(i))
      i += 1
    }
  }
  */

  abstract class ColBuffer
  case class IntColBuffer(data: Rep[Array[Int]]) extends ColBuffer
  case class StringColBuffer(data: Rep[Array[String]], len: Rep[Array[Int]]) extends ColBuffer

  class ArrayBuffer(dataSize: Int, schema: Schema) {
    val buf = schema.map {
      case hd if isNumericCol(hd) => IntColBuffer(NewArray[Int](dataSize))
      case _ => StringColBuffer(NewArray[String](dataSize), NewArray[Int](dataSize))
    }
    var len = 0
    def +=(x: Fields) = {
      this(len) = x
      len += 1
    }
    def update(i: Rep[Int], x: Fields) = (buf,x).zipped.foreach {
      case (IntColBuffer(b), RInt(x)) => b(i) = x
      case (StringColBuffer(b,l), RString(x,y)) => b(i) = x; l(i) = y
    }
    def update(i: Rep[Int], j: Int, x: RField) = (buf(j),x) match {
      case (IntColBuffer(b), RInt(x)) => b(i) = x
      case (StringColBuffer(b,l), RString(x,y)) => b(i) = x; l(i) = y
    }
    def apply(i: Rep[Int]) = buf.map {
      case IntColBuffer(b) => RInt(b(i))
      case StringColBuffer(b,l) => RString(b(i),l(i))
    }
    def apply(i: Rep[Int], j: Int) = buf(j) match {
      case IntColBuffer(b) => RInt(b(i))
      case StringColBuffer(b,l) => RString(b(i),l(i))
    }
  }

  // defaults for hash sizes etc

  object hashDefaults {
    val hashSize   = (1 << 8)
    val keysSize   = hashSize
    val bucketSize = (1 << 8)
    val dataSize   = keysSize * bucketSize
  }

  // common base class to factor out commonalities of group and join hash tables

  class HashMapBase(keySchema: Schema, schema: Schema) {
    import hashDefaults._

    val keys = new ArrayBuffer(keysSize, keySchema)
    val keyCount = var_new(0)

    val hashMask = hashSize - 1
    val htable = NewArray[Int](hashSize)
    for (i <- 0 until hashSize) { htable(i) = -1 }

    def lookup(k: Fields) = lookupInternal(k,None)
    def lookupOrUpdate(k: Fields)(init: Rep[Int]=>Rep[Unit]) = lookupInternal(k,Some(init))
    def lookupInternal(k: Fields, init: Option[Rep[Int]=>Rep[Unit]]): Rep[Int] =
    comment[Int]("hash_lookup") {
      val h = fieldsHash(k).toInt
      var pos = h & hashMask
      while (htable(pos) != -1 && !fieldsEqual(keys(htable(pos)),k)) {
        pos = (pos + 1) & hashMask
      }
      if (init.isDefined) {
        if (htable(pos) == -1) {
          val keyPos = keyCount: Rep[Int] // force read
          keys(keyPos) = k
          keyCount += 1
          htable(pos) = keyPos
          init.get(keyPos)
          keyPos
        } else {
          htable(pos)
        }
      } else {
        htable(pos)
      }
    }
  }

  // hash table for groupBy, storing sums

  class HashMapAgg(keySchema: Schema, schema: Schema) extends HashMapBase(keySchema: Schema, schema: Schema) {
    import hashDefaults._

    val values = new ArrayBuffer(keysSize, schema) // assuming all summation fields are numeric

    def apply(k: Fields) = new {
      def +=(v: Fields) = {
        val keyPos = lookupOrUpdate(k) { keyPos =>
          values(keyPos) = schema.map(_ => RInt(0))
        }
        values(keyPos) = (values(keyPos) zip v) map { case (RInt(x), RInt(y)) => RInt(x + y) }
      }
    }
    def foreach(f: (Fields,Fields) => Rep[Unit]): Rep[Unit] = {
      for (i <- 0 until keyCount) {
        f(keys(i),values(i))
      }
    }

  }

  // hash table for joins, storing lists of records

  class HashMapBuffer(keySchema: Schema, schema: Schema) extends HashMapBase(keySchema: Schema, schema: Schema) {
    import hashDefaults._

    val data = new ArrayBuffer(dataSize, schema)
    val dataCount = var_new(0)

    val buckets = NewArray[Int](dataSize)
    val bucketCounts = NewArray[Int](keysSize)

    def apply(k: Fields) = new {
      def +=(v: Fields) = {
        val dataPos = dataCount: Rep[Int] // force read
        data(dataPos) = v
        dataCount += 1

        val bucket = lookupOrUpdate(k)(bucket => bucketCounts(bucket) = 0)
        val bucketPos = bucketCounts(bucket)
        buckets(bucket * bucketSize + bucketPos) = dataPos
        bucketCounts(bucket) = bucketPos + 1
      }

      def foreach(f: Record => Rep[Unit]): Rep[Unit] = {
        val bucket = lookup(k)

        if (bucket != -1) {
          val bucketLen = bucketCounts(bucket)
          val bucketStart = bucket * bucketSize

          for (i <- bucketStart until (bucketStart + bucketLen)) {
            f(Record(data(buckets(i)),schema))
          }
        }
      }
    }
  }

}}