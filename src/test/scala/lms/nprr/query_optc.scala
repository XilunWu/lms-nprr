package scala.lms.nprr

import scala.lms.common._

object query_optc {
trait QueryCompiler extends Dsl with StagedQueryProcessor
with ScannerLowerBase with NprrJoinImp {
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
        val loader = new BitTrieLoader(resultSchema(p))
        execOp(p) { rec => loader += 
          rec.fields.map {
            case RInt (i: Rep[Int]) => i.AsInstanceOf[Int]
            case RString (str: Rep[String], len: Rep[Int]) => str.toInt
          }
        }
        //bintrie.buildIntTrie 
        loader.buildBitTrie
      }
      
      //Measure data loading and preprocessing time
      unchecked[Unit]("clock_t begin, end; double time_spent")
      unchecked[Unit]("begin = clock()")
      // nprr_iterative (tries, outSchema)
      nprr_lambda (tries, outSchema)
      unchecked[Unit]("end = clock(); printf(\"Query execution time: %f\\n\", (double)(end - begin) / CLOCKS_PER_SEC)")
      // unchecked[Unit]("printf(\"Union time: %f\\n\", union_time)")
      // unchecked[Unit]("printf(\"Decode time: %f\\n\", decoding_time)")
    case PrintCSV(parent) =>
      val schema = resultSchema(parent)
      printSchema(schema)
      execOp(parent) { rec => printFields(rec.fields) }
  }
  def execQuery(q: Operator): Unit = execOp(q) { _ => }


/**
Algorithm Implementations
*/
// Moved into intersect_optc.scala

/**
Data Structure Implementations
------------------------------
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