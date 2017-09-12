package scala.lms.nprr

import scala.lms.common._
import scala.annotation.switch

object query_optc {
trait QueryCompiler extends Dsl with StagedQueryProcessor
with ScannerLowerBase {
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
    def print() = printf("%d",value)
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
    /*
    case LFTJoin(parents, names)        =>
      //Only for TriangleCounting
      val schema = Schema("#X","#Y","#Z")
      schema
    */
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
      /*
    case LFTJoin(parents, names) =>
      val dataSize = Vector(30494867,30494867,30494867)//tablesInQuery.map(t => tableSize(tpchTables indexOf t)) 
      val schemaOfResult = resultSchema(LFTJoin(parents, names))
      //Measure data loading and preprocessing time
      unchecked[Unit]("clock_t begin, end; double time_spent")
      unchecked[Unit]("begin = clock()")
      //1. create trieArrays for those real relations
      //2. create iterators
      sealed abstract class Relation
      case class Origin(name: String) extends Relation
      case class Duplicate(index: Int, schema: Schema) extends Relation
      val schemas = parents.map(p => resultSchema(p))
      val rels = names.zipWithIndex.map{ case (name,index) =>
        val first = names indexOf name
        if (first == index) Origin(name)
        else Duplicate(first,schemas(index))
      }
      val trieArrays = (parents,rels,dataSize).zipped.map { case (p,r,size) => r match {
        case Origin(name)  => 
          val buf = new TrieArray(size, resultSchema(p), schemaOfResult)
          execOp(p) {rec => buf += rec.fields} //fields is of type Fields: Vector[RField]
          buf.noMoreInput
          Some(buf)
        case Duplicate(_,_) =>
          None
      }}
      val trieArrayIterators = rels.map(r => r match {
        case Origin(name) =>  
          //println(name)
          val t = trieArrays(names indexOf name) match {
            case Some(buf) => buf
          }
          new TrieArrayIterator(t, t.schema)
        case Duplicate(index,schema) =>           
          //println(schema)
          val t = trieArrays(index) match {
            case Some(buf) => buf
          }          
          new TrieArrayIterator(t, schema)
      })
      unchecked[Unit]("end = clock(); printf(\"Data loading: %f\\n\", (double)(end - begin) / CLOCKS_PER_SEC)")
      //Measure trie building time
      //trieArrayIterators.foreach(t => t.traversal)
      val join = new LFTJmain(trieArrayIterators, schemaOfResult)
      var i = 0
      while(i < 1) {
        unchecked[Unit]("begin = clock()")
        join.run(yld)
        unchecked[Unit]("end = clock(); printf(\"Join: %f\\n\", (double)(end - begin) / CLOCKS_PER_SEC)")
        i += 1
      }
      */
    case NprrJoin(parents, outSchema, num_threads) =>
      val tries = parents.map { p => 
        val bintrie = new IntTrie(resultSchema(p))
        execOp(p) { rec => bintrie += rec.fields}
        bintrie.buildIntTrie 
        bintrie
      //bintrie.compress
      //bintrie.my_print
      }
      val nprr = new NprrJoinAlgo(tries, outSchema)
      nprr.run(yld)

    case PrintCSV(parent) =>
      val schema = resultSchema(parent)
      printSchema(schema)
      execOp(parent) { rec => printFields(rec.fields) }
  }
  def execQuery(q: Operator): Unit = execOp(q) { _ => }


/**
Algorithm Implementations
*/

  class NprrJoinAlgo( tries : List[IntTrie], schema : Schema) {
    // tries is the list of TrieIterators involved in
    // schema is the result schema of join

    //param: yld is the function we'll introduce later, from NprrJoin
    def run(yld: Record => Rep[Unit]): Rep[Unit] = {
      val curr_set = new Matrix(schema.length, tries.length)
      val inter_data = new Matrix( schema.length, 1 << 12 )
      val curr_inter_data_index = NewArray[Int](schema.length)
      val inter_data_len = NewArray[Int](schema.length)

      var level = 0
      // init on curr_set(0), curr_inter_data_index(0), inter_data(0), and inter_data_len(0)
      init
      println(inter_data_len(0))
      while (level >= 0) {
        print("curr level: ")
        println(level)
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
            print("a")
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
          println(inter_data(level, curr_inter_data_index(level)))
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
                curr_int, 
                curr_set (prev_attr_index, relation))
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
        intersect_on_level_leapfrog(level)
      def intersect_on_level_leapfrog (level : Int) : Rep[Int] = {
        // intersect
        // and put result into inter_data ( level )
        val it = tries.filter( t => t.getSchema.contains(schema(level)))
        val arr = NewArray[Array[Int]]( it.length )
        it foreach { t =>
            arr update (tries indexOf t, t.getTrie)
        }
        val head = NewArray[Int]( it.length )
        it foreach { t =>
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
          print("curr_num = "); print(curr_num)
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
            print("max = "); print(max); print("; min = "); println(min)
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
      val start_of_elem = intTrieConst.sizeof_uint_set_header
      arr(head + start_of_elem + index)
    }
    def uint_trie_reach_end(arr: Rep[Array[Int]], head: Rep[Int], index: Rep[Int]) = {
      val card = arr(head + intTrieConst.loc_of_cardinality)
      index >= card
    }
    // function uint_trie_geq returns the index of first element in array
    // which is no less than value. 
    def uint_trie_geq(arr: Rep[Array[Int]], head: Rep[Int], value: Rep[Int], init_start: Rep[Int]) = {
      val card = arr(head + intTrieConst.loc_of_cardinality)
      var start = head + intTrieConst.sizeof_uint_set_header + init_start
      var end = head + intTrieConst.sizeof_uint_set_header + card
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
        print("curr_v = "); print(arr(start+i)); print(" < value = "); println(value)
        i += 1
      }
      start + i
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
  object intTrieConst{
    val initRawDataLen  = (1 << 20)
    val loc_of_type = 0
    val loc_of_cardinality = 1
    val sizeof_uint_set_header = 2
    val type_uintvec = 1
  }
  
  object binTrieConst{
    val initRawDataLen  = (1 << 10)
    val initDataLen     = (1 << 14)
    val sizeOfBlockHead = 21 //bytes
    //put all const here
    val type_bitvec = 0
    val type_uintvec = 1
    val sizeof_bitvec_header = 3
    val sizeof_uintvec_header = 2
  }
  class Matrix (row: Rep[Int], col: Rep[Int]) {
    val rows = NewArray[Array[Int]](row)
    var i = 0
    while (i < row) {
      val new_row = NewArray[Int](col)
      rows(i) = new_row
      i += 1
    }
    def apply(row_num: Rep[Int]) = rows(row_num)
    def apply(row_num: Rep[Int], col_num: Rep[Int]): Rep[Int] = {
      val row = rows(row_num)
      row(col_num)
    }
    def update(row_num: Rep[Int], col_num: Rep[Int], x: Rep[Int]) = {
      val row = rows(row_num)
      row update (col_num, x)
    }
  }

  class TrieIterator (trie: IntTrie) {
    // class UIntTrieIterator (uIntTrie) extends TrieIterator
    def getTrie: IntTrie = trie

    // element-level method for traversing/searching (LFTJ)
 

    //def setCurrSet
    //def findElemInCurrSet
  }

  class IntTrie (schema: Schema) {
    import intTrieConst._
    //index(i) is the start of child of value(i)
    //the intermediate datum used while generating uintTrie
    val indexArray = new Matrix (schema.length, initRawDataLen)
    val valueArray = new Matrix (schema.length, initRawDataLen)
    val lenArray = NewArray[Int](schema.length)

    //trie in linear array
    val uintTrie = NewArray[Int](initRawDataLen * schema.length * 2)

    def getTrie = uintTrie
    def getSchema = schema

    def +=(x: Fields):Rep[Unit] = {
      val intFields = x.map {
        case RInt (i: Rep[Int]) => i.AsInstanceOf[Int]
        case RString (str: Rep[String], len: Rep[Int]) => str.toInt
      }
      
      var diff = false
      intFields foreach { x =>
        val i = intFields indexOf x
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
/*
    def printTrie = {
      var i = 0
      while (i < schema.length) {
        var j = 0
        while (j < lenArray(i)) {
          print(valueArray(i, j))
          print(" ")
          print(indexArray(i, j))
          println(" ")
          j += 1
        }
        println("")
        i += 1
      }
    }
*/
    def buildIntSet(level: Rep[Int], start: Rep[Int], end: Rep[Int], addr: Rep[Int], addr_index: Rep[Int]) = {
      val v = valueArray(level)
      val num = end - start
      var addr_new = addr
      var addr_index_new = addr_index
      uintTrie(addr) = type_uintvec
      uintTrie(addr + 1) = num

      var i = start
      while (i < end) {
        //value
        uintTrie (addr + sizeof_uint_set_header + i - start) = v(i)
        //index. Except for the last column
        if (level != schema.length - 1) {
          uintTrie (addr + sizeof_uint_set_header + num + i - start) = addr_index_new
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
        indexArray update (i, lenArray(i), lenArray(i + 1))
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
    def findChild(curr_set: Rep[Int], curr_int: Rep[Int]): Rep[Int] = {
      //return -1 if no child found
      val index = curr_int - curr_set - sizeof_uint_set_header
      val set_size = uintTrie(curr_set + 1)
      val child_index_loc = curr_set + sizeof_uint_set_header + set_size + index
      val child = uintTrie(child_index_loc)
      //uninitialized index for last column
      if (child == 0) -1 else child
    }
    //def findParent(curr: Rep[Int]): Rep[Int] = {}
    def findNext(curr_set: Rep[Int], curr_int: Rep[Int]): Rep[Int] = {
      //return -1 if no next element found
      val index = curr_int - curr_set - sizeof_uint_set_header
      val set_size = uintTrie(curr_set + 1)
      if (index >= set_size - 1) -1
      else curr_int + 1
    }
    def findFirstElemInSet(set_head: Rep[Int]): Rep[Int] = {
      val first = set_head + sizeof_uint_set_header
      first
    }
    // set-level method 
    def findChildSetByValue ( value : Rep[Int], set : Rep[Int] ) = {
      // This can be done by helper function. Replace it later
      val arr = uintTrie
      val card = arr(set + intTrieConst.loc_of_cardinality)
      var start = set + intTrieConst.sizeof_uint_set_header
      var end = set + intTrieConst.sizeof_uint_set_header + card
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
      val index = start + i
      findChild(set, index)
    }
    def findFirstSet : Rep[Int] = 0

    def printTrie: Rep[Unit] = {
      val curr_int = NewArray[Int](schema.length)
      val curr_set = NewArray[Int](schema.length)
      val tp = uintTrie(0)
      var level = 0 

      if (tp == type_uintvec) {
        level = 0
        curr_set(0) = 0
        curr_int(0) = findFirstElemInSet(curr_set(0))
        while (level >= 0) {
          //print indent
          var i = 0
          while (i < 8 * level) { print(" "); i += 1 }
          print(uintTrie(curr_int(level)))
          //if it has child (findChild returns positive), go 1 level down
          val child_set = findChild(curr_set(level), curr_int(level))
          if (child_set >= 0) {
            println(" --> ")
            level += 1
            curr_set(level) = child_set
            curr_int(level) = findFirstElemInSet(child_set)
          }
          //if it has no child (findChild returns -1) 
          else {
            println("")
            val next_int = findNext(curr_set(level), curr_int(level))
            //go to next child
            if (next_int >= 0) {
              curr_int(level) = next_int
            }
            // or go 1 level up when reaching end. 
            // Note that we may need go multiple levels up 
            else {
              level -= 1
              var next_int = findNext(curr_set(level), curr_int(level))
              while (next_int < 0) {
                level -= 1
                next_int = findNext(curr_set(level), curr_int(level))
              }
              if (level >= 0) curr_int(level) = next_int
            }
          }
        }
      }
    }
  }
  class BinTrie (schema: Schema) {
    import binTrieConst._

    val intTrie = new IntTrie (schema)

    //initial byte vector size = 16KB
    val rawData = schema.map { a => NewArray[Int](initRawDataLen) }
    var rawLen = 0
    var rawMaxLen = initRawDataLen
    /*
    import scala.reflect.runtime.universe._
    def paramInfo[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val targs = tag.tpe match { case TypeRef(_, _, args) => args }
      println(s"type of $x has type arguments $targs")
    }
    */
    //paramInfo(rawData)

    val data = NewArray[Int](initDataLen)
    var len = 0
    var maxLen = initDataLen

    val bitvecData = schema.map { a => NewArray[Int](initDataLen) }
    val bitLen = NewArray[Int](schema.length)
    val dataLen = NewArray[Int](schema.length) //also is indexLen
    val indexLen = dataLen

    def my_print = {
      var i = 0
      while (i < len) print(data(i).toString + " ")
      println("")
    }

    def += (x:Fields) = {
      /*
      val intFields = x.map {
        case RInt (i: Rep[Int]) => i.AsInstanceOf[Int]
        case RString (str: Rep[String], len: Rep[Int]) => str.toInt
      }
      if (rawLen == rawMaxLen) {
        val newRawData = schema.map{a => NewArray[Int](2*rawMaxLen)}   
        (rawData, newRawData).zipped.foreach {
          case (src, dest) => 
            array_copy(src, 0, dest, 0, rawMaxLen)
        }
        rawMaxLen *= 2
        rawData = newRawData
      }
      

      paramInfo(intFields)
      (intFields,rawData).zipped.foreach { 
        case (field, col) => col(rawLen) = field
      }
      rawLen += 1
      */
      intTrie += x
    }
    def buildIntTrie = {
      //build simple TrieArray from raw data
    }
    def buildBitTrie = {}
    def buildHybridTrie = {}
    /*
    def BytesToInt32(bytes:Rep[Array[Byte]]):Rep[Int] = {
      val i0 = (bytes(0).AsInstanceOf[Int] & 0xff) << 24
      val i1 = (bytes(1).AsInstanceOf[Int] & 0xff) << 16
      val i2 = (bytes(2).AsInstanceOf[Int] & 0xff) << 8
      val i3 = (bytes(3).AsInstanceOf[Int] & 0xff)
      i0 | i1 | i2 | i3
    }
    def Int32ToBytes(i:Rep[Int]):Rep[Array[Byte]] = {
      val bytes = NewArray[Byte](4)
      bytes(0) = ((i >>> 24) & 0xff).AsInstanceOf[Byte]
      bytes(1) = ((i >>> 16) & 0xff).AsInstanceOf[Byte]
      bytes(2) = ((i >>> 8) & 0xff).AsInstanceOf[Byte]
      bytes(3) = (i & 0xff).AsInstanceOf[Byte]
      bytes
    }
    
    def pushByte(byte: Rep[Byte]) = {
      /*
      if (len == maxLen) {
        val newData = NewArray[Byte](maxLen * 2)
        array_copy(data, 0, newData, 0, maxLen)
        maxLen *= 2
        data = newData
      }
      */
      data(len) = byte
      len += 1
    }
    def pushBytes(bytes: Rep[Array[Byte]], arrLen:Rep[Int]) = {
      /*
      if (len+arrLen > maxLen) {
        val len1 = len+arrLen
        val len2 = maxLen*2
        val newLen = Math.max(len1, len2)
        val newData = NewArray[Byte](newLen)
        array_copy(data, 0, newData, 0, maxLen)
        maxLen = newLen
        data = newData
      }
      */
      var i = 0
      while (i < arrLen) {
        data(len + i) = bytes(i)
        i += 1
      }
      len += arrLen
    }
    */
    //def decode(bitv: Rep[Array[Int]], start: Rep[Int]): Rep[Array[Int]]
    def decode(bitv: Rep[Array[Int]]): Rep[Array[Int]] = {
      val blength = bitv.length
      val res = NewArray[Int](32 * (blength - sizeof_bitvec_header))
      val min = bitv(1)

      var next_int_index = 0
      var i = sizeof_bitvec_header
      var base = 0
      while (i < blength) {
        val bit = bitv(i)
        while (base < 32) {
          val mask = 0x80000000 >>> base
          if ((bit & mask) != 0) {
            res(next_int_index) = min + (i - sizeof_bitvec_header) * 32 + base
            next_int_index += 1
          }
          base += 1
        }
        base = 0
        i += 1
      }
      if (next_int_index < res.length) res(next_int_index) = -1
      res
    }
    def encode(arr: Rep[Array[Int]], start: Rep[Int], len: Rep[Int]): Rep[Array[Int]] = {
      val min = arr(start)
      val max = arr(start + len - 1)
      val blength = (max + 31) / 32 + sizeof_bitvec_header
      val bitv = NewArray[Int](blength)

      var i = 0
      var next_bit_loc = 0
      var bit_tmp = 0

      bitv(0) = type_bitvec
      bitv(1) = min
      bitv(2) = max

      while (i < len) {
        val diff = arr(start + i) - min
        if (diff >= 32 * next_bit_loc + 32) {
          bitv(next_bit_loc + sizeof_bitvec_header) = bit_tmp
          next_bit_loc += 1
          bit_tmp = 0
        }
        else {
          bit_tmp = bit_tmp | (0x80000000 >>> diff)
          i += 1
        }
      }
      bitv(next_bit_loc + sizeof_bitvec_header) = bit_tmp
      bitv
    }
    def buildBitmap(arr: Rep[Array[Int]], start: Rep[Int], len: Rep[Int]): Rep[Array[Int]] = {
      val bitv = encode(arr, start, len)
      //remove return value. Insert bitv into where it should be 
      bitv
    }/*
    def buildIndex(arr: Rep[Array[Int]], len: Rep[Int]): Rep[Array[Int]] = {
      if ((rawData indexOf arr) != schema.length - 1) {
        val col_index = rawData indexOf arr
        val child = indexArray(col_index + 1)
        var cursor = 0
        cursor = find_next_max(arr, cursor, len)
      }
      else NewArray[Int](0)
    }*/
    def compress: Rep[Unit] = {
      /*
      var index : Rep[Array[Int]] = unit(null)
      rawData foreach { col => 
        if ((rawData indexOf col) == 0) index = compressColAndIndex(col)
        else if ((rawData indexOf col) == (rawData.length - 1)) compressCol(index, col)
        else index = compressColAndIndex(index, col) 
      }
      refineIndex
      */
      rawData foreach { col => 
        //1. build bit vector
        val bitvec = buildBitmap(col, 0, rawLen) 
        bitLen(rawData indexOf col) = bitvec.length
        val dest = bitvecData(rawData indexOf col)
        var i = 0
        while (i < bitvec.length) {
          dest update (i, bitvec(i))
          i += 1
        }
        //2. build index array
        /*
        if ((rawData indexOf col) != schema.length - 1) {
          val index = buildIndex(col, rawLen)
          i = 0
          while (i < dataLen(rawData indexOf col)) {
            indexArray(rawData indexOf col) update (i, index(i))
            i += 1
          }
        }*/
      }
      //uncompress
    } 
    //used for debugging
    def uncompress: Rep[Unit] = {
      bitvecData foreach { vec =>
        val intv = decode(vec)
        var i = 0
        while (i < intv.length) {
          if (intv(i) < 0) i = intv.length
          else {
            print(intv(i))
            print(" ")
            i += 1
          }
        }
        println("")
      }
      println("")
    }   

    def refineIndex = { //TODO

    }
    /*
    def compressColAndIndex(col: Rep[Array[Int]]): Rep[Array[Int]] = {
        val newIndex = NewArray[Int](rawLen)
        var i = 0;
        var j = 0;
        var lastInt = -1
        while (i < rawLen) {
          if (lastInt == col(i)) i += 1
          else {
            lastInt = col(i)
            newIndex(j) = i
            j += 1
            i += 1
          }
        }
        newIndex(j) = rawLen
        newIndex(j+1) = -1
        compressCol(col)
        newIndex
    }
    def compressColAndIndex(index: Rep[Array[Int]], col: Rep[Array[Int]]): Rep[Array[Int]] = {
      val newIndex = NewArray[Int](rawLen)

      var i = 0;
      var j = 0;
      var k = 0;
      var lastInt = -1
      while (i < rawLen) {
        if (i == index(k)) {
          newIndex(j) = i
          k += 1
          j += 1
          i += 1
          lastInt = col(i)
        }
        else if (lastInt == col(i)) i += 1
        else {
          lastInt = col(i)
          newIndex(j) = i
          j += 1
          i += 1
        }
      }
      newIndex(j) = rawLen
      newIndex(j+1) = -1
      compressCol(index, col)
      newIndex
    }
    def compressCol(col: Rep[Array[Int]]): Rep[Unit] = {
      compressBlock(col, 0, rawLen)
    }
    def compressCol(index: Rep[Array[Int]], col: Rep[Array[Int]]): Rep[Unit] = {
      var j = 1
      while (index(j) != -1) {
        compressBlock(col, index(j-1), index(j))
        j += 1
      }
    }
    def compressBlock(col: Rep[Array[Int]], start: Rep[Int], end: Rep[Int]) = {
      var i = start
      var num = 1
      val min = col(start)
      val max = col(end-1)
      val range = max - min
      while (i < end-1) {
        if (col(i) != col(i+1)) num += 1
        i += 1
      }
      writeHead(num, min, max)
      val typeOfSet = if (1.0 * range/num > 32) 0.asInstanceOf[Byte] else 1.asInstanceOf[Byte]
      //write data
      if (typeOfSet == 0) {       //UInt
        pushBytes(Int32ToBytes(col(start)), 4)
        i = start + 1
        while (i < end) {
          if (col(i-1) != col(i)) pushBytes(Int32ToBytes(col(i)), 4)
          i += 1
        }
        //allocate space for indices
        val sizeOfIndex = 4*num
        len += sizeOfIndex
      } else {                          //Bitset
        val sizeOfSet = (max - min + 8)/8  //bytes
        //pushBytes(makeBitset(col, start, end), sizeOfSet)
        //allocate space for indices
        val sizeOfIndex = (range+8)/8*4
        len += sizeOfIndex
      }
    }
    def writeHead(numOfInt: Rep[Int], min: Rep[Int], max: Rep[Int]): Rep[Unit] = {
      val range = max-min
      val sizeOfHead = binTrieConst.sizeOfBlockHead
      val typeOfSet: Rep[Byte] = if (1.0 * range/numOfInt > 32) 0.asInstanceOf[Byte] else 1.asInstanceOf[Byte]
      val sizeOfSet = if (typeOfSet == 0) 4*numOfInt else (range+8)/8
      val sizeOfIndex = if (typeOfSet == 0) 4*numOfInt else (range+8)/8*4
      val sizeOfBlock = sizeOfHead + sizeOfSet + sizeOfIndex
      val startOfIndex = sizeOfHead + sizeOfSet
      pushBytes(Int32ToBytes(sizeOfBlock), 4)
      pushBytes(Int32ToBytes(numOfInt), 4)
      pushBytes(Int32ToBytes(startOfIndex), 4)
      pushBytes(Int32ToBytes(min), 4)
      pushBytes(Int32ToBytes(max), 4)
      pushByte(typeOfSet)
    }
*/
  }

  def access(i: Rep[Int], len: Int)(f: Int => Rep[Unit]): Rep[Unit] = {
    if(i < 0) unit()
    else accessHelp(i, len, 0)(f)
  }
  def accessHelp(i: Rep[Int], len: Int, count: Int)(f: Int => Rep[Unit]): Rep[Unit] = {
    if (i == count)
      f(count)
    else if (count < len - 1)
      accessHelp(i, len, count + 1)(f)
    else
      unit()
  }
  class TrieArrayIterator(ta: TrieArray, schema: Schema) {
    val schemaOfResult = ta.schemaOfResult
    val valueArray = ta.valueArray
    val indexArray = ta.indexArray
    val lenArray = ta.lenArray
    val cursor = schema.map{f => var_new[Int](0)}
    val flagTable = schemaOfResult.map(f => schema contains f)
    val levelTable = schema.map(f => schemaOfResult indexOf f)
    def hasCol(i: Int): Boolean = (i >= 0) && (i < schemaOfResult.length) && flagTable(i)
    def levelOf(i : Int): Int = levelTable indexOf i
    def traversal:Rep[Unit] = ta.traversal
    def key(level:Int): RField = {
      val lv:Int = levelOf(level)
      valueArray(cursor(lv),lv)
    }
    def open(level:Int): Rep[Unit] = {
      val lv:Int = levelOf(level)
      if (lv == 0) var_assign(cursor(0),0)
      else var_assign(cursor(lv),indexArray(lv-1)(cursor(lv-1)))
    }
    def next(level:Int): Rep[Unit] = {
      val lv:Int = levelOf(level)
      cursor(lv) += 1
    }
    def atEnd(level:Int): Rep[Boolean] = {
      val lv:Int = levelOf(level)
      if (lv == 0 && cursor(lv) == lenArray(lv)) true
      else if (lv != 0 && cursor(lv) == indexArray(lv-1)(cursor(lv-1)+1)) true
      else false
    }
    def seek(level:Int,seekKey: RField): Rep[Unit] = {
      val lv:Int = levelOf(level)
      val start = readVar(cursor(lv))
      if (!(valueArray(cursor(lv),lv) compare seekKey)) {
        val end:Rep[Int] = if (lv == 0) lenArray(0) else indexArray(lv-1)(cursor(lv-1)+1)
        interpolation_search(lv,seekKey,start,end)
        //bsearch(lv,seekKey,start,end)
      }
    }
    //not good for encoded
    def expSearch(lv:Int,seekKey:RField,start:Rep[Int],end:Rep[Int]):Rep[Unit] = {
      val size = end-start
      if (size == 1) {
        if (valueArray(start,lv) lessThan seekKey) var_assign(cursor(lv),end)
        else var_assign(cursor(lv),start)
      } else {
        var bound = 1
        var flag = false
        while(bound < size && (valueArray(start+bound,lv) lessThan seekKey)) {
          if (valueArray(start+bound,lv) compare seekKey) {var_assign(cursor(lv),start+bound);bound = size;flag = true}
          else bound*=2
        }
        if(!flag) {
          val min = if(start+bound+1 < end) start+bound+1 else end        
          bsearch(lv,seekKey,start+bound/2,min)
        }
      }
    }
    def interpolation_search(lv:Int, seekKey: RField, start: Rep[Int], end: Rep[Int]): Rep[Unit] = {
      var vstart = start
      var vend = end
      while(vstart != vend) {
        //if less than 5 elements, do linear search instead of b-search
        if (vend - vstart < 5) {vstart = lsearch(lv,seekKey,vstart,vend); vend = vstart}
        else {
          var diff = seekKey - valueArray(vstart,lv)
          var range = valueArray(vend-1,lv) - valueArray(vstart,lv)
          if (diff <=0 ) vend = vstart
          else if (diff > range) vstart = vend
          else {
            var mid = vstart + (1.0 * diff * (vend - vstart - 1) 
            / range).toInt
            val pivot = valueArray(mid,lv)
            if (pivot compare seekKey) {vstart = mid; vend = vstart}
            else if (pivot lessThan seekKey) {vstart = mid + 1}
            else {vend = mid}
          }
        }
      }
      var_assign(cursor(lv),vstart)
    }
    def lsearch(lv:Int, seekKey: RField, start: Rep[Int], end: Rep[Int]): Rep[Int] = {
      var vstart = start
      var vend = end
      while(vstart != vend) {
        if (valueArray(vstart,lv) lessThan seekKey) vstart += 1
        else vend = vstart
      }
      vstart
    }
    def bsearch(lv:Int, seekKey: RField, start: Rep[Int], end: Rep[Int]): Rep[Unit] = {
      //if the current value is the seekKey it means we don't need actually do search.
      //there're many search strategies for searching which are data dependently. 
      //we want to have some optimal strategies for general data.
      var vstart = start
      var vend = end
      while(vstart != vend) {
        //if less than 5 elements, do linear search instead of b-search
        if (vend - vstart < 5) {vstart = lsearch(lv,seekKey,vstart,vend); vend = vstart}
        else {
          val pivot = valueArray((vstart + vend) / 2,lv)
          if (pivot compare seekKey) {vstart = (vstart + vend) / 2; vend = vstart}
          else if (pivot lessThan seekKey) {vstart = (vstart + vend) / 2 + 1}
          else {vend = (vstart + vend) / 2}
        }
      }
      var_assign(cursor(lv),vstart)
    }
    def resetCursor = {
      cursor.foreach{c => 
        var_assign(c, 0)
      }
    }
  }

  class TrieArray (dataSize: Int, val schema: Schema, val schemaOfResult: Schema) {
    val valueArray = new ArrayBuffer(dataSize, schema)
    val indexArray = schema.map(f => NewArray[Int](dataSize))
    val lenArray = schema.map{f => var_new[Int](0)}
    def +=(x: Fields):Rep[Unit] = {
      var diff = false
      x.foreach { xx =>
        val i = x indexOf xx
        if (lenArray(i) == 0) diff = true
        else if (!diff) diff = !(valueArray((lenArray(i)-1),i) compare xx)
        if (diff) {
          valueArray.update(lenArray(i),i,xx)
          if (i != schema.length - 1) indexArray(i)(lenArray(i)) = lenArray(i+1)          
          lenArray(i)+=1
        }
      }
    }
    def noMoreInput:Rep[Unit] = {
      indexArray.zipWithIndex.foreach{ case(array,i) =>
        if (i != schema.length - 1) array(lenArray(i)) = lenArray(i+1)
      }
    }
    def traversal:Rep[Unit] = {
      schema.foreach{ a =>
        val i = schema indexOf a
        var j = 0
        var k = 1
        while (j < lenArray(i)) {
          valueArray(j,i).print
          print(" ")
          if (i != 0) {
            if(j == indexArray(i-1)(k) - 1) {
              k += 1
              print("| ")
            }
          }
          j+=1
        }
        println("")
        println("")
      }
    }
  }

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

  /**
      Trie-Join
      --------------------------
      */
  class LFTJmain (rels : List[TrieArrayIterator], schema: Schema){
    //ArrayBuffer(1,schema) is inefficient
    val maxkeys = new ArrayBuffer(1, schema)
    val minkeys = new ArrayBuffer(1, schema)
    val res = new ArrayBuffer(1, schema)
    var currLv = 0
    def run(yld: Record => Rep[Unit]) = {
      while (currLv != -1) {
        unlift(leapFrogJoin)(yld, currLv, schema.length)
      }
      //reset all variables
      currLv = 0
      rels foreach {r => r.resetCursor}
    }
    def leapFrogJoin(level: Int, yld: Record => Rep[Unit]): Rep[Unit] = {
      /*print("level ")
      print(level)
      print(": ")*/
      if (rels.filter(r => r.hasCol(level)).length > 1) search(level)
      if (level == schema.length - 1) {
        if (atEnd(level)) {currLv -= 1; next(level-1)/*; println("up")*/}
        else {pushIntoRes(level, key(level)); next(level); yld(makeRecord)/*; print("found one:"); printFields(res(0))*/}
      }
      else {
        if (atEnd(level)) {currLv -= 1; next(level-1)/*; println("up")*/}
        else {pushIntoRes(level, key(level)); currLv += 1; open(level+1)/*; println("open")*/}
      }
    }
    def unlift(f: (Int, Record => Rep[Unit]) => Rep[Unit])(yld: Record => Rep[Unit], numUnLift: Rep[Int], upperBound: Int, lowerBound: Int = 0, count: Int = 0): Rep[Unit] = {
      if (numUnLift == count)
        f(count,yld)
      else if (lowerBound <= count && count < upperBound - 1)
        unlift(f)(yld, numUnLift, upperBound, lowerBound, count + 1)
      else unit() //exit
    }
    def next(level: Int): Rep[Unit] = {
      //call iterator.next for every iterator in vector
      rels.filter(r => r.hasCol(level)) foreach {r => r.next(level)}
    }
    def search(level: Int): Rep[Unit] = {
      val ops = rels.filter(r => r.hasCol(level))
      var keyFound = false
      maxkeys.update(0, level, key(level))
      keys(level).foreach {k => 
        if(maxkeys(0,level) lessThan k)              
          maxkeys.update(0,level,k)    
      }
      while (!keyFound && !atEnd(level)) {
        val kArray = keys(level)
        minkeys.update(0, level, kArray(0))
        kArray foreach { k =>
          if (k lessThan minkeys(0,level))
            minkeys.update(0,level,k)    
        }
        keyFound = maxkeys(0,level) compare minkeys(0,level)
        if (!keyFound) ops.foreach { r => 
          r.seek(level, maxkeys(0,level))
          if (!r.atEnd(level)) maxkeys.update(0,level,r.key(level))
        }
      }
    }
    def key(level: Int) = keys(level)(0)
    def keys(level: Int) = {
      val array = rels.filter(r => r.hasCol(level)).map{r => r.key(level)}
      array
    }
    def atEnd(level: Int): Rep[Boolean] = //rels.filter(r => r.hasCol(level)).foldLeft(unit(false))((a, x) => a || x.atEnd(level))
    {
      val ops = rels.filter(r => r.hasCol(level))
      val length = ops.length
      def F(i:Int):Rep[Boolean] = {
        if (i < length-1) {
          if (ops(i).atEnd(level)) true
          else F(i+1)
        }
        else ops(i).atEnd(level)
      }
      F(0)
    }
    def open(level:Int): Rep[Unit] = rels.filter(r => r.hasCol(level)).foreach(r => r.open(level))
    def makeRecord: Record = {
      Record(res(0), schema)
    }
    def pushIntoRes(level: Int, key: RField) = {
      res.update(0,level,key)
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