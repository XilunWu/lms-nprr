package scala.lms.nprr

import scala.lms.common._

/**
Relational Algebra AST
----------------------

The core of any query processing engine is an AST representation of
relational algebra operators.
*/
trait QueryAST {
  type Table
  type Schema = Vector[String]

  // relational algebra ops
  sealed abstract class Operator
  case class Scan(name: Table, schema: Schema, delim: Char, extSchema: Boolean) extends Operator
  case class PrintCSV(parent: Operator) extends Operator
  case class Project(outSchema: Schema, inSchema: Schema, parent: Operator) extends Operator
  case class Filter(pred: Predicate, parent: Operator) extends Operator
  case class Join(parent1: Operator, parent2: Operator) extends Operator
  case class Group(keys: Schema, agg: Schema, parent: Operator) extends Operator
  case class HashJoin(parent1: Operator, parent2: Operator) extends Operator
  case class LFTJoin(parents: List[Operator], names: List[String]) extends Operator
  //TODO: NprrJoin
  case class NprrJoin(parents:List[Operator], outSchema:Schema, num_threads:Int) extends Operator
  case class Count(parent: Operator) extends Operator

  // filter predicates
  sealed abstract class Predicate
  case class Eq(a: Ref, b: Ref) extends Predicate
  case class LT(a: Ref, b: Ref) extends Predicate
  case class GTE(a: Ref, b: Ref) extends Predicate
  case class CON(a: Ref, b: Ref) extends Predicate

  sealed abstract class Ref
  case class Field(name: String) extends Ref
  case class Value(x: Any) extends Ref

  // some smart constructors
  def Schema(schema: String*): Schema = schema.toVector
  def Scan(tableName: String): Scan = Scan(tableName, None, None)
  def Scan(tableName: String, schema: Option[Schema], delim: Option[Char]): Scan
}

trait QueryProcessor extends QueryAST {
  def version: String
  val defaultFieldDelimiter = ','

  def filePath(table: String): String = System.getProperty("user.dir") + "/src/data/" + table
  def dynamicFilePath(table: String): Table

  def Scan(tableName: String, schema: Option[Schema], delim: Option[Char]): Scan = {
    val dfile = dynamicFilePath(tableName)
    val (schema1, externalSchema) = schema.map(s=>(s,true)).getOrElse((loadSchema(filePath(tableName)),false))
    Scan(dfile, schema1, delim.getOrElse(defaultFieldDelimiter), externalSchema)
  }

  def loadSchema(filename: String): Schema = {
    val s = new Scanner(filename)
    val schema = Schema(s.next('\n').split(defaultFieldDelimiter): _*)
    s.close
    schema
  }
  //query execution function. overriden by specific engine
  def execQuery(q: Operator): Unit
}

trait PlainQueryProcessor extends QueryProcessor {
  type Table = String
}

trait StagedQueryProcessor extends QueryProcessor with Dsl {
  type Table = Rep[String] // dynamic filename
  override def filePath(table: String) = super.filePath(table)
}

trait Engine extends QueryProcessor with ExpectedASTs{
  import java.io._

  val debug_writecode = true
  def query: String
  def filename: String
  def queryplan: Operator
  def eval: Unit
  def prepare: Unit = {}
  def run: Unit = execQuery(PrintCSV(queryplan))

  def liftTable(n: String): Table
  override def dynamicFilePath(table: String): Table =
    liftTable(filePath(table))
  //print program's output to screen
  def evalString = {
    val source = new java.io.ByteArrayOutputStream()
    utils.withOutputFull(new java.io.PrintStream(source)) {
      eval
    }
    source.toString
  }
  //print code out
  val outputDir = "src/out/"
  def writeFileIndented(name: String, content: String) {
    val out = new java.io.PrintWriter(new File(name))
    printIndented(content)(out)
    out.close()
  }
  def writeCode(problem: String, raw_code: String, target_code: String = "scala") = {
    val name = outputDir+problem+"."+target_code
    println("Writing code into file: " + name)
    writeFileIndented(name, raw_code)
  }
  def printIndented(str: String)(out: PrintWriter): Unit = {
    val lines = str.split("[\n\r]")
    var indent = 0
    for (l0 <- lines) {
      val l = l0.trim
      if (l.length > 0) {
        var open = 0
        var close = 0
        var initClose = 0
        var nonWsChar = false
        l foreach {
          case '{' => {
            open += 1
            if (!nonWsChar) {
              nonWsChar = true
              initClose = close
            }
          }
          case '}' => close += 1
          case x => if (!nonWsChar && !x.isWhitespace) {
            nonWsChar = true
            initClose = close
          }
        }
        if (!nonWsChar) initClose = close
        out.println("  " * (indent - initClose) + l)
        indent += (open - close)
      }
    }
  }
}
trait StagedEngine extends Engine with StagedQueryProcessor {
  override def liftTable(n: String) = unit(n)
}

object Run {
  var qu: String = _
  var fn: String = _

  trait MainEngine extends Engine {
    override def query = qu
    override def filename = fn
    override def queryplan = expectedAstForTest(query)
  }

  def unstaged_engine: Engine =
    new Engine with MainEngine with query_unstaged.QueryInterpreter {
      override def liftTable(n: Table) = n
      override def eval = run
    }
  def scala_engine =
    new DslDriver[String,Unit] with ScannerExp
    with StagedEngine with MainEngine with query_staged.QueryCompiler { q =>
      override val codegen = new DslGen with ScalaGenScanner {
        val IR: q.type = q
      }
      override def snippet(fn: Table): Rep[Unit] = run
      override def prepare: Unit = {precompile; if (debug_writecode) writeCode(qu, code, "scala")}
      override def eval: Unit = eval(filename)
    }
  def c_engine =
    new DslDriverC[String,Unit] with ScannerLowerExp
    with StagedEngine with MainEngine with query_optc.QueryCompiler { q =>
      override val codegen = new DslGenC with CGenScannerLower {
        val IR: q.type = q
      }
      override def snippet(fn: Table): Rep[Unit] = run
      override def prepare: Unit = {}
      override def eval: Unit = {if (debug_writecode) writeCode(qu, code, "c"); /*eval(filename)*/}
    }

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("syntax:")
      println("   test:run (unstaged|scala|c) test_case")
      println()
      println("example usage:")
      println("   test:run c t1")
      return
    }
    val version = args(0)
    val engine = version match {
      case "c" => c_engine
      case "scala" => scala_engine
      case "unstaged" => unstaged_engine
      case _ => println("warning: unexpected engine, using 'unstaged' by default")
        unstaged_engine
    }
    qu = args(1)
    if (args.length > 2)
      fn = args(2)

    try {
      engine.prepare
      utils.time(engine.eval)
    } catch {
      case ex: Exception =>
        println("ERROR: " + ex)
    }
  }
}

trait ExpectedASTs extends QueryAST {
  val scan_t = Scan("t.csv")

  val num_threads = 1

  val edge_0_1 = Scan("t.csv", Some(Schema("0","1")), Some('\t'))
  val edge_0_2 = Scan("t.csv", Some(Schema("0","2")), Some('\t'))
  val edge_1_2 = Scan("t.csv", Some(Schema("1","2")), Some('\t'))
  val count_t = NprrJoin(List(edge_0_1, edge_0_2, edge_1_2),
    Schema("0", "1", "2"),
    num_threads)

  val expectedAstForTest = Map(
    "t1" -> scan_t,
    "triangle_counting" -> count_t
  ) 
}
