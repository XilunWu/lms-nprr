package scala.lms.nprr

import scala.lms.common._
import scala.reflect.SourceContext


// should this be added to LMS?
trait UtilOps extends Base { this: Dsl =>
  def infix_HashCode[T:Typ](o: Rep[T])(implicit pos: SourceContext): Rep[Long]
  def infix_HashCode(o: Rep[String], len: Rep[Int])(implicit v: Overloaded1, pos: SourceContext): Rep[Long]
}
trait UtilOpsExp extends UtilOps with BaseExp { this: DslExp =>
  case class ObjHashCode[T:Typ](o: Rep[T])(implicit pos: SourceContext) extends Def[Long] { def m = typ[T] }
  case class StrSubHashCode(o: Rep[String], len: Rep[Int])(implicit pos: SourceContext) extends Def[Long]
  def infix_HashCode[T:Typ](o: Rep[T])(implicit pos: SourceContext) = ObjHashCode(o)
  def infix_HashCode(o: Rep[String], len: Rep[Int])(implicit v: Overloaded1, pos: SourceContext) = StrSubHashCode(o,len)

  override def mirror[A:Typ](e: Def[A], f: Transformer)(implicit pos: SourceContext): Exp[A] = (e match {
    case e@ObjHashCode(a) => infix_HashCode(f(a))(e.m,pos)
    case e@StrSubHashCode(o,len) => infix_HashCode(f(o),f(len))
    case _ => super.mirror(e,f)
  }).asInstanceOf[Exp[A]]

  override def printsrc(x: =>Any) { System.err.println(x) }
}
trait ScalaGenUtilOps extends ScalaGenBase {
  val IR: UtilOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case ObjHashCode(o) => emitValDef(sym, src"$o.##")
    case _ => super.emitNode(sym, rhs)
  }
}
trait CGenUtilOps extends CGenBase {
  val IR: UtilOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case StrSubHashCode(o,len) => emitValDef(sym, src"hash($o,$len)")
    case _ => super.emitNode(sym, rhs)
  }
}

// we add Long support here
trait Dsl extends PrimitiveOps with NumericOps with MathOps with BooleanOps with LiftString with LiftPrimitives with LiftNumeric with LiftBoolean with IfThenElse with Equal with RangeOps with OrderingOps with MiscOps with ArrayOps with StringOps with SeqOps with Functions with While with StaticData with Variables with LiftVariables with ObjectOps with UtilOps with CastingOps{
  implicit def repStrToSeqOps(a: Rep[String]) = new SeqOpsCls(a.asInstanceOf[Rep[Seq[Char]]])
  override def infix_&&(lhs: Rep[Boolean], rhs: => Rep[Boolean])(implicit pos: scala.reflect.SourceContext): Rep[Boolean] =
    __ifThenElse(lhs, rhs, unit(false))
  def generate_comment(l: String): Rep[Unit]
  def comment[A:Typ](l: String, verbose: Boolean = true)(b: => Rep[A]): Rep[A]
}

trait DslExp extends Dsl with PrimitiveOpsExpOpt with NumericOpsExpOpt with MathOpsExp with BooleanOpsExp with IfThenElseExpOpt with EqualExpBridgeOpt with RangeOpsExp with OrderingOpsExp with MiscOpsExp with EffectExp with ArrayOpsExpOpt with StringOpsExp with SeqOpsExp with FunctionsRecursiveExp with WhileExp with StaticDataExp with VariablesExpOpt with ObjectOpsExpOpt with UtilOpsExp with CastingOpsExp{
  override def boolean_or(lhs: Exp[Boolean], rhs: Exp[Boolean])(implicit pos: SourceContext) : Exp[Boolean] = lhs match {
    case Const(false) => rhs
    case _ => super.boolean_or(lhs, rhs)
  }
  override def boolean_and(lhs: Exp[Boolean], rhs: Exp[Boolean])(implicit pos: SourceContext) : Exp[Boolean] = lhs match {
    case Const(true) => rhs
    case _ => super.boolean_and(lhs, rhs)
  }

  case class GenerateComment(l: String) extends Def[Unit]
  def generate_comment(l: String) = reflectEffect(GenerateComment(l))
  case class Comment[A:Typ](l: String, verbose: Boolean, b: Block[A]) extends Def[A]
  def comment[A:Typ](l: String, verbose: Boolean)(b: => Rep[A]): Rep[A] = {
    val br = reifyEffects(b)
    val be = summarizeEffects(br)
    reflectEffect[A](Comment(l, verbose, br), be)
  }

  override def boundSyms(e: Any): List[Sym[Any]] = e match {
    case Comment(_, _, b) => effectSyms(b)
    case _ => super.boundSyms(e)
  }

  override def array_apply[T:Typ](x: Exp[Array[T]], n: Exp[Int])(implicit pos: SourceContext): Exp[T] = (x,n) match {
    case (Def(StaticData(x:Array[T])), Const(n)) =>
      val y = x(n)
      if (y.isInstanceOf[Int]) unit(y) else staticData(y)
    case _ => super.array_apply(x,n)
  }

  // TODO: should this be in LMS?
  override def isPrimitiveType[T](m: Typ[T]) = (m == manifest[String]) || super.isPrimitiveType(m)
}
trait DslGen extends ScalaGenNumericOps
    with ScalaGenPrimitiveOps with ScalaGenMathOps with ScalaGenBooleanOps with ScalaGenIfThenElse
    with ScalaGenEqual with ScalaGenRangeOps with ScalaGenOrderingOps
    with ScalaGenMiscOps with ScalaGenArrayOps with ScalaGenStringOps
    with ScalaGenSeqOps with ScalaGenFunctions with ScalaGenWhile
    with ScalaGenStaticData with ScalaGenVariables
    with ScalaGenObjectOps
    with ScalaGenUtilOps {
  val IR: DslExp

  import IR._

  override def quote(x: Exp[Any]) = x match {
    case Const('\n') if x.tp == typ[Char] => "'\\n'"
    case Const('\t') if x.tp == typ[Char] => "'\\t'"
    case Const(0)    if x.tp == typ[Char] => "'\\0'"
    case _ => super.quote(x)
  }
  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case IfThenElse(c,Block(Const(true)),Block(Const(false))) =>
      emitValDef(sym, quote(c))
    case PrintF(f:String,xs) => 
      emitValDef(sym, src"printf(${Const(f)::xs})")
    case GenerateComment(s) =>
      stream.println("// "+s)
    case Comment(s, verbose, b) =>
      stream.println("val " + quote(sym) + " = {")
      stream.println("//#" + s)
      if (verbose) {
        stream.println("// generated code for " + s.replace('_', ' '))
      } else {
        stream.println("// generated code")
      }
      emitBlock(b)
      stream.println(quote(getBlockResult(b)))
      stream.println("//#" + s)
      stream.println("}")
    case _ => super.emitNode(sym, rhs)
  }
}
trait DslImpl extends DslExp { q =>
  val codegen = new DslGen {
    val IR: q.type = q
  }
}

// TODO: currently part of this is specific to the query tests. generalize? move?
trait DslGenC extends CGenNumericOps
    with CGenPrimitiveOps with CGenMathOps with CGenBooleanOps with CGenIfThenElse
    with CGenEqual with CGenRangeOps with CGenOrderingOps
    with CGenMiscOps with CGenArrayOps with CGenStringOps
    with CGenSeqOps with CGenFunctions with CGenWhile
    with CGenStaticData with CGenVariables
    with CGenObjectOps
    with CGenUtilOps 
    with CGenCastingOps {
  val IR: DslExp
  import IR._

  def getMemoryAllocString(count: String, memType: String): String = {
      "(" + memType + "*)malloc(" + count + " * sizeof(" + memType + "));"
  }

  //try modifying remapWithRef func
  override def remapWithRef[A](m: Typ[A]): String = remap(m) + " "

  override def remap[A](m: Typ[A]): String = m.toString match {
    case "Int" => "int32_t"
    case "java.lang.String" => "char*"
    case "Array[Char]" => "char*"
    case "Array[Int]"  => "int32_t*"
    case "Char" => "char"
    case _ => super.remap(m)
  }
  override def format(s: Exp[Any]): String = {
    remap(s.tp) match {
      // %lld for numeric type data pointers
      case "int64_t**" => "%lld"
      case "int64_t*" => "%lld"
      case "int32_t**" => "%lld"
      case "int32_t*" => "%lld"
      case "uint16_t" => "%c"
      case "bool" | "int8_t" | "int16_t" | "int32_t" => "%d"
      case "int64_t" => "%lld"
      case "float" | "double" => "%f"
      case "string" => "%s"
      case "char*" => "%s"
      case "char" => "%c"
      case "void" => "%c"
      case _ =>
        import scala.lms.internal.GenerationFailedException
        throw new GenerationFailedException("CGenMiscOps: cannot print type " + remap(s.tp))
    }
  }
  override def quoteRawString(s: Exp[Any]): String = {
    remap(s.tp) match {
      case "string" => quote(s) + ".c_str()"
      case _ => quote(s)
    }
  }
  // we treat string as a primitive type to prevent memory management on strings
  // strings are always stack allocated and freed automatically at the scope exit
  override def isPrimitiveType(tpe: String) : Boolean = {
    tpe match {
      case "char*" => true
      case "char" => true
      case _ => super.isPrimitiveType(tpe)
    }
  }

  override def quote(x: Exp[Any]) = x match {
    case Const(s: String) => "\""+s.replace("\"", "\\\"")+"\"" // TODO: more escapes?
    case Const('\n') if x.tp == typ[Char] => "'\\n'"
    case Const('\t') if x.tp == typ[Char] => "'\\t'"
    case Const(0)    if x.tp == typ[Char] => "'\\0'"
    case _ => super.quote(x)
  }
  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case a@ArrayNew(n) =>
      val arrType = remap(a.m)
      stream.println(arrType + "* " + quote(sym) + " = " + getMemoryAllocString(quote(n), arrType))
      // initialize: memset ( void * ptr, int value, size_t num );
      stream.println("memset(" + quote(sym) + ", 0, " + quote(n) + " * sizeof(" + arrType + "));")
    case ArrayApply(x,n) => emitValDef(sym, quote(x) + "[" + quote(n) + "]")
    case ArrayUpdate(x,n,y) => stream.println(quote(x) + "[" + quote(n) + "] = " + quote(y) + ";")
    case PrintLn(s) => stream.println("printf(\"" + format(s) + "\\n\"," + quoteRawString(s) + ");")
    case StringCharAt(s,i) => emitValDef(sym, "%s[%s]".format(quote(s), quote(i)))
    case Comment(s, verbose, b) =>
      stream.println("//#" + s)
      if (verbose) {
        stream.println("// generated code for " + s.replace('_', ' '))
      } else {
        stream.println("// generated code")
      }
      emitBlock(b)
      emitValDef(sym, quote(getBlockResult(b)))
      stream.println("//#" + s)
    // translate Int to int64_t rather than int32_t
    case DoubleToInt(lhs) => emitValDef(sym, "(int64_t)" + quote(lhs))
    case FloatToInt(lhs) => emitValDef(sym, "(int64_t)" + quote(lhs))
    case IntShiftRightLogical(lhs, rhs) => emitValDef(sym, "(uint64_t)" + quote(lhs) + " >> " + quote(rhs))
    case LongToInt(lhs) => emitValDef(sym, "(int64_t)"+quote(lhs))
    case _ => super.emitNode(sym,rhs)
  }
  override def emitSource[A:Typ](args: List[Sym[_]], body: Block[A], functionName: String, out: java.io.PrintWriter) = {
    withStream(out) {
    	//the head of generated C files. 
      stream.println("""
      #include <fcntl.h>
      #include <errno.h>
      #include <err.h>
      #include <sys/mman.h>
      #include <sys/stat.h>
      #include <stdio.h>
      #include <stdlib.h>
      #include <stdint.h>
      #include <unistd.h>
      #include <time.h>
      #include <x86intrin.h>
      #include <functional>
      #include <stdbool.h>

      using namespace std;
      #define BITS_IN_AVX_REG 128 // or 256

      #ifndef MAP_FILE
      #define MAP_FILE MAP_SHARED
      #endif
      int64_t fsize(int fd) {
        struct stat stat;
        int64_t res = fstat(fd,&stat);
        return stat.st_size;
      }
      int printll(char* s) {
        while (*s != '\n' && *s != ',' && *s != '\t') {
          putchar(*s++);
        }
        return 0;
      }
      long hash(char *str0, int len)
      {
        unsigned char* str = (unsigned char*)str0;
        unsigned long hash = 5381;
        int c;

        while ((c = *str++) && len--)
          hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

        return hash;
      }
      void Snippet(char*);
      int main(int argc, char *argv[])
      {
        if (argc != 2) {
          printf("usage: query <filename>\n");
          return 0;
        }
        Snippet(argv[1]);
        return 0;
      }
      inline int string_toInt(const char * s)
      {
        return atoi(s);
      }

      uint64_t decode2(uint32_t* vec, uint32_t *bitmap, size_t len, size_t min);

      inline uint32_t simd_bitset_intersection_helper(uint32_t * output, uint32_t * a_in, uint32_t * b_in, size_t len) {
        //we assume equal size and bitmaps all are already aligned here:                                                             
        size_t i = 0;
        size_t curr = 0;
        size_t last_non_zero = 0;
        bool flag = false;
        size_t res_len = 0;
        size_t start = 0;
        size_t count = 0;
        uint32_t * set_data = output + 4;
        uint32_t vec[8];

        while ((i+8) <= len) {
            __m256 m_a = _mm256_loadu_ps((float*) &(a_in[i]));
            const __m256 m_b = _mm256_loadu_ps((float*) &(b_in[i]));
            m_a = _mm256_and_ps(m_a, m_b);

            // separate r into 8 uint32_t
            vec[0] = _mm256_extract_epi32((__m256i)m_a, 0);
            vec[1] = _mm256_extract_epi32((__m256i)m_a, 1);
            vec[2] = _mm256_extract_epi32((__m256i)m_a, 2);
            vec[3] = _mm256_extract_epi32((__m256i)m_a, 3);

            vec[4] = _mm256_extract_epi32((__m256i)m_a, 4);
            vec[5] = _mm256_extract_epi32((__m256i)m_a, 5);
            vec[6] = _mm256_extract_epi32((__m256i)m_a, 6);
            vec[7] = _mm256_extract_epi32((__m256i)m_a, 7);

            for(unsigned int index = 0; index < 8; ++index) {
              uint32_t c = vec[index];
              if (flag == false && c != 0) {
                flag = true;
                start = i+index;
              }
              if (flag == true) {
                if (c != 0) {
                  last_non_zero = curr;
                  count += __builtin_popcount(c);
                } 
                set_data[curr++] = c;
              }
            }
            i += 8;
        }
        while (i < len) {
            uint32_t c = a_in[i];
            c &= b_in[i];

            if (flag == false && c != 0)  {
                flag = true;
                start = i;
            }
            if (flag == true) {
              if (c != 0) {
                last_non_zero = curr;
                count += __builtin_popcount(c);
              } 
              set_data[curr++] = c;
            }
            i += 1;
        }
        if (flag == false) res_len = 0;
        else res_len = last_non_zero + 1;
        *(output + 0) = 1; // SetType.BitSet
        *(output + 1) = res_len + 4;
        *(output + 2) = count;
        *(output + 3) = start * (1 << 5);
        return res_len + 4;
      }

      inline uint64_t simd_bitset_intersection_count_helper(uint32_t * a_in, uint32_t * b_in, size_t len) {
        uint64_t count = 0;
        uint32_t i = 0;

        while ((i+8) <= len) {
            __m256 m_a = _mm256_loadu_ps((float*) &(a_in[i]));
            const __m256 m_b = _mm256_loadu_ps((float*) &(b_in[i]));
            m_a = _mm256_and_ps(m_a, m_b);

            // separate r into 8 uint32_t
            count += __builtin_popcount(_mm256_extract_epi32((__m256i)m_a, 0));
            count += __builtin_popcount(_mm256_extract_epi32((__m256i)m_a, 1));
            count += __builtin_popcount(_mm256_extract_epi32((__m256i)m_a, 2));
            count += __builtin_popcount(_mm256_extract_epi32((__m256i)m_a, 3));

            count += __builtin_popcount(_mm256_extract_epi32((__m256i)m_a, 4));
            count += __builtin_popcount(_mm256_extract_epi32((__m256i)m_a, 5));
            count += __builtin_popcount(_mm256_extract_epi32((__m256i)m_a, 6));
            count += __builtin_popcount(_mm256_extract_epi32((__m256i)m_a, 7));

            i += 8;
        }
        while (i < len) {
            uint32_t c = a_in[i];
            c &= b_in[i];
            count += __builtin_popcount(c);
            i += 1;
        }
        return count;
      }

      inline uint32_t simd_bitset_intersection(uint32_t * output, size_t o_start, uint32_t * a_in, size_t a_start, uint32_t * b_in, size_t b_start, size_t len) {
        return simd_bitset_intersection_helper(&output[o_start], &a_in[a_start], &b_in[b_start], len);
      }
      inline uint64_t simd_bitset_intersection_count(uint32_t * a_in, size_t a_start, uint32_t * b_in, size_t b_start, size_t len) {
        return simd_bitset_intersection_count_helper(&a_in[a_start], &b_in[b_start], len);
      }

      inline uint32_t decode2(uint32_t* vec, uint32_t *bitmap, size_t len, uint32_t min) {
          size_t i = 0;
          size_t count = 0;
          while (i < len) {
              size_t num = __builtin_popcount(bitmap[i]);
              uint32_t bitval = bitmap[i];
              count += num;
              for(size_t j = 0; j < num; ++j) {
                  size_t pos = __builtin_ctzl(bitval);
                  bitval ^= 1l << pos;
                  vec[count-j-1] = min+i*32+31-pos;
              }
              i += 1;
          }
          return count;
      }
      inline uint32_t decode(uint32_t* vec, uint32_t *bitmap, size_t start, size_t len, uint32_t min) {
        return decode2(vec, &bitmap[start], len, min);
      }
      """)
    }
    super.emitSource[A](args, body, functionName, out)
  }
}


abstract class DslSnippet[A:Manifest,B:Manifest] extends Dsl {
  def snippet(x: Rep[A]): Rep[B]
}

abstract class DslDriver[A:Manifest,B:Manifest] extends DslSnippet[A,B] with DslImpl with CompileScala {
  lazy val f = compile(snippet)(manifestTyp[A],manifestTyp[B])
  def precompile: Unit = f
  def precompileSilently: Unit = utils.devnull(f)
  def eval(x: A): B = f(x)
  lazy val code: String = {
    val source = new java.io.StringWriter()
    codegen.emitSource(snippet, "Snippet", new java.io.PrintWriter(source))(manifestTyp[A],manifestTyp[B])
    source.toString
  }
}

abstract class DslDriverC[A:Manifest,B:Manifest] extends DslSnippet[A,B] with DslExp { q =>
  val codegen = new DslGenC {
    val IR: q.type = q
  }
  lazy val code: String = {
    implicit val mA = manifestTyp[A]
    implicit val mB = manifestTyp[B]
    val source = new java.io.StringWriter()
    codegen.emitSource(snippet, "Snippet", new java.io.PrintWriter(source))
    source.toString
  }
  def eval(a:A): Unit = { // TBD: should read result of type B?
  	//TODO: modify compilation commands
    val out = new java.io.PrintWriter("/tmp/snippet.c")
    out.println(code)
    out.close
    //TODO: use precompile
    (new java.io.File("/tmp/snippet")).delete
    import scala.sys.process._
    // (s"g++  -mavx2 -fPIC -std=c++0x -pedantic -O3 -Wall -Wno-unused-function -Wextra -march=native -mtune=native /tmp/snippet.c -o /tmp/snippet":ProcessBuilder).lines.foreach(Console.println _)
    // Just compile without running on my laptop
    // (s"/tmp/snippet $a":ProcessBuilder).lines.foreach(Console.println _)
  }
}
