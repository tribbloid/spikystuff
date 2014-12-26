package org.tribbloid.spikystuff.spike

/**
 * Created by peng on 12/2/14.
 */
object TestCompoundTypeVsExtend {

  case class Base(a: Int = 0, b: Int = 2) {
  }

  trait Aux {
    self: Base =>

    override val b: Int = 10
  }

  class Ext(override val a: Int =3) extends Base(a) with Aux {
//    override val b: Int = 5
  }

  type Comp = Base with Aux

  type SB = Base with Serializable

  def main(args: Array[String]) {

    val b = new Base(3)

//    val e = b.asInstanceOf[Ext]
//    val e = b.asInstanceOf[Comp]
    val e = b.asInstanceOf[SB]

    print(e)
  }
}
