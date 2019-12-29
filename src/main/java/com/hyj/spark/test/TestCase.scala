package com.hyj.spark.test

object TestCase {
  def main(args: Array[String]): Unit = {
    case class Book(title: String, pages: Int)
    val books = Seq( Book("Future of Scala developers", 85),
      Book("Parallel algorithms", 240),
      Book("Object Oriented Programming", 130),
      Book("Mobile Development", 495) )
    //Book(Mobile Development,495)
    val b1=books.maxBy(_.pages)
    println(b1)
    //Book(Future of Scala developers,85)
    val b2=books.minBy(book => book.pages)
    println(b2)
  }



}
