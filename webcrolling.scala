package com.haiteam

object webcrolling {
  def main(args: Array[String]): Unit = {
    import scala.io.Source
    import java.nio.charset.CodingErrorAction
    import scala.io.Codec

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.IGNORE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val url = "https://finance.naver.com/"
    var html = Source.fromURL(url)
    val s = html.getLines.mkString("\n")

  }
}
