package utils

import java.util.StringTokenizer

object Helper {

  def tokenize(s: String, delims: String = ","): List[String] = {
    val st = new StringTokenizer(s, delims)
    Iterator.continually(st).takeWhile(_.hasMoreTokens).map(_.nextToken()).toList
  }

}
