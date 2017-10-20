package de.fuberlin.wiwiss.silk.workspace.test.filesCreator

object RegexTester {
  
  def main(args: Array[String]) {
    
       val PropertyRegex = """.*<([^>]*)>""".r
       var PropertyRegex(clean_prop) = "?a/<http://catalog/T>"
       println(clean_prop)
  }
  
}