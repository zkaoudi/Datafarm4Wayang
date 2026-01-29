package Generator

trait AbstractJob {
  var filledBody:String = fillBody()
  var filledSBT:String = fillSBT()

  def fillBody(): String
  
  def fillSBT(): String

  def createProject(projectPath:String): Unit 
}

