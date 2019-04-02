package utils

import java.io.{File, FileInputStream}
import java.util
import org.yaml.snakeyaml.Yaml

object AppConfiguration {
  private var props : java.util.HashMap[String,String] = new util.HashMap


  var inputFormat = ""
  var inputPath = ""
  var outputPath = ""
  var maxKtoCompute = ""



  def initializeAppConfig(configFilePath : String) : Unit = {

    val fileInputStream = new FileInputStream(new File(configFilePath))
    val confObj = new Yaml().load(fileInputStream)

    props = confObj.asInstanceOf[java.util.HashMap[String,String]]
    inputFormat = props.get("INPUT_FORMAT")
    inputPath = props.get("INPUT_PATH")
    outputPath = props.get("OUTPUT_PATH")
    maxKtoCompute = props.get("MAX_K")


  }

}
