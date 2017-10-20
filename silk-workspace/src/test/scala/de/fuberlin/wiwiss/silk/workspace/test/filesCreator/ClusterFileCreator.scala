package de.fuberlin.wiwiss.silk.workspace.test.filesCreator

import de.fuberlin.wiwiss.silk.workspace.modules.linking.PropertyClusterMember
import de.fuberlin.wiwiss.silk.workspace.modules.linking.SchemaErrors
import io.Source
import xml.{PrettyPrinter, NodeSeq}
import java.io.File
import de.fuberlin.wiwiss.silk.util.XMLUtils

object ClusterFileCreator {
  
  def main(args: Array[String]) {
        
    val home = System.getProperty("user.home")
    println("Home dir:"+home.toString())
    println("File dir:"+s"${home}/.silk/workspace/example/resources/source.nt")
    //read nt file
    var sourceFile = Source.fromFile(s"${home}/.silk/workspace/example/resources/source.nt")

    //store property cluster member objects
    var propertyClusterMembers = readNTriples(sourceFile)
    
    //group by property name
    var clusters = propertyClusterMembers.groupBy(_.property_name)
    
    //write as xml
    var nodeseq = toXML(clusters)
    
    //print file
    var xml_File = new XMLUtils(nodeseq)
    xml_File.write(new File("C:/Users/User/.silk/workspace/example/linking/example/CLUSTERS.xml"))
    
  } 
  
  private val NTriplesRegex = """<([^>]*)>\s*<([^>]*)>\s*[<"]?([^>]*)[>"].*""".r
 
  //read source file
  def readNTriples(source: Source) : Set[PropertyClusterMember] = {    
    var members = Set[PropertyClusterMember]()
  
    for (line <- source.getLines)  {
      var NTriplesRegex(subject_, predicate_, object_) = line

      var existing = members.filter(p => p.entity_uri.equals(subject_) && p.property_name.equals(predicate_))
      var member = PropertyClusterMember()
      if (existing.size==0) {
        member = new PropertyClusterMember("", subject_, predicate_, "default_id", Set[String](), SchemaErrors())
      }
      else {
        member = existing.head
        members = members - existing.head
      }
      
      member.property_values += (object_)
      
      //add the new member
      members = members + member
    }
    println(members.size)
    members
  }
  
   def toXML(clusters: Map[String,Set[PropertyClusterMember]]): NodeSeq = {
    <PropertyClusters>
      { for((label,members) <- clusters) yield {
        
        <cluster>
				<index></index>
				<label>{label}</label>
				<members>
				{for (m <- members) yield {
				  <member>
						<entity_uri>{m.entity_uri}</entity_uri>
						<property_name>{label}</property_name>
						<property_values>
						{for (v <- m.property_values) yield {
						  <Val>
							<e>{v}</e>
				  		</Val>
						}						  
						}
						</property_values>
					</member>
				}}
				</members>
				</cluster>
        
      } }
    </PropertyClusters>
  }
   
  
}