package de.fuberlin.wiwiss.silk.workspace.io

import io.Source
import de.fuberlin.wiwiss.silk.entity.{Entity, EntityDescription, Path}
import de.fuberlin.wiwiss.silk.workspace.modules.linking.{PropertyCluster,PropertyClusterMember,SchemaErrors, PropertyClusterTableColumn}
import de.fuberlin.wiwiss.silk.util.XMLUtils
import java.io.File
import xml.NodeSeq

object PropertyClusterImporter {
  
   def loadInfoForXML(sourcePath: File, property_index: IndexedSeq[Path]): Traversable[PropertyCluster] =  {
     
     //transform property index to a map <property, index number as string>
    var index = transform(property_index)
     
    //read source file 
    var sourceFile = io.Source.fromFile(sourcePath)
    
    //create cluster members
    var propertyClusterMembers = readNTriples(sourceFile,index)
    
    //create cluster objects out of member objects
    var clusters = Traversable[PropertyCluster]()
    
    var tableColumns = propertyClusterMembers.groupBy(member => (member.cluster_id, member.property_name, member.table_id))
    for ((tableKey, members) <- tableColumns){
      var column = new PropertyClusterTableColumn(tableKey._1, tableKey._3, members)
      
      var existingCluster = clusters.filter(c => c.index == column.cluster_id)
      if (existingCluster.isEmpty) {
        clusters = clusters ++ Seq(new PropertyCluster(tableKey._1, tableKey._2, Seq(PropertyClusterTableColumn(column.cluster_id, column.table_id, column.members) )))
      }
      //TODO: head because there cant be two clusters with the same id - but consider implementing ids
      else existingCluster.head.tableColumns = existingCluster.head.tableColumns ++ Seq(PropertyClusterTableColumn(column.cluster_id, column.table_id, column.members) )
    }
    
    clusters
   }
  
   def writeXML (clusters:Traversable[PropertyCluster], output:File) {
    
    //create xml node sequence
    var nodeseq = toXML(clusters)
    
    //sort the clusters by index 
    val updatedXml = <PropertyClusters>{(nodeseq \ "cluster").sortBy(x => (x \ "index").text)}</PropertyClusters>
    
    //print file
    var xml_File = new XMLUtils(updatedXml)

    xml_File.write(output)
    
  }
   
   
   private val PropertyRegex = """.*<([^>]*)>""".r
   def transform(property_index: IndexedSeq[Path]) : IndexedSeq[String] = {
     
     var index = for (p <- property_index.indices) yield {
       var PropertyRegex(clean_prop) = property_index(p).toString()
       clean_prop
     }
     index
   }
   
   
  private val NTriplesRegex = """<([^>]*)>\s*<([^>]*)>\s*[<"]?([^>]*)[>"].*""".r
  private val domainRegex = """http:\/\/([^\/]+)\/.*""".r

  def readNTriples(source: Source, index:IndexedSeq[String]) : Set[PropertyClusterMember] = {    
    var members = Set[PropertyClusterMember]()
    var entities_tablesIDs = Map[String, String]()
    
    for (line <- source.getLines)  {
      var NTriplesRegex(subject_, predicate_, object_) = line

      if (predicate_.contains("url")) {
        var domainRegex(domain) = object_
        entities_tablesIDs += (subject_ -> domain)
      }
      else {
        var existing = members.filter(p => p.entity_uri.equals(subject_) && p.property_name.equals(predicate_))
        var member = PropertyClusterMember()
        if (existing.size==0) {
          member = new PropertyClusterMember(index.indexOf(predicate_).toString(), subject_, predicate_, "defaultID", Set[String](), SchemaErrors())
        }
        else {
          member = existing.head
          members = members - existing.head
        }
        
        member.property_values += (object_)
        
        //add the new member
        members = members + member
        }
      
    }
    
    //add the table_ids to every cluster member
    for (m <- members) {
      m.table_id = entities_tablesIDs(m.entity_uri)
    }
    members
  }
  
  
  def toXML(clusters: Traversable[PropertyCluster]): NodeSeq = {
    
    <PropertyClusters>
      { for(c <- clusters) yield {
        
        <cluster>
				<index>{c.index}</index>
				<label>{c.label}</label>
		    {
		    var tableColumns_ = c.tableColumns
		    for (t <- tableColumns_) yield {
		      <table_column>
		      <table_id>{t.table_id}</table_id>
				  <members>

  				{for (m <- t.members) yield {
  
  				  <member>
						<entity_uri>{m.entity_uri}</entity_uri>
						<property_name>{m.property_name}</property_name>
						<property_values>
						{
						  for (v <- m.property_values) yield {
						
  						  <Val>
							<e>{v}</e>
				  		</Val>
  						}						  
  				  }
						</property_values>
					</member>
  				}}
  			</members>
  			</table_column>
				}}
		    
				</cluster>
        
      } }
    </PropertyClusters>
  }
  
}