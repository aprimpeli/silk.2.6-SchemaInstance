/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.fuberlin.wiwiss.silk.workspace.modules.linking

import de.fuberlin.wiwiss.silk.output.Output
import de.fuberlin.wiwiss.silk.datasource.{Source}
import xml.Node
import de.fuberlin.wiwiss.silk.util.{Identifier, ValidatingXMLReader}
import de.fuberlin.wiwiss.silk.runtime.resource.ResourceLoader
import de.fuberlin.wiwiss.silk.entity.{Entity, EntityDescription, PathOperator}
import de.fuberlin.wiwiss.silk.config.Prefixes
import java.io.{InputStream, File}
import xml.{Node, XML, NodeSeq}
import scala.collection.mutable.HashSet
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.util.DPair
import de.fuberlin.wiwiss.silk.linkagerule.similarity.Comparison
import de.fuberlin.wiwiss.silk.linkagerule.input.PathInput
import de.fuberlin.wiwiss.silk.linkagerule.input.Input
import de.fuberlin.wiwiss.silk.linkagerule.input.TransformInput
import de.fuberlin.wiwiss.silk.evaluation.EvaluationResult
import scala.util.Random
import de.uni_mannheim.informatik.dws.winter.similarity.string.JaccardOnNGramsSimilarity
import de.fuberlin.wiwiss.silk.plugins.distance.tokenbased.SoftJaccardDistance
import de.fuberlin.wiwiss.silk.evaluation.ReferenceEntities



/**
 * @author Anna Primpeli
 *
 */
case class PropertyClusters(var clusters: Traversable[PropertyCluster] ){
  
  //a member can be part of more than one clusters
  def locateMember(entity_uri:String, property_name:String) : Set[PropertyClusterMember] = {
     
     var members =  Set[PropertyClusterMember]()
   
    var example =  for (c <- clusters) yield {
      for (t <- c.tableColumns) yield {
        for (m <- t.members) yield {
         if (m.entity_uri==entity_uri && c.label==property_name) {
           members.+= (m)
         }        
       }
      }
       
     }
     
    members
  }
  
   def deleteMember(tableColumn: PropertyClusterTableColumn) = {
     
     for (c <- clusters){
       for (t <- c.tableColumns){
            if (tableColumn.cluster_id.equals(c.index)) c.tableColumns = c.tableColumns.filterNot(m => m.equals(tableColumn)) 

       }
     }
  }
   
   def addMember (tableColumn: PropertyClusterTableColumn, cluster_id:String) = {
     
     for (c <- clusters){
       if (cluster_id.equals(c.index)) {
         //update the cluster id and reset the doubt counter in the members  
         tableColumn.members.foreach { m =>
            m.cluster_id=cluster_id
            m.property_name=c.label
            m.schema_errors.doubts=0
          }
         c.tableColumns = c.tableColumns ++ Seq(PropertyClusterTableColumn(cluster_id, tableColumn.table_id, tableColumn.table_column_id, tableColumn.correct_label,  tableColumn.members))
        }
     }
   }
   
   def recluster(tableColumn:PropertyClusterTableColumn,  new_cluster_id:String) = {
     
     println("Cluster Member:"+tableColumn.cluster_id+"--"+tableColumn.table_id+" is moved to cluster with id "+new_cluster_id)
     //delete from initial locations
     deleteMember (tableColumn)
     
     //add to new clusters
     addMember(tableColumn, new_cluster_id)
   }
   


   def updateSchemaInfo(rule: LinkageRule, training_result: EvaluationResult, schemaAnalyzer: SchemaAnalyzer ) {
     
     rule.getComparisonOperators()
     
     for (e <- training_result.falseNegativeEntities.get ++ training_result.truePositiveEntities.get) {
          var possibly_wrong_properties = schemaAnalyzer.analyzeSchemaOfEntityPair(rule, training_result, e)
          
          //we believe that falsePositives cannot give any information on schema reclustering
          for (wp <- possibly_wrong_properties) {
                
             //normalize the property name as it is in the form of path
             val PropertyRegex = """.*<([^>]*)>""".r
             var PropertyRegex(property_name) = wp.toString()
             
             //TODO check. is that always right?
             var candidate_cluster_members = locateMember(e.source.uri, property_name)
             
             for (c <- candidate_cluster_members) {
               if (!c.cluster_id.equals("")) {
                  c.schema_errors.doubts += 1
                
               }
             }
             
           }
       }
     }
   
   def evaluateReclusteringCondition () : Set [(PropertyClusterMember, String, String)] = {
     
     var feedback = Set [(PropertyClusterMember, String, String)]()
     //TODO change reclustering condition
     //filter the table for which one doubt is raised
     var toBeReclustered = clusters.flatMap(_.tableColumns).filter(doubtCondition(_)==true).toSet
     
     //recluster per table
     for ( tableColumn <- toBeReclustered) {       
       var new_cluster= findAppropriateCluster(tableColumn)
       recluster(tableColumn, new_cluster.toString())  
       for (m <- tableColumn.members)
         feedback += ((m,tableColumn.cluster_id, new_cluster.toString()))
       
     }
     feedback
   }
   
   /**
 * @param tableColumn
 * @return
 * Decide where to recluster the tableColumn
 * Based on extended Jaccard for lists 
 */
  def findAppropriateCluster(tableColumn: PropertyClusterTableColumn) : Integer =  {

    var values_tableColumn = tableColumn.members.flatMap(_.property_values)

    var max_similarity = -1.0
    var fittest_cluster = -1
    var intercluster_similarity = -1.0
    
    for (c <- clusters) {
      if ( !c.index.equals(tableColumn.cluster_id)) {
        var values_cluster = c.tableColumns.flatMap(_.members.flatMap(_.property_values))
        //TODO soft as double and not integer. It has to be proportionate to the average size of the values
        var similarity = 1 - new SoftJaccardDistance(3).apply(values_tableColumn, values_cluster)

        if (similarity> max_similarity) {
          max_similarity = similarity
          fittest_cluster = c.index.toInt
        }
      }
      else {
        var values_same_cluster = c.tableColumns.filterNot(_.equals(tableColumn))flatMap(_.members.flatMap(_.property_values))
        intercluster_similarity = 1 - new SoftJaccardDistance(3).apply(values_tableColumn, values_same_cluster)
      }
    }
    fittest_cluster
   }
   
  /**
 * @param tableColumn
 * @return
 * Choose a random cluster to recluster the current table column
 */
def findRandomCluster(tableColumn: PropertyClusterTableColumn) : Integer =  {
       var random_id = scala.util.Random.nextInt(clusters.size-1) //index starts from 0
       //new cluster id should not be equal with the current cluster id
       while (random_id.equals(tableColumn.cluster_id.toInt)) random_id = scala.util.Random.nextInt(clusters.size-1)
       
       random_id
  }
  /**
 * @param tableColumn
 * @return
 * current reclustering condition if there at least 10% with at least two raised doubts
 */
def doubtCondition (tableColumn: PropertyClusterTableColumn) :Boolean = { 
  
    var minDoubts = 0.1*(tableColumn.members.size)
    tableColumn.members.filter(_.schema_errors.doubts>2).size > minDoubts
    
   }
   
   def updateEntitiesWithSchemaInfo(refEntities: ReferenceEntities) : ReferenceEntities = {

    var entity_members = clusters.flatMap(_.tableColumns).flatMap(_.members).groupBy(m => m.entity_uri)
    
    for (e <- refEntities.positive ++ refEntities.negative){
      
      var entity = e._2.source

      var membersofEntity = entity_members.get(entity.uri).get
      //initialize
      entity.valuesOfColumns = Some({
        for (i <- 0 to entity.values.length-1) yield {
          
          var membersOfthisPath = membersofEntity.filter(_.cluster_id.toInt == i)
          var valuesWithSchemaInfo  = Set[(String, Integer)]()
          for (m <- membersOfthisPath) {
            for (v <- m.property_values)
            valuesWithSchemaInfo += ((v, m.table_column_id))
          }
          
          valuesWithSchemaInfo
        }
      }.toIndexedSeq)
      
      
    }
    
    refEntities
   }
}


object PropertyClusters {
      
    private val schemaLocation = "de/fuberlin/wiwiss/silk/PropertyClustersStructure.xsd"

    def load (file: File): PropertyClusters = {
      
        // check if the property cluster file conforms with the required structure
        new ValidatingXMLReader(node => XML.loadFile(file), schemaLocation).apply(file)
        // parse and feed property cluster data model
        new PropertyClusters (clusters = readClusters(XML.loadFile(file)).toSet)
    }
     
   private def readClusters(xml: Node): Traversable[PropertyCluster] = {
   
   //get the paths of the properties
    for (cluster <- xml \ "cluster"  ) yield {
      
        val cluster_id = cluster \ "index" text
        
        val tables = for (t <- cluster \ "table_column") yield {
          var table_col_id = (t \ "table_column_id" text).toInt
          var correct_label = t \ "correct_predicate" text
          
          val members = for(m <- t \ "members" \ "member") yield {
          new PropertyClusterMember (
              
            cluster_id = cluster_id,
            entity_uri = m \ "entity_uri" text,
            property_name = m \ "property_name" text,
            table_id = t \ "table_id" text,
            property_values =  (for(v <- m \ "property_values" \ "Val" ) yield v \ "e" text).toSet ,
            table_column_id = table_col_id,
            correct_predicate = correct_label,
            schema_errors = new SchemaErrors(doubts = 0)
            )
          
        }
          new PropertyClusterTableColumn (
            cluster_id = cluster_id,
            table_id = t \ "table_id" text,
            table_column_id =table_col_id,
            correct_label = correct_label,
            members = members
          )
          
        }
        
        

      new PropertyCluster(
        index = cluster_id,
        label = cluster \ "label" text,
        tableColumns = tables)

    }
  }
 
   
  }

case class PropertyCluster (index: String = "",
                            label: String = "",
                            var tableColumns: Traversable[PropertyClusterTableColumn]) {
  
}

case class PropertyClusterTableColumn(var cluster_id:String="", table_id:String= "", table_column_id:Integer, correct_label:String, var members: Traversable[PropertyClusterMember] ) {
  
}
                            
case class PropertyClusterMember(var cluster_id: String="", entity_uri: String="", var property_name:String ="", var correct_predicate:String ="", var table_id:String = "" , var table_column_id:Integer=0, var property_values:Set[String]= Set[String](), schema_errors:SchemaErrors =  SchemaErrors())

case class SchemaErrors (var doubts: Int=0) {
  
 
}

