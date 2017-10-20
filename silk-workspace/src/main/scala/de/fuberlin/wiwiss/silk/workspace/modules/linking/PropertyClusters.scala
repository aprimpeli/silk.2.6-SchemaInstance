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
         if (m.entity_uri==entity_uri && m.property_name==property_name) {
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
            m.schema_errors.doubts=0
          }
         c.tableColumns = c.tableColumns ++ Seq(PropertyClusterTableColumn(cluster_id, tableColumn.table_id, tableColumn.members))
        }
     }
   }
   
   def recluster(tableColumn:PropertyClusterTableColumn,  new_cluster_id:String) = {
     
     println("Cluster Member:"+tableColumn+" is moved to cluster with id "+new_cluster_id)
     //delete from initial locations
     deleteMember (tableColumn)
     
     //add to new clusters
     addMember(tableColumn, new_cluster_id)
   }
   

   def updateSchemaInfo (rule: LinkageRule, training_result: EvaluationResult )  = {
     
     rule.getComparisonOperators()
     
     //we believe that falsePositives cannot give any information on schema reclustering
      for (e <- training_result.falseNegativeEntities.get ++ training_result.truePositiveEntities.get) {
       var source_entity = e.source.uri
       
       var possibly_wrong_properties = Set[PathOperator]()
       var source_properties = e.source.desc.paths.flatMap(_.operators)
        
       //exploit the information from the properties belonging to the linkage rule
       var comparatorsInformation = getComparatorsInfo(e, rule, training_result)
       possibly_wrong_properties = comparatorsInformation._1
       var checked_properties = comparatorsInformation._2
             
       //now exploit the rest of the properties for the entities that are annotated as positive matches
       println("Size of paths:"+e.source.desc.paths.size)
//       for  (p <- e.source.desc.paths){
//         println("Current path:"+p.toString())
//         //TODO
//         if (!checked_properties.contains(p.operators.head)){
//           var propertyIndex = p.hashCode();
//           var sourceValues = e.source.values.indexOf(propertyIndex)
//           var targetValues = e.target.values.indexOf(propertyIndex)
//           //compare the source and the target values based on their datatype
//           println("compare source values"+sourceValues)
//           println("with target values"+targetValues)
//           
//         }        
//       }
       //we should not only consider the properties of the operators BUT all the properties
       for (wp <- possibly_wrong_properties) {
                
         //normalize the property name as it is in the form of path
         val PropertyRegex = """.*<([^>]*)>""".r
         var PropertyRegex(property_name) = wp.toString()
         
         var candidate_cluster_members = locateMember(source_entity, property_name)
         
         for (c <- candidate_cluster_members) {
           if (!c.cluster_id.equals("")) {
              c.schema_errors.doubts += 1
            
           }
         }
         
       }
     }

   }
   
   def getComparatorsInfo (e:DPair[Entity] , rule: LinkageRule, training_result: EvaluationResult) : (Set[PathOperator],Set[PathOperator])= {
     
     var possibly_wrong_properties = Set[PathOperator]()
     var checked_properties = Set[PathOperator]()
     
     for (operator <- rule.comparison_operators) {
         //TODO check if this the real threshold
         var result = operator.apply(e, operator.threshold)
         var property = operator.inputs.source
         checked_properties = checked_properties ++ (property.getPropertyPaths()) //keep track of the properties that are not part of the linkage rule
         //if the result for this specific operator is different than it should be
         //heuristic consider the false negatives for schema reclustering if the rule is good (training result is high)
         if (training_result.fMeasure>0.5){
           if (result.isDefined && ((result.get>0.0 && training_result.falsePositiveEntities.get.contains(e)) 
             || (result.get<0.0 && training_result.falseNegativeEntities.get.contains(e)))) {       
            possibly_wrong_properties = possibly_wrong_properties ++ (property.getPropertyPaths())    
         }
         }
         
         
         //raise a doubt if there are true positives but the source cluster id and the target property do not match
         //heuristic consider the true positives for schema reclustering if the rule is bad (training result is low)
         if (training_result.fMeasure<0.5){
           if (result.isDefined && ((result.get>0.0 && training_result.truePositiveEntities.get.contains(e))) ){     
             if (operator.inputs.source.getPropertyPaths().size>1 || operator.inputs.target.getPropertyPaths().size>1)
               println("This should not happen. CHECK!")
             if (!operator.inputs.source.getPropertyPaths().head.equals(operator.inputs.target.getPropertyPaths().head))
               possibly_wrong_properties = possibly_wrong_properties ++ (property.getPropertyPaths())
           }
         }  
       }
     (possibly_wrong_properties, checked_properties)
   }
   
   def evaluateReclusteringCondition () : Set [(PropertyClusterMember, String, String)] = {
     
     var feedback = Set [(PropertyClusterMember, String, String)]()
     //TODO change reclustering condition
     //filter the table for which one doubt is raised
     var toBeReclustered = clusters.flatMap(_.tableColumns).filter(doubtCondition(_)==true).toSet
     
     //recluster per table
     for ( tableColumn <- toBeReclustered) {       
       //TODO replace the random reclustering
       var random_id = scala.util.Random.nextInt(clusters.size-1) //index starts from 0
       //new cluster id should not be equal with the current cluster id
       while (random_id.equals(tableColumn.cluster_id.toInt)) random_id = scala.util.Random.nextInt(clusters.size-1)
       var new_random_cluster = random_id 

       recluster(tableColumn, new_random_cluster.toString())  
       for (m <- tableColumn.members)
         feedback += ((m,tableColumn.cluster_id, new_random_cluster.toString()))
       
     }
     feedback
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
          
          
          val members = for(m <- t \ "members" \ "member") yield {
          new PropertyClusterMember (
              
            cluster_id = cluster_id,
            entity_uri = m \ "entity_uri" text,
            property_name = m \ "property_name" text,
            table_id = t \ "table_id" text,
            property_values =  (for(v <- m \ "property_values" \ "Val" ) yield v \ "e" text).toSet ,
            schema_errors = new SchemaErrors(doubts = 0)
            )
          
        }
          new PropertyClusterTableColumn (
            cluster_id = cluster_id,
            table_id = t \ "table_id" text,
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

case class PropertyClusterTableColumn(var cluster_id:String="", table_id:String="", var members: Traversable[PropertyClusterMember] ) {
  
}
                            
case class PropertyClusterMember(var cluster_id: String="", entity_uri: String="", property_name:String ="", var table_id:String="", var property_values:Set[String]= Set[String](), schema_errors:SchemaErrors =  SchemaErrors())

case class SchemaErrors (var doubts: Int=0) {
  
 
}

