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



/**
 * @author Anna Primpeli
 *
 */
case class SchemaAnalyzer (var source_desc:EntityDescription, var target_desc:EntityDescription){
  
  val source_properties = source_desc.paths.flatMap(_.operators)
  val target_properties = target_desc.paths.flatMap(_.operators)   
  val sourceToTargetOperators = mapOperators()
  
  def analyzeSchemaOfEntityPair (rule: LinkageRule, training_result: EvaluationResult, e: DPair[Entity]) : Set[PathOperator] = {
           
       var source_entity = e.source.uri
       var possibly_wrong_properties = Set[PathOperator]()
              
       //exploit the information from the properties belonging to the linkage rule
       var comparatorsInfo = getComparatorsInfo(e, rule, training_result)
       possibly_wrong_properties = comparatorsInfo._1
       var checked_properties = comparatorsInfo._2
             
       //now exploit the rest of the properties for the entities that are annotated as positive matches
       var positiveMatchesInfo = getPositiveMatchesInfo(e, checked_properties)
       possibly_wrong_properties = possibly_wrong_properties ++ positiveMatchesInfo
       
      possibly_wrong_properties
  }
  
  def mapOperators() : Map[PathOperator,Integer] = {
     
     var mapping = Map[PathOperator, Integer]()
     
     for (s <- source_properties) {
         
         //should be calculated only once
         var targetCurrentProp = target_properties.filter(p => p.toString().equals(s.toString()))
         if (!targetCurrentProp.isEmpty) {          
           var targetIndex = target_properties.indexOf(targetCurrentProp.head) 
           mapping += s -> targetIndex
         }
       }
     mapping
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
  
  def getPositiveMatchesInfo (e: DPair[Entity], checked_properties:Set[PathOperator]) : Set[PathOperator] = {
     
     var possibly_wrong_props =  Set[PathOperator]()
     for  (sourceIndex <- 0 until source_properties.length)  {
       var currentProp = source_properties(sourceIndex)
       
       //should be calculated only once
       var targetIndex = sourceToTargetOperators.get(currentProp)
     
       //TODO targetIndex>0 this is because silk restrits to the top x dense properties. So a source path may not be contained in the catalog paths
       if (targetIndex.isDefined) {
         
         if (!checked_properties.contains(currentProp)){
           var sourceValues = e.source.values(sourceIndex)
           var targetValues = e.target.values(targetIndex.get)
           //compare only if both are non null
           if (!sourceValues.isEmpty && !targetValues.isEmpty){
             //TODO empty string fix
             if (sourceValues.head.length()>0 && targetValues.head.length()>0){
               var jaccardOnBigrams = new JaccardOnNGramsSimilarity(2);
               //TODO different for every datatype (also single values vs lists)
               var result = jaccardOnBigrams.calculate(sourceValues.head, targetValues.head)
               if (result<0.5) possibly_wrong_props += currentProp
             }
           }
           
         }        
       }
     }
     possibly_wrong_props
   }
}
