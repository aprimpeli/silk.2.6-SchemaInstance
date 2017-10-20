package de.fuberlin.wiwiss.silk.workspace.scripts

import de.fuberlin.wiwiss.silk.runtime.task.Task
import de.fuberlin.wiwiss.silk.learning.individual.Population
import de.fuberlin.wiwiss.silk.learning.active.ActiveLearningTask
import de.fuberlin.wiwiss.silk.evaluation.{LinkageRuleEvaluator, ReferenceEntities}
import de.fuberlin.wiwiss.silk.workspace.scripts.RunResult.Run
import de.fuberlin.wiwiss.silk.learning.{LearningResult, LearningConfiguration}
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.linkagerule.similarity.Comparison
import de.fuberlin.wiwiss.silk.entity.Link
import de.fuberlin.wiwiss.silk.util.DPair
import collection.mutable.ListBuffer
import util.Random
import java.util.Map.Entry
import de.fuberlin.wiwiss.silk.linkagerule.similarity.SimilarityOperator
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import java.io.File
import de.fuberlin.wiwiss.silk.workspace.io.PropertyClusterImporter
import de.fuberlin.wiwiss.silk.workspace.modules.linking.{PropertyClusters, SchemaAnalyzer}
import de.fuberlin.wiwiss.silk.workspace.User
import de.fuberlin.wiwiss.silk.util.XMLUtils

object ActiveLearningEvaluation extends EvaluationScript {

  override protected def run() {
    val experiment = Experiment.representations
    val datasets = Dataset.fromWorkspace

    val values =
      for(dataset <- datasets) yield {
        for(config <- experiment.configurations) yield {
          execute(config, dataset)
        }
        
      }

    val result =
      MultipleTables.build(
        name = experiment.name,
        metrics = experiment.metrics,
        header = experiment.configurations.map(_.name),
        rowLabels = datasets.map(_.name),
        values = values
      )

     println(result.toLatex)
     println(result.toCsv)
  }

  private def execute(config: LearningConfiguration, dataset: Dataset): RunResult = {
    val cache = dataset.task.cache
    cache.waitUntilLoaded()
    val task = new ActiveLearningEvaluator(config, dataset)
    task()
  }
}

class ActiveLearningEvaluator(config: LearningConfiguration,
                               ds: Dataset) extends Task[RunResult] {

  val numRuns = 1

  val maxLinks =40

  val maxPosRefLinks = 80

  val maxNegRefLinks = 800
    
  val schemaMatching = true
  
  
  def createPropertyClusters(){
    
    //create property clusters file and objects
    var clustersFile = new File ("C:/Users/User/.silk/workspace/"+ds.name+"/linking/"+ds.name+"/propertyClusters.xml")
    var sourceFile = new File ("C:/Users/User/.silk/workspace/"+ds.name+"/resources/source")
    var property_index = ds.task.cache.referenceEntitiesCache.value.negative.head._2.source.desc.paths
    var loadedClusters = PropertyClusterImporter.loadInfoForXML(sourceFile, property_index)
    PropertyClusterImporter.writeXML(loadedClusters, clustersFile)
    ds.task.propertyClusters = Some(PropertyClusters.load(clustersFile)) 
    
  }
  
//  val numberOfSeedingPositiveLinks = 0
//  val numberOfSeedingNegativeLinks = 0
  
 protected override def execute() = {
    //Execute the active learning runs
    println("Cross Validation")
    
    if (schemaMatching) createPropertyClusters()

    val results = for(run <- 1 to numRuns) yield runActiveLearning(run)

    //Print aggregated results
    val aggregatedResults = for((iterationResults, i) <- results.transpose.zipWithIndex) yield AggregatedLearningResult(iterationResults, i)

    println("Results for experiment " + config.name + " on data set " + ds.name)
    println(AggregatedLearningResult.format(aggregatedResults, includeStandardDeviation = true, includeComplexity = false).toLatex)
    println()
    println(AggregatedLearningResult.format(aggregatedResults, includeStandardDeviation = false, includeComplexity = false).toCsv)

    RunResult(results.map(Run(_)))
  }

  private def runActiveLearning(run: Int): Seq[LearningResult] = {
    logger.info("Experiment " + config.name + " on data set " + ds.name +  ": run " + run )

    var propertyClusters = ds.task.propertyClusters
    
    var referenceEntities = ReferenceEntities()
    var entities = ds.task.cache.entities
    var entity_source_desc = entities.negative.head._2.source.desc
    var entity_target_desc = entities.negative.head._2.target.desc
    //initialize the schema analyzer
    var schemaAnalyzer = SchemaAnalyzer(entity_source_desc, entity_target_desc)
   
    val posEntities = Random.shuffle(entities.positive.values)
    val negEntities = Random.shuffle(entities.negative.values)
    
    //pool entities
    val posPoolEntities = posEntities.take(maxPosRefLinks)
    val negPoolEntities = negEntities.take(maxNegRefLinks)
    val poolEntities = ReferenceEntities.fromEntities(posPoolEntities, negPoolEntities)
    //seeding entities
   // referenceEntities = ReferenceEntities.fromEntities(posPoolEntities.take(numberOfSeedingPositiveLinks) , negPoolEntities.take(numberOfSeedingNegativeLinks))
    
    
    //validation entities
    val posValEntities = posEntities.drop(maxPosRefLinks)
    val negValEntities = negEntities.drop(maxNegRefLinks)
    var valEntities = ReferenceEntities.fromEntities(posValEntities, negValEntities)
    

    //construct the links of the pool
    var positivePoolLinks = for((link, entityPair) <- poolEntities.positive) yield link.update(entities = Some(entityPair))
    
    var negativePoolLinks = for((link, entityPair) <- poolEntities.negative) yield link.update(entities = Some(entityPair))

    
    var pool: Traversable[Link] = positivePoolLinks ++ negativePoolLinks
    //part of the active learning task already
    pool = pool.filterNot(referenceEntities.positive.contains).filterNot(referenceEntities.negative.contains)
    
    var population = Population.empty
    val startTime = System.currentTimeMillis()

    //Holds the validation result from each iteration
    var learningResults = List[LearningResult]()

    
    var matrix_entries =ListBuffer[(Link,LinkageRule, Option[Double])]()
    //-numberOfSeedingPositiveLinks-numberOfSeedingNegativeLinks         
    for(i <- 1 to maxLinks) yield {

      //println("Size of reference entities:"+referenceEntities.negative.size+"---"+referenceEntities.positive.size)
     
      val task =
        new ActiveLearningTask(
          config = config,
          sources = ds.sources,
          linkSpec = ds.task.linkSpec,
          paths = ds.task.cache.entityDescs.map(_.paths),
          referenceEntities = referenceEntities,
          pool = pool,
          population = population
        )

      task()

      pool = task.pool
      population = task.population

      //Evaluate performance of learned linkage rule
      val rule = population.bestIndividual.node.build
      println(rule);
      
      val trainScores = LinkageRuleEvaluator(rule, referenceEntities)
      // eliminate the validation entities by the entities of the pool
      
      val valScores = LinkageRuleEvaluator(rule, valEntities)
      val learningResult =
        LearningResult(
          iterations = i,
          time = System.currentTimeMillis() - startTime,
          population = population,
          trainingResult =  trainScores,
          validationResult = valScores,
          status = LearningResult.NotStarted
        )

//      println("False negatives in the training:"+ learningResult.trainingResult.falseNegativeEntities)
//      println("False positives in the training:"+ learningResult.trainingResult.falsePositiveEntities)
//
//      println("Best Rule: "+rule.toString())

      println(i + " - TRAIN - "+ referenceEntities.negative.size +"/"+ referenceEntities.positive.size+"-" + trainScores)
      println(i + " - TEST -"+ valEntities.negative.size +"/"+ valEntities.positive.size+"-" + valScores)
      learningResults ::= learningResult
      
      if (schemaMatching && enoughConfidence(i)) {
        //update schema information
        propertyClusters.get.updateSchemaInfo(rule, learningResult.trainingResult, schemaAnalyzer)
        //evaluate reclustering condition and recluster the member of which the condition is met
        var reclusturedMembers = propertyClusters.get.evaluateReclusteringCondition()
        //consider the changes in the clusters and update the schema information of the current entities (reference, validation, pool)
        for (rc <- reclusturedMembers) {
        
            entities.negative.values.filter(_.source.uri.equals(rc._1.entity_uri)).foreach(p => p.source.update(rc._2.toInt, rc._1.property_values, rc._3.toInt))
            entities.positive.values.filter(_.source.uri.equals(rc._1.entity_uri)).foreach(p => p.source.update(rc._2.toInt, rc._1.property_values, rc._3.toInt))
         
        }
        
      }

//      if(valScores.fMeasure > 0.999) {
//        return learningResults.reverse
//      }

      //Evaluate new lin
      val link = task.links.head
      if(poolEntities.positive.contains(link)) {
//        println(link + " added to positive")
        referenceEntities = referenceEntities.withPositive(link.entities.get)
      }
      else {
//        println(link + " added to negative")
        referenceEntities = referenceEntities.withNegative(link.entities.get)
      }
      
      
    }

   //TODO write new clusters file: spaghettii
    if (schemaMatching)
   PropertyClusterImporter.writeXML(propertyClusters.get.clusters, new File("C:/Users/User/.silk/workspace/"+ds.name+"/linking/"+ds.name+"/updatedClusters.xml"))
   learningResults.reverse
    
  }
  
  def enoughConfidence(iterations:Integer):Boolean ={
    //TODO change the confidence condition
    if (iterations > 5) true
    else false
  }
}