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

package de.fuberlin.wiwiss.silk.workspace.scripts

import de.fuberlin.wiwiss.silk.runtime.task.Task
import de.fuberlin.wiwiss.silk.evaluation.ReferenceEntities
import java.util.logging.Level

import de.fuberlin.wiwiss.silk.entity.Path

import scala.util.Random
import de.fuberlin.wiwiss.silk.learning._
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.workspace.scripts.RunResult.Run

object CrossValidation extends EvaluationScript {

  override protected def run() {
    runExperiment()
    println("Evaluation finished")
  }

  protected def runExperiment() {
    val experiment = Experiment.default
    val datasets = Dataset.fromWorkspace
    
    val values =
      for(ds <- datasets) yield {
        for(config <- experiment.configurations) yield {
          execute(ds, config)
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
    println(result.transpose.toLatex)
  }
  
  private def execute(dataset: Dataset, config: LearningConfiguration): RunResult = {
    log.info("Running: " + dataset.name)
    val cache = dataset.task.cache
    cache.waitUntilLoaded()
    println("RULES:"+Seq(dataset.task.linkSpec.rule).toString())
    println("LOADED:"+cache.entities.positive.size+"--"+cache.entities.negative.size)
    val task = new CrossValidation(cache.entities, config, Seq(dataset.task.linkSpec.rule))
    task()
  }
}

/**
 * Performs multiple cross validation runs and outputs the statistics.
 */
class CrossValidation(entities : ReferenceEntities, config: LearningConfiguration, seed : Traversable[LinkageRule]) extends Task[RunResult] {
  require(entities.isDefined, "Reference Entities are required")
  
  /** The number of cross validation runs. */
  private val numRuns = 5

  /** The number of splits used for cross-validation. */
  private val numFolds = 4

  /** Don't log progress. */
  progressLogLevel = Level.FINE

  /**
   * Executes all cross validation runs.
   */
  override def execute() = {
    //Execute the cross validation runs
    val results = for(run <- 0 until numRuns; result <- crossValidation(run)) yield result
    //Make sure that all runs have the same number of results
    val paddedResults = results.map(r => r.padTo(config.params.maxIterations + 1, r.last))

    //Print aggregated results
    val aggregatedResults = for((iterationResults, i) <- paddedResults.transpose.zipWithIndex) yield AggregatedLearningResult(iterationResults, i)

    //println(AggregatedLearningResult.format(aggregatedResults, includeStandardDeviation = true, includeComplexity = false).toLatex)
    println()
    println(AggregatedLearningResult.format(aggregatedResults, includeStandardDeviation = false, includeComplexity = false).toCsv)

    RunResult(paddedResults.map(Run(_)))
  }

  /**
   * Executes one cross validation run.
   */
  private def crossValidation(run: Int): Seq[Seq[LearningResult]] = {
    logger.info("Cross validation run " + run)
    
    val splits = splitReferenceEntities()


    for((split, index) <- splits.zipWithIndex) yield {


      val learningTask = new LearningTask(split, config)

      var results = List[LearningResult]()
      val addResult = (result: LearningResult) => {
        println("Iteration:"+result.iterations)
        if (result.iterations > results.view.map(_.iterations).headOption.getOrElse(0))
          results ::= result
      }
      learningTask.value.onUpdate(addResult)

      executeSubTask(learningTask, (run.toDouble + index.toDouble / splits.size) / numRuns)

      //Add the learning result to the list
      results = learningTask.value.get :: results.tail

      results.reverse
    }
  }

  /**
   * Splits the reference entities..
   */
  private def splitReferenceEntities(): IndexedSeq[LearningInput] = {
    //Get the positive and negative reference entities
    val posEntities = Random.shuffle(entities.positive.values)
    val negEntities = Random.shuffle(entities.negative.values)

    //Split the reference entities into numFolds samples
    val posSamples = posEntities.grouped((posEntities.size.toDouble / numFolds).ceil.toInt).toStream
    val negSamples = negEntities.grouped((negEntities.size.toDouble / numFolds).ceil.toInt).toStream

    //Generate numFold splits
    val posSplits = (posSamples ++ posSamples).sliding(posSamples.size+1).take(posSamples.size)
    val negSplits = (negSamples ++ negSamples).sliding(negSamples.size+1).take(negSamples.size)

    //Generate a learning set from each split
    val splits =
      for((p, n) <- posSplits zip negSplits) yield {
        LearningInput(
          seedLinkageRules = Seq.empty,
          trainingEntities = ReferenceEntities.fromEntities(p.tail.flatten, n.tail.flatten),
          validationEntities = ReferenceEntities.fromEntities(p.head, n.head)
        )
      }

    splits.toIndexedSeq
  }


  /**
    * Splits 2/3 1/3 the reference entities.
    */
  private def split7030ReferenceEntities(): IndexedSeq[LearningInput] = {
    //Get the positive and negative reference entities
    val posEntities = Random.shuffle(entities.positive.values)
    val negEntities = Random.shuffle(entities.negative.values)

    //Split the reference entities into numFolds samples
    val posSamples = posEntities.grouped((posEntities.size.toDouble / 3).ceil.toInt).toStream
    val negSamples = negEntities.grouped((negEntities.size.toDouble / 3).ceil.toInt).toStream

    //Generate numFold splits
    //val posSplits = (posSamples ++ posSamples).sliding(posSamples.size+1).take(posSamples.size)
    //val negSplits = (negSamples ++ negSamples).sliding(negSamples.size+1).take(negSamples.size)

    //Generate a learning set from each split
    val split = LearningInput(
          seedLinkageRules = Seq.empty, //seed //for seeding
          trainingEntities = ReferenceEntities.fromEntities(posSamples.take(posSamples.size*2/3).flatten, negSamples.take(posSamples.size*2/3).flatten),
          validationEntities = ReferenceEntities.fromEntities(posSamples.drop(posSamples.size*2/3).flatten, negSamples.drop(posSamples.size*2/3).flatten)
        )

    (split :: Nil).toIndexedSeq
  }


}