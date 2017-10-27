/* 
 * Copyright 2009-2011 Freie Universität Berlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.fuberlin.wiwiss.silk.workspace.modules.linking

import scala.collection.mutable.HashSet
import de.fuberlin.wiwiss.silk.evaluation.EvaluationResult

object SchemaClustersEvaluator {
  def apply(clusters: PropertyClusters): EvaluationResult = {
    var truePositives: Int = 0
    var trueNegatives: Int = 0
    var falsePositives: Int = 0
    var falseNegatives: Int = 0


    for (tb <- clusters.clusters.flatMap(_.tableColumns)) {
      val correct_label = tb.correct_label
      val clustered_in = clusters.clusters.filter(_.index.equals(tb.cluster_id)).map(_.label).toSet
     
      for (c <- clusters.clusters){
        if (correct_label.equals(c.label)){
          if (clustered_in.contains(c.label)) truePositives = truePositives+1
          else falseNegatives = falseNegatives +1
        }
        else {
          if (clustered_in.contains(c.label)) falsePositives = falsePositives+1
          else trueNegatives = trueNegatives+1
        }
      }
    }

    

   

    new EvaluationResult(truePositives, trueNegatives, falsePositives, falseNegatives)
  }
}