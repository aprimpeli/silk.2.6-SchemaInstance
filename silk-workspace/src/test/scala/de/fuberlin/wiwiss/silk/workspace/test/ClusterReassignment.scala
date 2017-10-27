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

package de.fuberlin.wiwiss.silk.workspace.test

import de.fuberlin.wiwiss.silk.workspace.scripts.Dataset
import de.fuberlin.wiwiss.silk.plugins.Plugins
import de.fuberlin.wiwiss.silk.plugins.jena.JenaPlugins
import de.fuberlin.wiwiss.silk.workspace.modules.linking.PropertyClusters
import de.fuberlin.wiwiss.silk.entity.Entity
import de.fuberlin.wiwiss.silk.entity.Link
import de.fuberlin.wiwiss.silk.util.DPair
import scala.collection.mutable.HashSet
import java.io.File


object ClusterReassignemnt {
  
  def main(args: Array[String]) {
        
    Plugins.register()
    JenaPlugins.register()
   
    
    val datasets = Dataset.fromWorkspace
    
     //create property clusters file and objects
    var clustersFile = new File ("C:/Users/User/.silk/workspace/phones/linking/phones/propertyClusters.xml")
    datasets.head.task.propertyClusters = Some(PropertyClusters.load(clustersFile)) 
    
    var clusters = datasets.head.task.propertyClusters.get.clusters

    var tableColumns = clusters.flatMap(_.tableColumns)
    for (t <- tableColumns) {
        datasets.head.task.propertyClusters.get.findAppropriateCluster(t)
    }
   
  }
}

