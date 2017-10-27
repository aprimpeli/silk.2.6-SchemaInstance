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
import java.io.File
import de.fuberlin.wiwiss.silk.workspace.io.PropertyClusterImporter
import de.fuberlin.wiwiss.silk.workspace.modules.linking.PropertyClusters

object ClusterLoader {
  
  def main(args: Array[String]) {
        
    Plugins.register()
    JenaPlugins.register()
  
    
    val datasets = Dataset.fromWorkspace
    val cache = datasets.head.task.cache
    cache.waitUntilLoaded()
    println("Datasets")
    
    var clustersFile = new File ("C:/Users/User/.silk/workspace/phones/linking/phones/propertyClusters.xml")
    var sourceFile = new File ("C:/Users/User/.silk/workspace/phones/resources/source_schema")
    var property_index = datasets.head.task.cache.referenceEntitiesCache.value.negative.head._2.source.desc.paths
    println("load clusters")
    var loadedClusters = PropertyClusterImporter.loadInfoForXML(sourceFile, property_index)
    println("write clusters")

    PropertyClusterImporter.writeXML(loadedClusters, clustersFile)
    datasets.head.task.propertyClusters = Some(PropertyClusters.load(clustersFile)) 

  }
}

