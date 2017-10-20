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
import scala.collection.mutable.HashMap
import de.fuberlin.wiwiss.silk.entity.Entity
import de.fuberlin.wiwiss.silk.evaluation.{LinkageRuleEvaluator, ReferenceEntities}


object EntitiesIdentifier {
  
  def main(args: Array[String]) {
        
    Plugins.register()
    JenaPlugins.register()
  
    
    val datasets = Dataset.fromWorkspace
    val cache = datasets.head.task.cache
    cache.waitUntilLoaded()
    val referenceEntities = ReferenceEntities.fromEntities(cache.entities.positive.values,cache.entities.negative.values)
    
    var entities = new HashMap[String, Entity]()
    
    for (r <- referenceEntities.positive.values++referenceEntities.negative.values) {
        if (r.source.uri.equals("http://example/e1") ) entities.+=("http://example/e1" -> r.source)
        if (r.target.uri.equals("http://catalog/c1") ) entities.+=("http://catalog/c1" -> r.target)
    }
    
    println("Unique entities size:"+entities.size)
    println(entities)
    
    } 
}

