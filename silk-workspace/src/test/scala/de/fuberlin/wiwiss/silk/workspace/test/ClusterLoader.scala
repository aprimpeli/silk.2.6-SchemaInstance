///*
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *       http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package de.fuberlin.wiwiss.silk.workspace.test
//
//import de.fuberlin.wiwiss.silk.workspace.scripts.Dataset
//import de.fuberlin.wiwiss.silk.plugins.Plugins
//import de.fuberlin.wiwiss.silk.plugins.jena.JenaPlugins
//
//object ClusterLoader {
//  
//  def main(args: Array[String]) {
//        
//    Plugins.register()
//    JenaPlugins.register()
//  
//    
//    val datasets = Dataset.fromWorkspace
//    var clusters = datasets.head.task.propertyClusters.get.clusters
//    
//    for (c <- clusters) {
//      println("--- New Cluster ---")
//      println(c.index+" : "+c.label)
//      println("with members:")
//      for (m <- c.members){
//        println("Member name-values:"+m.property_name+"---"+m.property_values)
//        println("of entity:"+m.entity_uri)
//      }
//      
//    }
//    } 
//}
//
