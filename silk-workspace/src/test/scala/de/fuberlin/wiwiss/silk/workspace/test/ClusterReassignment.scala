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
//import de.fuberlin.wiwiss.silk.workspace.modules.linking.PropertyClusters
//import de.fuberlin.wiwiss.silk.entity.Entity
//import de.fuberlin.wiwiss.silk.entity.Link
//import de.fuberlin.wiwiss.silk.util.DPair
//import scala.collection.mutable.HashSet
//
//object ClusterReassignemnt {
//  
//  def main(args: Array[String]) {
//        
//    Plugins.register()
//    JenaPlugins.register()
//   
//    
//    val datasets = Dataset.fromWorkspace
//    
//    
//    var clusters = datasets.head.task.propertyClusters.get.clusters
//    
//    println("INITIAL CLUSTER STATE")
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
//    
//    var propertyClusters = datasets.head.task.propertyClusters
//    //now recluster the http://example/e1 Home_Address to http://catalog/N (2) from any cluster that it was located before
//    //locate member
//    var members = propertyClusters.get.locateMember("http://example/e2", "Home_Address")
//    for (m <- members) {
//       //delete member from its current property cluster
//      propertyClusters.get.deleteMember(m)
//      //add it to the new clusters 
//      propertyClusters.get.addMember(m, "2")
//    }
//   
//    
//    //now recluster the http://example/e1 Home_Address to http://catalog/N (2) from any cluster that it was located before
//    //or you can do all the three methods in one with the below line
//    //propertyClusters.get.recluster("http://example/e2", "Home_Address", HashSet[String]("2"))
//    
//    println("AFTER RECLUSTERING")
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
//    
//    } 
//  //make method for additions deletions here
//}
//
