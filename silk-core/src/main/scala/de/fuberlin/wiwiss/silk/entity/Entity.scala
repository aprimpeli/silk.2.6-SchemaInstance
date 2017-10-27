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

package de.fuberlin.wiwiss.silk.entity

import xml.Node
import java.io.{DataOutput, DataInput}

/**
 * A single entity.
 */
class Entity(val uri: String, var values: IndexedSeq[(Set[String])] ,   val desc: EntityDescription, var valuesOfColumns:Option[IndexedSeq[(Set[(String, Integer)])]]=  Some(IndexedSeq[(Set[(String, Integer)])]()) ){
  
  
  def evaluate(path: Path): Set[String] = {
    if(path.operators.isEmpty)
      Set(uri)
    else
      evaluate(desc.pathIndex(path))
  }

  def evaluate(pathIndex: Int): Set[String] = values(pathIndex)

  override def toString = uri + "\n{\n  " + values.mkString("\n  ") + "\n}"

  def toXML = {
    <Entity uri={uri}> {
      for (valueSet <- values) yield {
        <Val> {
          for (value <- valueSet) yield {
            <e>{value}</e>
          }
        }
        </Val>
      }
    }
    </Entity>
  }

  def serialize(stream: DataOutput) {
    stream.writeUTF(uri)
    for (valueSet <- values) {
      stream.writeInt(valueSet.size)
      for (value <- valueSet) {
        stream.writeUTF(value)
      }
    }
  }
  
  def update(oldIndex: Int, valuesReassign: Set[String], newIndex: Int) = {
    
    var filtered = Set[String] ()
     
    for (v <- valuesReassign) if (values(oldIndex).contains(v)) filtered += v
    
    
    var newvalues= for (i<- 0 to values.size-1) yield {
      if(i == oldIndex){
         values(i).--(filtered)
      }
      else if (i == newIndex){
        values(i).++(filtered)
      }
      else values(i)
    }
        
    values = newvalues
    
  }
}

  

object Entity {
  def fromXML(node: Node, desc: EntityDescription) = {
    new Entity(
      uri = (node \ "@uri").text.trim,
      values = {
        for (valNode <- node \ "Val") yield {
          { for (e <- valNode \ "e") yield e.text }.toSet
        }
      }.toIndexedSeq,
      desc = desc
    )
  }

  def deserialize(stream: DataInput, desc: EntityDescription) = {
    //Read URI
    val uri = stream.readUTF()

    //Read Values
    def readValue = Traversable.fill(stream.readInt)(stream.readUTF).toSet
    val values = IndexedSeq.fill(desc.paths.size)(readValue)

    new Entity(uri, values, desc)
  }
  
  
}
