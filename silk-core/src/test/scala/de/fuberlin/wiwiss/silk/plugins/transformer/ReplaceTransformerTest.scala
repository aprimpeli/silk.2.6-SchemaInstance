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

package de.fuberlin.wiwiss.silk.plugins.transformer

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FlatSpec
import de.fuberlin.wiwiss.silk.plugins.Plugins
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import de.fuberlin.wiwiss.silk.plugins.transformer.replace.ReplaceTransformer

@RunWith(classOf[JUnitRunner])
class ReplaceTransformerTest extends FlatSpec with ShouldMatchers {
  Plugins.register()

  val transformer = new ReplaceTransformer(search = " ", replace = "")

  "ReplaceTransformer" should "return 'abc'" in {
    transformer.evaluate("a b c") should equal("abc")
  }

  val transformer1 = new ReplaceTransformer(search = "abc", replace = "")

  "ReplaceTransformer" should "return 'def'" in {
    transformer1.evaluate("abcdef") should equal("def")
  }
}