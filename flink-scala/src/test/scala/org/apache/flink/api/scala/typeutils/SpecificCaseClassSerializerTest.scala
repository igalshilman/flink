/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.typeutils.SerializerTestBase
import org.apache.flink.api.common.typeutils.base.{IntSerializer, StringSerializer}
import org.apache.flink.api.scala.typeutils.SpecificCaseClassSerializerReflectionTest.SimpleCaseClass

/**
  * Test [[SpecificCaseClassSerializer]].
  */
class SpecificCaseClassSerializerTest
    extends SerializerTestBase[SimpleCaseClass] {

  override protected def createSerializer() =
    new SpecificCaseClassSerializer[SimpleCaseClass](
      classOf[SimpleCaseClass],
      Array(StringSerializer.INSTANCE, IntSerializer.INSTANCE)
    )

  override protected def getLength = -1

  override protected def getTypeClass = classOf[SimpleCaseClass]

  override protected def getTestData = Array(
    SimpleCaseClass("a", 1),
    SimpleCaseClass("b", -1),
    SimpleCaseClass("c", 5)
  )
}

object SpecificCaseClassSerializerTest {

  case class SimpleCaseClass(name: String, var age: Int) {

    def this(name: String) = this(name, 0)

  }

}
