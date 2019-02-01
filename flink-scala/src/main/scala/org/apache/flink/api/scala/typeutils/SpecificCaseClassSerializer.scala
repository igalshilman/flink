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

import java.io.ObjectInputStream
import java.lang.invoke.{MethodHandle, MethodHandles}

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot.SelfMigrating
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerConfigSnapshot
import org.apache.flink.api.scala.typeutils.SpecificCaseClassSerializer.lookupConstructor

import scala.collection.JavaConverters._

object SpecificCaseClassSerializer {

  def lookupConstructor(clazz: Class[_]): MethodHandle = {
    val constructor = clazz.getDeclaredConstructors()(0)
    MethodHandles
      .publicLookup()
      .unreflectConstructor(constructor)
      .asSpreader(classOf[Array[AnyRef]], constructor.getParameterCount)
  }
}

class SpecificCaseClassSerializer[T <: Product](
  clazz: Class[T],
  scalaFieldSerializers: Array[TypeSerializer[_]]
) extends CaseClassSerializer[T](clazz, scalaFieldSerializers)
    with SelfMigrating[T] {

  @transient
  private var constructor = lookupConstructor(clazz)

  override def createInstance(fields: Array[AnyRef]): T = {
    constructor.invoke(fields).asInstanceOf[T]
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] = {
    new SpecificCaseClassSerializerSnapshot[T](this)
  }

  override def resolveSchemaCompatibilityViaRedirectingToNewSnapshotClass(
    s: TypeSerializerConfigSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] = {

    require(s.isInstanceOf[TupleSerializerConfigSnapshot[_]])

    val configSnapshot = s.asInstanceOf[TupleSerializerConfigSnapshot[T]]
    val nestedSnapshots = configSnapshot.getNestedSerializersAndConfigs.asScala
      .map(t => t.f1)
      .toArray

    val newCompositeSnapshot =
      new SpecificCaseClassSerializerSnapshot[T](configSnapshot.getTupleClass)

    delegateCompatibilityCheckToNewSnapshot(
      this,
      newCompositeSnapshot,
      nestedSnapshots: _*
    )
  }

  private def readObject(in: ObjectInputStream): Unit = {
    // this should be removed once we make sure that serializer are no long java serialized.
    in.defaultReadObject()
    constructor = lookupConstructor(clazz)
  }

}
