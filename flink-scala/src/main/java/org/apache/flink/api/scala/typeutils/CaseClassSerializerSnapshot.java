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

package org.apache.flink.api.scala.typeutils;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link TypeSerializerSnapshot} for {@link SpecificCaseClassSerializer}.
 */
public final class CaseClassSerializerSnapshot<T extends scala.Product>
	extends CompositeTypeSerializerSnapshot<T, SpecificCaseClassSerializer<T>> {

	private static final int VERSION = 2;

	private Class<T> type;

	public CaseClassSerializerSnapshot() {
		super(correspondingSerializerClass());
	}

	public CaseClassSerializerSnapshot(SpecificCaseClassSerializer<T> serializerInstance) {
		super(serializerInstance);
		this.type = serializerInstance.getTupleClass();
	}

	// for legacy
	public CaseClassSerializerSnapshot(Class<T> type, TypeSerializer<Object>[] fieldSerializers) {
		super(new SpecificCaseClassSerializer<>(type, fieldSerializers));
		this.type = type;
	}

	@Override
	protected int getCurrentOuterSnapshotVersion() {
		return VERSION;
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(SpecificCaseClassSerializer<T> outerSerializer) {
		return outerSerializer.getFieldSerializers();
	}

	@Override
	protected SpecificCaseClassSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		return new SpecificCaseClassSerializer<>(type, nestedSerializers);
	}

	@Override
	protected void writeOuterSnapshot(DataOutputView out) throws IOException {
		out.writeUTF(type.getName());
	}

	@Override
	protected void readOuterSnapshot(int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		final String className = in.readUTF();
		try {
			@SuppressWarnings("unchecked")
			Class<T> typeClass = (Class<T>) Class.forName(className, false, userCodeClassLoader);
			this.type = typeClass;
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("Can not find the case class '" + type + "'", e);
		}
	}

	@Override
	protected boolean isOuterSnapshotCompatible(SpecificCaseClassSerializer<T> newSerializer) {
		// TODO: think this through?
		return Objects.equals(type, newSerializer.getTupleClass());
	}

	@SuppressWarnings("unchecked")
	private static <T extends scala.Product> Class<SpecificCaseClassSerializer<T>> correspondingSerializerClass() {
		return (Class<SpecificCaseClassSerializer<T>>) (Class<?>) SpecificCaseClassSerializer.class;
	}
}
