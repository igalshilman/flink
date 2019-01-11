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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.ExecutionConfig.SerializableSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.esotericsoftware.kryo.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializerSnapshotData.createFrom;
import static org.apache.flink.api.java.typeutils.runtime.kryo.OptionalMap.optionalMapOf;

public class KryoSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

	private static final Logger LOG = LoggerFactory.getLogger(KryoSerializerSnapshot.class);

	private static final int VERSION = 2;

	private KryoSerializerSnapshotData<T> snapshotData;

	@SuppressWarnings("unused")
	public KryoSerializerSnapshot() {
	}

	KryoSerializerSnapshot(Class<T> typeClass,
						   LinkedHashMap<Class<?>, SerializableSerializer<?>> defaultKryoSerializers,
						   LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses,
						   LinkedHashMap<String, KryoRegistration> kryoRegistrations) {

		this.snapshotData = createFrom(typeClass, defaultKryoSerializers, defaultKryoSerializerClasses, kryoRegistrations);
	}

	@Override
	public int getCurrentVersion() {
		return VERSION;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		snapshotData.writeSnapshotData(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		this.snapshotData = createFrom(in, userCodeClassLoader);
	}

	@Override
	public TypeSerializer<T> restoreSerializer() {
		return new KryoSerializer<>(
			snapshotData.getTypeClass(),
			snapshotData.getDefaultKryoSerializers().unwrapOptionals(),
			snapshotData.getDefaultKryoSerializerClasses().unwrapOptionals(),
			snapshotData.getKryoRegistrations().unwrapOptionals());
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
		if (!(newSerializer instanceof KryoSerializer)) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
		KryoSerializer<T> kryoSerializer = (KryoSerializer<T>) newSerializer;
		if (kryoSerializer.getType() != snapshotData.getTypeClass()) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
		return resolveSchemaCompatibility(kryoSerializer);
	}

	private TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(KryoSerializer<T> newSerializer) {
		// merge the default serializers
		final MergeResult<Class<?>, SerializableSerializer<?>> reconfiguredDefaultKryoSerializers = MergeResult.compute(
			snapshotData.getDefaultKryoSerializers(),
			optionalMapOf(newSerializer.getDefaultKryoSerializers(), Class::getName));

		if (reconfiguredDefaultKryoSerializers.hasMissingKeys()) {
			reconfiguredDefaultKryoSerializers.logMissingKeys();
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		// merge default serializer classes
		final MergeResult<Class<?>, Class<? extends Serializer<?>>> reconfiguredDefaultKryoSerializerClasses = MergeResult.compute(
			snapshotData.getDefaultKryoSerializerClasses(),
			optionalMapOf(newSerializer.getDefaultKryoSerializerClasses(), Class::getName));

		if (reconfiguredDefaultKryoSerializerClasses.hasMissingKeys()) {
			reconfiguredDefaultKryoSerializerClasses.logMissingKeys();
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		// merge registration
		final MergeResult<String, KryoRegistration> reconfiguredRegistrations = MergeResult.compute(
			snapshotData.getKryoRegistrations(),
			optionalMapOf(newSerializer.getKryoRegistrations(), Function.identity()));

		if (reconfiguredRegistrations.hasMissingKeys()) {
			reconfiguredRegistrations.logMissingKeys();
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		// there are no missing keys, now we have to decide rather we are compatibly as-is or we require reconfiguration.
		return resolveSchemaCompatibility(
			reconfiguredDefaultKryoSerializers,
			reconfiguredDefaultKryoSerializerClasses,
			reconfiguredRegistrations);
	}

	private TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
		MergeResult<Class<?>, SerializableSerializer<?>> reconfiguredDefaultKryoSerializers,
		MergeResult<Class<?>, Class<? extends Serializer<?>>> reconfiguredDefaultKryoSerializerClasses,
		MergeResult<String, KryoRegistration> reconfiguredRegistrations) {

		if (reconfiguredDefaultKryoSerializers.isOrderedSubset() &&
			reconfiguredDefaultKryoSerializerClasses.isOrderedSubset() &&
			reconfiguredRegistrations.isOrderedSubset()) {

			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}

		// reconfigure a new KryoSerializer
		KryoSerializer<T> reconfiguredSerializer = new KryoSerializer<>(
			snapshotData.getTypeClass(),
			reconfiguredDefaultKryoSerializers.getMerged(),
			reconfiguredDefaultKryoSerializerClasses.getMerged(),
			reconfiguredRegistrations.getMerged());

		return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(reconfiguredSerializer);
	}

	private static final class MergeResult<K, V> {

		/**
		 * Tries to merges the keys and the values of @right into @left.
		 */
		static <K, V> MergeResult<K, V> compute(OptionalMap<K, V> left, OptionalMap<K, V> right) {
			OptionalMap<K, V> merged = new OptionalMap<>(left);
			merged.putAll(right);

			return new MergeResult<>(merged, isLeftPrefixOfRight(left, right));
		}

		private static <K, V> boolean isLeftPrefixOfRight(OptionalMap<K, V> left, OptionalMap<K, V> right) {
			Iterator<String> rightKeys = right.keyNames().iterator();

			for (String leftKey : left.keyNames()) {
				if (!rightKeys.hasNext()) {
					return false;
				}
				String rightKey = rightKeys.next();
				if (!leftKey.equals(rightKey)) {
					return false;
				}
			}
			return true;
		}

		// ----------------------------------------------------------------------------------------------------

		private final OptionalMap<K, V> merged;
		private final Set<String> missingKeys;
		private final boolean isOrderedSubset;

		MergeResult(OptionalMap<K, V> merged, boolean isOrderedSubset) {
			this.merged = merged;
			this.missingKeys = merged.absentKeysOrValues();
			this.isOrderedSubset = isOrderedSubset;
		}

		boolean hasMissingKeys() {
			return !missingKeys.isEmpty();
		}

		void logMissingKeys() {
			missingKeys.forEach(key -> LOG.warn("The Kryo registration for a previously registered class {} does not have a " +
				"proper serializer, because its previous serializer cannot be loaded or is no " +
				"longer valid but a new serializer is not available", key));
		}

		LinkedHashMap<K, V> getMerged() {
			return merged.unwrapOptionals();
		}

		/**
		 * returns {@code true} if left
		 */
		boolean isOrderedSubset() {
			return isOrderedSubset;
		}
	}

}
