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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An OptionalMap is an order preserving map (like {@link LinkedHashMap}) where keys have a unique string name, but are
 * optionally present, and the values are optional.
 */
final class OptionalMap<K, V> {

	// --------------------------------------------------------------------------------------------------------
	// Factory
	// --------------------------------------------------------------------------------------------------------


	/**
	 * Creates an {@code OptionalMap} from the provided map.
	 *
	 * <p>This method is the equivalent of {@link Optional#of(Object)} but for maps. To support more than one {@code NULL}
	 * key, an optional map requires a unique string name to be associated with each key (provided by keyNameGetter)
	 *
	 * @param sourceMap     a source map to wrap as an optional map.
	 * @param keyNameGetter function that assigns a unique name to the keys of the source map.
	 * @param <K>           key type
	 * @param <V>           value type
	 * @return an {@code OptionalMap} with optional named keys, and optional values.
	 */
	static <K, V> OptionalMap<K, V> optionalMapOf(LinkedHashMap<K, V> sourceMap, Function<K, String> keyNameGetter) {

		LinkedHashMap<NamedKey<K>, Optional<V>> underlyingMap = new LinkedHashMap<>(sourceMap.size());

		sourceMap.forEach((k, v) -> {
			String keyName = keyNameGetter.apply(k);
			NamedKey<K> namedKey = new NamedKey<>(keyName, k);
			Optional<V> value = Optional.ofNullable(v);

			underlyingMap.put(namedKey, value);
		});

		return new OptionalMap<>(underlyingMap);
	}

	// --------------------------------------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------------------------------------

	private final LinkedHashMap<NamedKey<K>, Optional<V>> underlyingMap;

	OptionalMap() {
		this(new LinkedHashMap<>());
	}

	@SuppressWarnings("CopyConstructorMissesField")
	OptionalMap(OptionalMap<K, V> optionalMap) {
		this(new LinkedHashMap<>(optionalMap.underlyingMap));
	}

	private OptionalMap(LinkedHashMap<NamedKey<K>, Optional<V>> underlyingMap) {
		this.underlyingMap = underlyingMap;
	}

	// --------------------------------------------------------------------------------------------------------
	// API
	// --------------------------------------------------------------------------------------------------------

	int size() {
		return underlyingMap.size();
	}

	void put(String keyName, @Nullable K key, @Nullable V value) {
		underlyingMap.put(new NamedKey<>(keyName, key), Optional.ofNullable(value));
	}

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	void put(String keyName, @Nullable K key, Optional<V> value) {
		underlyingMap.put(new NamedKey<>(keyName, key), value);
	}

	void putAll(OptionalMap<K, V> right) {
		this.underlyingMap.putAll(right.underlyingMap);
	}

	/**
	 * returns the key names of any keys or values that are absent.
	 */
	Set<String> absentKeysOrValues() {
		return underlyingMap.entrySet()
			.stream()
			.filter(OptionalMap::keyOrValueIsAbsent)
			.map(entry -> entry.getKey().getKeyName())
			.collect(Collectors.toSet());
	}

	/**
	 * assuming all the entries of this map are present (keys and values) this method would return
	 * a map with these key and values, striped from their Optional wrappers.
	 * NOTE: please note that if any of the key or values are absent this method would throw an {@link IllegalStateException}.
	 */
	LinkedHashMap<K, V> unwrapOptionals() {
		final LinkedHashMap<K, V> unwrapped = new LinkedHashMap<>(underlyingMap.size());

		for (Map.Entry<NamedKey<K>, Optional<V>> entry : underlyingMap.entrySet()) {
			NamedKey<K> namedKey = entry.getKey();
			Optional<V> maybeValue = entry.getValue();
			if (namedKey.isKeyAbsent()) {
				throw new IllegalStateException("Missing key '" + namedKey.getKeyName() + "'");
			}
			if (!maybeValue.isPresent()) {
				throw new IllegalStateException("Missing value for the key '" + namedKey.getKeyName() + "'");
			}
			unwrapped.put(namedKey.getKey(), maybeValue.get());
		}

		return unwrapped;
	}

	private static <K, V> boolean keyOrValueIsAbsent(Entry<NamedKey<K>, Optional<V>> entry) {
		return entry.getKey().isKeyAbsent() || !entry.getValue().isPresent();
	}

	/**
	 * Note: that the equality is defined solely by the key-name (while ignoring the key-value)
	 */
	static final class NamedKey<T> {
		@Nonnull
		private final String name;

		@Nullable
		private final T key;

		NamedKey(String keyName, @Nullable T key) {
			this.name = checkNotNull(keyName, "name cannot be NULL.");
			this.key = key;
		}

		String getKeyName() {
			return name;
		}

		boolean isKeyAbsent() {
			return key == null;
		}

		T getKey() {
			if (key == null) {
				throw new IllegalStateException("get() cannot be called on an absent value");
			}
			return key;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			NamedKey<?> that = (NamedKey<?>) o;
			return name.equals(that.name);
		}

		@Override
		public int hashCode() {
			return name.hashCode();
		}
	}
}
