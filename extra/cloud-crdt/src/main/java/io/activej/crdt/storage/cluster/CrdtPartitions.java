/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.crdt.storage.cluster;

import io.activej.common.initializer.WithInitializer;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.RendezvousHashSharder;
import io.activej.jmx.api.attribute.JmxAttribute;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static java.util.stream.Collectors.toList;

public final class CrdtPartitions<K extends Comparable<K>, S, P extends Comparable<P>> implements WithInitializer<CrdtPartitions<K, S, P>> {
	private final SortedMap<P, CrdtStorage<K, S>> currentPartitions = new TreeMap<>();
	private final Map<P, CrdtStorage<K, S>> partitionsView = Collections.unmodifiableMap(currentPartitions);

	private RendezvousHashSharder<P> sharder;

	private int topShards = 1;

	private CrdtPartitions() {
	}

	public static <K extends Comparable<K>, S, P extends Comparable<P>> CrdtPartitions<K, S, P> create() {
		return new CrdtPartitions<>();
	}

	public CrdtPartitions<K, S, P> withTopShards(int topShards) {
		this.topShards = topShards;
		return this;
	}

	public void setTopShards(int topShards) {
		this.topShards = topShards;
		sharder = RendezvousHashSharder.create(this.currentPartitions.keySet(), topShards);
	}

	public RendezvousHashSharder<P> getSharder() {
		return sharder;
	}

	/**
	 * Returns an unmodifiable view of all partitions
	 */
	public Map<P, CrdtStorage<K, S>> getPartitions() {
		return partitionsView;
	}

	/**
	 * Returns alive {@link CrdtStorage} by given id
	 *
	 * @param partitionId id of {@link CrdtStorage}
	 * @return alive {@link CrdtStorage}
	 */
	public @Nullable CrdtStorage<K, S> get(P partitionId) {
		return currentPartitions.get(partitionId);
	}

	private void recompute() {
		sharder = RendezvousHashSharder.create(currentPartitions.keySet(), topShards);
	}

	@Override
	public String toString() {
		return "CrdtPartitions{partitions=" + currentPartitions;
	}

	// region JMX
	@JmxAttribute
	public List<String> getAllPartitions() {
		return currentPartitions.keySet().stream()
				.map(Object::toString)
				.collect(toList());
	}
	// endregion
}
