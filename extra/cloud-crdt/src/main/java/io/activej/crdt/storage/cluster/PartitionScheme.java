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

import io.activej.crdt.storage.CrdtStorage;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public final class PartitionScheme<K extends Comparable<K>, S, P extends Comparable<P>> {
	private final Map<P, ? extends CrdtStorage<K, S>> currentPartitions;
	private final @Nullable Map<P, ? extends CrdtStorage<K, S>> targetPartitions;

	public PartitionScheme(Map<P, ? extends CrdtStorage<K, S>> currentPartitions, @Nullable Map<P, ? extends CrdtStorage<K, S>> targetPartitions) {
		this.currentPartitions = currentPartitions;
		this.targetPartitions = targetPartitions;
	}

	public Map<P, ? extends CrdtStorage<K, S>> getCurrentPartitions() {
		return currentPartitions;
	}

	@Nullable
	public Map<P, ? extends CrdtStorage<K, S>> getTargetPartitions() {
		return targetPartitions;
	}
}
