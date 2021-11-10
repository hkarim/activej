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

import io.activej.async.callback.Callback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public interface DiscoveryService<K extends Comparable<K>, S, P extends Comparable<P>> {

	void discover(
			@Nullable PartitionScheme<K, S, P> previous,
			Callback<PartitionScheme<K, S, P>> cb
	);

	static <K extends Comparable<K>, S, P extends Comparable<P>> DiscoveryService<K, S, P> constant(@NotNull PartitionScheme<K, S, P> partitionScheme) {
		return (previous, cb) -> {
			if (previous == null ||
					!Objects.equals(previous.getCurrentPartitions(), partitionScheme.getCurrentPartitions()) ||
					!Objects.equals(previous.getTargetPartitions(), partitionScheme.getTargetPartitions())
			) {
				cb.accept(partitionScheme, null);
			}
		};
	}
}
