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

package io.activej.rpc.client.sender;

import io.activej.async.callback.Callback;
import io.activej.rpc.client.RpcClientConnectionPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkState;
import static io.activej.rpc.client.sender.RpcStrategies.NO_SENDER_AVAILABLE_EXCEPTION;

public final class RpcStrategyTypeDispatching implements RpcStrategy {
	private final Map<Class<?>, RpcStrategy> dataTypeToStrategy = new HashMap<>();
	private RpcStrategy defaultStrategy;

	private RpcStrategyTypeDispatching() {}

	public static RpcStrategyTypeDispatching create() {return new RpcStrategyTypeDispatching();}

	public RpcStrategyTypeDispatching on(@NotNull Class<?> dataType, @NotNull RpcStrategy strategy) {
		checkState(!dataTypeToStrategy.containsKey(dataType),
				() -> "Strategy for type " + dataType + " is already set");
		dataTypeToStrategy.put(dataType, strategy);
		return this;
	}

	public RpcStrategyTypeDispatching onDefault(RpcStrategy strategy) {
		checkState(defaultStrategy == null, "Default Strategy is already set");
		defaultStrategy = strategy;
		return this;
	}

	@Override
	public DiscoveryService getDiscoveryService() {
		List<DiscoveryService> discoveryServices = new ArrayList<>();
		for (RpcStrategy value : dataTypeToStrategy.values()) {
			discoveryServices.add(value.getDiscoveryService());
		}
		discoveryServices.add(defaultStrategy.getDiscoveryService());
		return DiscoveryService.combined(discoveryServices);
	}

	@Override
	public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
		HashMap<Class<?>, RpcSender> typeToSender = new HashMap<>();
		for (Map.Entry<Class<?>, RpcStrategy> entry : dataTypeToStrategy.entrySet()) {
			RpcSender sender = entry.getValue().createSender(pool);
			if (sender == null)
				return null;
			typeToSender.put(entry.getKey(), sender);
		}
		RpcSender defaultSender = null;
		if (defaultStrategy != null) {
			defaultSender = defaultStrategy.createSender(pool);
			if (typeToSender.isEmpty())
				return defaultSender;
			if (defaultSender == null)
				return null;
		}
		return new Sender(typeToSender, defaultSender);
	}

	static final class Sender implements RpcSender {
		private final HashMap<Class<?>, RpcSender> typeToSender;
		private final @Nullable RpcSender defaultSender;

		Sender(@NotNull HashMap<Class<?>, RpcSender> typeToSender, @Nullable RpcSender defaultSender) {
			this.typeToSender = typeToSender;
			this.defaultSender = defaultSender;
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, @NotNull Callback<O> cb) {
			RpcSender sender = typeToSender.get(request.getClass());
			if (sender == null) {
				sender = defaultSender;
			}
			if (sender != null) {
				sender.sendRequest(request, timeout, cb);
			} else {
				cb.accept(null, NO_SENDER_AVAILABLE_EXCEPTION);
			}
		}
	}
}
