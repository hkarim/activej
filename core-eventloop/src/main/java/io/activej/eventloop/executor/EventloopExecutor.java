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

package io.activej.eventloop.executor;

import io.activej.async.callback.AsyncComputation;
import io.activej.common.function.RunnableEx;
import io.activej.common.function.SupplierEx;
import io.activej.eventloop.Eventloop;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * An abstraction over the {@link Eventloop} that can receive, dispatch and run tasks in it.
 * <p>
 * As a plain case, {@link Eventloop} itself implements it and posts received tasks to its own queues.
 *
 * @see BlockingEventloopExecutor
 */
public interface EventloopExecutor extends Executor {
	/**
	 * Executes the given computation at some time in the future in some underlying eventloop.
	 */
	@NotNull CompletableFuture<Void> submit(@NotNull RunnableEx computation);

	/**
	 * Executes the given computation at some time in the future in some underlying eventloop
	 * and returns its result in a {@link CompletableFuture future}.
	 */
	<T> @NotNull CompletableFuture<T> submit(AsyncComputation<? extends T> computation);

	/**
	 * Executes the given computation at some time in the future in some underlying eventloop
	 * and returns its result in a {@link CompletableFuture future}.
	 */
	default <T> @NotNull CompletableFuture<T> submit(SupplierEx<? extends AsyncComputation<? extends T>> computation) {
		return submit(AsyncComputation.ofDeferred(computation));
	}
}
