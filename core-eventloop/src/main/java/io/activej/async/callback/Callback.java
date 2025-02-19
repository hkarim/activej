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

package io.activej.async.callback;

import io.activej.eventloop.Eventloop;
import org.jetbrains.annotations.Nullable;

import static io.activej.eventloop.util.RunnableWithContext.wrapContext;

/**
 * Represents a universal Callback interface
 */
@FunctionalInterface
public interface Callback<T> {
	/**
	 * Performs action upon completion of some computation
	 */
	void accept(T result, @Nullable Exception e);

	static <T> Callback<T> toAnotherEventloop(Eventloop anotherEventloop, Callback<T> cb) {
		return (result, e) -> anotherEventloop.execute(wrapContext(cb, () -> cb.accept(result, e)));
	}
}
