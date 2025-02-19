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

package io.activej.serializer.impl;

import io.activej.codegen.expression.Expression;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import org.jetbrains.annotations.NotNull;

import java.util.EnumMap;

import static io.activej.codegen.expression.Expressions.constructor;
import static io.activej.codegen.expression.Expressions.value;

public final class SerializerDefEnumMap extends SerializerDefRegularMap {
	public SerializerDefEnumMap(SerializerDef keySerializer, SerializerDef valueSerializer) {
		this(keySerializer, valueSerializer, false);
	}

	private SerializerDefEnumMap(SerializerDef keySerializer, SerializerDef valueSerializer, boolean nullable) {
		super(keySerializer, valueSerializer, EnumMap.class, EnumMap.class, Enum.class, Object.class, nullable);
	}

	@Override
	protected @NotNull SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDefEnumMap(keySerializer, valueSerializer, true);
	}

	@Override
	protected @NotNull Expression createBuilder(Expression length) {
		return constructor(EnumMap.class, value(keySerializer.getDecodeType()));
	}

}
