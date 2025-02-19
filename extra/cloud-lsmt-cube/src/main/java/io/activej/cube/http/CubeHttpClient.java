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

package io.activej.cube.http;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.initializer.WithInitializer;
import io.activej.cube.CubeQuery;
import io.activej.cube.ICube;
import io.activej.cube.QueryResult;
import io.activej.cube.exception.CubeException;
import io.activej.http.AsyncHttpClient;
import io.activej.http.HttpRequest;
import io.activej.http.HttpUtils;
import io.activej.http.IAsyncHttpClient;
import io.activej.promise.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.cube.Utils.fromJson;
import static io.activej.cube.Utils.toJson;
import static io.activej.cube.http.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class CubeHttpClient implements ICube, WithInitializer<CubeHttpClient> {
	private static final Logger logger = LoggerFactory.getLogger(CubeHttpClient.class);

	private final String url;
	private final IAsyncHttpClient httpClient;
	private QueryResultCodec queryResultCodec;
	private AggregationPredicateCodec aggregationPredicateCodec;
	private final Map<String, Type> attributeTypes = new LinkedHashMap<>();
	private final Map<String, Type> measureTypes = new LinkedHashMap<>();

	private DefiningClassLoader classLoader = DefiningClassLoader.create();

	private CubeHttpClient(IAsyncHttpClient httpClient, String url) {
		this.url = url.replaceAll("/$", "");
		this.httpClient = httpClient;
	}

	public static CubeHttpClient create(AsyncHttpClient httpClient, String cubeServletUrl) {
		return new CubeHttpClient(httpClient, cubeServletUrl);
	}

	public static CubeHttpClient create(AsyncHttpClient httpClient, URI cubeServletUrl) {
		return create(httpClient, cubeServletUrl.toString());
	}

	public CubeHttpClient withClassLoader(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
		return this;
	}

	public CubeHttpClient withAttribute(String attribute, Type type) {
		attributeTypes.put(attribute, type);
		return this;
	}

	public CubeHttpClient withMeasure(String measureId, Type type) {
		measureTypes.put(measureId, type);
		return this;
	}

	private AggregationPredicateCodec getAggregationPredicateCodec() {
		if (aggregationPredicateCodec == null) {
			aggregationPredicateCodec = AggregationPredicateCodec.create(attributeTypes, measureTypes);
		}
		return aggregationPredicateCodec;
	}

	private QueryResultCodec getQueryResultCodec() {
		if (queryResultCodec == null) {
			queryResultCodec = QueryResultCodec.create(classLoader, attributeTypes, measureTypes);
		}
		return queryResultCodec;
	}

	@Override
	public Map<String, Type> getAttributeTypes() {
		return attributeTypes;
	}

	@Override
	public Map<String, Type> getMeasureTypes() {
		return measureTypes;
	}

	@Override
	public Promise<QueryResult> query(CubeQuery query) {
		return httpClient.request(buildRequest(query))
				.mapException(e -> new CubeException("HTTP request failed", e))
				.then(response -> response.loadBody()
						.mapException(e -> new CubeException("HTTP request failed", e))
						.map(body -> {
							try {
								if (response.getCode() != 200) {
									throw new CubeException("CubeHTTP query failed. Response code: " + response.getCode() + " Body: " + body.getString(UTF_8));
								}
								return fromJson(getQueryResultCodec(), body);
							} catch (MalformedDataException e) {
								throw new CubeException("Cube HTTP query failed. Invalid data received", e);
							}
						})
						.whenComplete(toLogger(logger, "query", query)));
	}

	private HttpRequest buildRequest(CubeQuery query) {
		Map<String, String> urlParams = new LinkedHashMap<>();

		urlParams.put(ATTRIBUTES_PARAM, String.join(",", query.getAttributes()));
		urlParams.put(MEASURES_PARAM, String.join(",", query.getMeasures()));
		urlParams.put(WHERE_PARAM, toJson(getAggregationPredicateCodec(), query.getWhere()));
		urlParams.put(SORT_PARAM, formatOrderings(query.getOrderings()));
		urlParams.put(HAVING_PARAM, toJson(getAggregationPredicateCodec(), query.getHaving()));
		if (query.getLimit() != null)
			urlParams.put(LIMIT_PARAM, query.getLimit().toString());
		if (query.getOffset() != null)
			urlParams.put(OFFSET_PARAM, query.getOffset().toString());
		urlParams.put(REPORT_TYPE_PARAM, query.getReportType().toString().toLowerCase());
		String url = this.url + "/" + "?" + HttpUtils.renderQueryString(urlParams);

		return HttpRequest.get(url);
	}
}
