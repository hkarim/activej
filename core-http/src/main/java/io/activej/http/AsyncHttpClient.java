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

package io.activej.http;

import io.activej.async.exception.AsyncTimeoutException;
import io.activej.async.service.EventloopService;
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.MemSize;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.inspector.AbstractInspector;
import io.activej.common.inspector.BaseInspector;
import io.activej.dns.AsyncDnsClient;
import io.activej.dns.RemoteAsyncDnsClient;
import io.activej.dns.protocol.DnsQueryException;
import io.activej.dns.protocol.DnsResponse;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.eventloop.net.SocketSettings;
import io.activej.eventloop.schedule.ScheduledRunnable;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executor;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.http.HttpUtils.translateToHttpException;
import static io.activej.http.Protocol.*;
import static io.activej.jmx.stats.MBeanFormat.formatListAsMultilineString;
import static io.activej.net.socket.tcp.AsyncTcpSocketSsl.wrapClientSocket;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of {@link IAsyncHttpClient} that asynchronously connects
 * to real HTTP servers and gets responses from them.
 * <p>
 * It is also an {@link EventloopService} that needs its close method to be called
 * to clean up the keep-alive connections etc.
 */
@SuppressWarnings({"WeakerAccess", "unused", "UnusedReturnValue"})
public final class AsyncHttpClient implements IAsyncHttpClient, IAsyncWebSocketClient, EventloopService, EventloopJmxBeanWithStats, WithInitializer<AsyncHttpClient> {
	private static final Logger logger = getLogger(AsyncHttpClient.class);
	private static final boolean CHECK = Checks.isEnabled(AsyncHttpClient.class);

	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();
	public static final Duration CONNECT_TIMEOUT = ApplicationSettings.getDuration(AsyncHttpClient.class, "connectTimeout", Duration.ZERO);
	public static final Duration READ_WRITE_TIMEOUT = ApplicationSettings.getDuration(AsyncHttpClient.class, "readWriteTimeout", Duration.ZERO);
	public static final Duration READ_WRITE_TIMEOUT_SHUTDOWN = ApplicationSettings.getDuration(AsyncHttpClient.class, "readWriteTimeout_Shutdown", Duration.ofSeconds(3));
	public static final Duration KEEP_ALIVE_TIMEOUT = ApplicationSettings.getDuration(AsyncHttpClient.class, "keepAliveTimeout", Duration.ZERO);
	public static final MemSize MAX_BODY_SIZE = ApplicationSettings.getMemSize(AsyncHttpClient.class, "maxBodySize", MemSize.ZERO);
	public static final MemSize MAX_WEB_SOCKET_MESSAGE_SIZE = ApplicationSettings.getMemSize(AsyncHttpClient.class, "maxWebSocketMessageSize", MemSize.megabytes(1));
	public static final int MAX_KEEP_ALIVE_REQUESTS = ApplicationSettings.getInt(AsyncHttpClient.class, "maxKeepAliveRequests", 0);

	private final @NotNull Eventloop eventloop;
	private @NotNull AsyncDnsClient asyncDnsClient;
	private @NotNull SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	final HashMap<InetSocketAddress, AddressLinkedList> addresses = new HashMap<>();
	final ConnectionsLinkedList poolKeepAlive = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolReadWrite = new ConnectionsLinkedList();
	private int poolKeepAliveExpired;
	private int poolReadWriteExpired;

	private @Nullable ScheduledRunnable expiredConnectionsCheck;

	// timeouts
	int connectTimeoutMillis = (int) CONNECT_TIMEOUT.toMillis();
	int readWriteTimeoutMillis = (int) READ_WRITE_TIMEOUT.toMillis();
	int readWriteTimeoutMillisShutdown = (int) READ_WRITE_TIMEOUT_SHUTDOWN.toMillis();
	int keepAliveTimeoutMillis = (int) KEEP_ALIVE_TIMEOUT.toMillis();
	int maxBodySize = MAX_BODY_SIZE.toInt();
	int maxWebSocketMessageSize = MAX_WEB_SOCKET_MESSAGE_SIZE.toInt();
	int maxKeepAliveRequests = MAX_KEEP_ALIVE_REQUESTS;

	// SSL
	private SSLContext sslContext;
	private Executor sslExecutor;

	private @Nullable AsyncTcpSocketNio.Inspector socketInspector;
	private @Nullable AsyncTcpSocketNio.Inspector socketSslInspector;
	@Nullable Inspector inspector;

	public interface Inspector extends BaseInspector<Inspector> {
		void onRequest(HttpRequest request);

		void onResolve(HttpRequest request, DnsResponse dnsResponse);

		void onResolveError(HttpRequest request, Exception e);

		default void onConnecting(HttpRequest request, InetSocketAddress address) {
		}

		void onConnect(HttpRequest request, HttpClientConnection connection);

		void onConnectError(HttpRequest request, InetSocketAddress address, Exception e);

		void onHttpResponse(HttpResponse response);

		void onHttpError(HttpClientConnection connection, Exception e);

		void onDisconnect(HttpClientConnection connection);
	}

	@SuppressWarnings("WeakerAccess")
	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
		private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final EventStats totalRequests = EventStats.create(SMOOTHING_WINDOW);
		private final ExceptionStats resolveErrors = ExceptionStats.create();
		private final EventStats connected = EventStats.create(SMOOTHING_WINDOW);
		private final ExceptionStats connectErrors = ExceptionStats.create();
		private long responses;
		private final EventStats httpTimeouts = EventStats.create(SMOOTHING_WINDOW);
		private final ExceptionStats httpErrors = ExceptionStats.create();
		private long responsesErrors;
		private final EventStats sslErrors = EventStats.create(SMOOTHING_WINDOW);
		private long activeConnections;
		private int connecting;

		@Override
		public void onRequest(HttpRequest request) {
			totalRequests.recordEvent();
		}

		@Override
		public void onResolve(HttpRequest request, DnsResponse dnsResponse) {
		}

		@Override
		public void onResolveError(HttpRequest request, Exception e) {
			resolveErrors.recordException(e, request.getUrl().getHost());
		}

		@Override
		public void onConnecting(HttpRequest request, InetSocketAddress address) {
			connecting++;
		}

		@Override
		public void onConnect(HttpRequest request, HttpClientConnection connection) {
			activeConnections++;
			connecting--;
			connected.recordEvent();
		}

		@Override
		public void onConnectError(HttpRequest request, InetSocketAddress address, Exception e) {
			connecting--;
			connectErrors.recordException(e, request.getUrl().getHost());
		}

		@Override
		public void onHttpResponse(HttpResponse response) {
			responses++;
		}

		@Override
		public void onHttpError(HttpClientConnection connection, Exception e) {
			if (e instanceof AsyncTimeoutException) {
				httpTimeouts.recordEvent();
				return;
			}
			httpErrors.recordException(e);
			if (e instanceof SSLException) {
				sslErrors.recordEvent();
			}
			// when connection is in keep-alive state, it means that the response already happened,
			// so error of keep-alive connection is not a response error
			if (!connection.isKeepAlive()) {
				responsesErrors++;
			}
		}

		@Override
		public void onDisconnect(HttpClientConnection connection) {
			activeConnections--;
		}

		@JmxAttribute(extraSubAttributes = "totalCount", description = "all requests that were sent (both successful and failed)")
		public EventStats getTotalRequests() {
			return totalRequests;
		}

		@JmxAttribute
		public ExceptionStats getResolveErrors() {
			return resolveErrors;
		}

		@JmxAttribute
		public ExceptionStats getConnectErrors() {
			return connectErrors;
		}

		@JmxAttribute(description = "number of \"open connection\" events)")
		public EventStats getConnected() {
			return connected;
		}

		@JmxAttribute
		public EventStats getHttpTimeouts() {
			return httpTimeouts;
		}

		@JmxAttribute
		public ExceptionStats getHttpErrors() {
			return httpErrors;
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public long getActiveRequests() {
			return totalRequests.getTotalCount() -
					(httpTimeouts.getTotalCount() + resolveErrors.getTotal() + connectErrors.getTotal() + responsesErrors + responses);
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public long getTotalResponses() {
			return responses;
		}

		@JmxAttribute
		public EventStats getSslErrors() {
			return sslErrors;
		}

		@JmxAttribute
		public long getActiveConnections() {
			return activeConnections;
		}

		@JmxAttribute(description = "number of \"currently connecting\" sockets)")
		public int getConnecting() {
			return connecting;
		}
	}

	private int inetAddressIdx = 0;

	// region builders
	private AsyncHttpClient(@NotNull Eventloop eventloop, @NotNull AsyncDnsClient asyncDnsClient) {
		this.eventloop = eventloop;
		this.asyncDnsClient = asyncDnsClient;
	}

	public static AsyncHttpClient create(@NotNull Eventloop eventloop) {
		AsyncDnsClient defaultDnsClient = RemoteAsyncDnsClient.create(eventloop);
		return new AsyncHttpClient(eventloop, defaultDnsClient);
	}

	public AsyncHttpClient withSocketSettings(@NotNull SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}

	public AsyncHttpClient withDnsClient(@NotNull AsyncDnsClient asyncDnsClient) {
		this.asyncDnsClient = asyncDnsClient;
		return this;
	}

	public AsyncHttpClient withSslEnabled(@NotNull SSLContext sslContext, @NotNull Executor sslExecutor) {
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
		return this;
	}

	public AsyncHttpClient withKeepAliveTimeout(@NotNull Duration keepAliveTime) {
		this.keepAliveTimeoutMillis = (int) keepAliveTime.toMillis();
		return this;
	}

	public AsyncHttpClient withNoKeepAlive() {
		return withKeepAliveTimeout(Duration.ZERO);
	}

	public AsyncHttpClient withMaxKeepAliveRequests(int maxKeepAliveRequests) {
		checkArgument(maxKeepAliveRequests >= 0, "Maximum number of requests per keep-alive connection should not be less than zero");
		this.maxKeepAliveRequests = maxKeepAliveRequests;
		return this;
	}

	public AsyncHttpClient withReadWriteTimeout(@NotNull Duration readWriteTimeout) {
		this.readWriteTimeoutMillis = (int) readWriteTimeout.toMillis();
		return this;
	}

	public AsyncHttpClient withReadWriteTimeout(@NotNull Duration readWriteTimeout, @NotNull Duration readWriteTimeoutShutdown) {
		this.readWriteTimeoutMillis = (int) readWriteTimeout.toMillis();
		this.readWriteTimeoutMillisShutdown = (int) readWriteTimeoutShutdown.toMillis();
		return this;
	}

	public AsyncHttpClient withConnectTimeout(@NotNull Duration connectTimeout) {
		this.connectTimeoutMillis = (int) connectTimeout.toMillis();
		return this;
	}

	public AsyncHttpClient withMaxBodySize(MemSize maxBodySize) {
		return withMaxBodySize(maxBodySize.toInt());
	}

	public AsyncHttpClient withMaxBodySize(int maxBodySize) {
		this.maxBodySize = maxBodySize != 0 ? maxBodySize : Integer.MAX_VALUE;
		return this;
	}

	public AsyncHttpClient withMaxWebSocketMessageSize(MemSize maxWebSocketMessageSize) {
		this.maxWebSocketMessageSize = maxWebSocketMessageSize.toInt();
		return this;
	}

	public AsyncHttpClient withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	public AsyncHttpClient withSocketInspector(AsyncTcpSocketNio.Inspector socketInspector) {
		this.socketInspector = socketInspector;
		return this;
	}

	public AsyncHttpClient withSocketSslInspector(AsyncTcpSocketNio.Inspector socketSslInspector) {
		this.socketSslInspector = socketSslInspector;
		return this;
	}
	// endregion

	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.delayBackground(1000L, () -> {
			expiredConnectionsCheck = null;
			poolKeepAliveExpired += poolKeepAlive.closeExpiredConnections(eventloop.currentTimeMillis() - keepAliveTimeoutMillis);
			boolean isClosing = closePromise != null;
			if (readWriteTimeoutMillis != 0 || isClosing) {
				poolReadWriteExpired += poolReadWrite.closeExpiredConnections(eventloop.currentTimeMillis() -
						(!isClosing ? readWriteTimeoutMillis : readWriteTimeoutMillisShutdown), new AsyncTimeoutException("Read timeout"));
			}
			if (getConnectionsCount() != 0) {
				scheduleExpiredConnectionsCheck();
				if (isClosing) {
					logger.info("...Waiting for {}", this);
				}
			}
		});
	}

	private @Nullable HttpClientConnection takeKeepAliveConnection(InetSocketAddress address) {
		AddressLinkedList addresses = this.addresses.get(address);
		if (addresses == null)
			return null;
		HttpClientConnection connection = addresses.removeLastNode();
		assert connection != null;
		assert connection.pool == poolKeepAlive;
		assert connection.remoteAddress.equals(address);
		connection.pool.removeNode(connection); // moving from keep-alive state to taken(null) state
		if (addresses.isEmpty()) {
			this.addresses.remove(address);
		}
		return connection;
	}

	void returnToKeepAlivePool(HttpClientConnection connection) {
		assert !connection.isClosed();
		AddressLinkedList addresses = this.addresses.get(connection.remoteAddress);
		if (addresses == null) {
			addresses = new AddressLinkedList();
			this.addresses.put(connection.remoteAddress, addresses);
		}
		addresses.addLastNode(connection);
		connection.switchPool(poolKeepAlive);

		if (expiredConnectionsCheck == null) {
			scheduleExpiredConnectionsCheck();
		}
	}

	@Override
	public Promise<HttpResponse> request(HttpRequest request) {
		if (CHECK) checkArgument(request.getProtocol(), protocol -> protocol == HTTP || protocol == HTTPS);

		//noinspection unchecked
		return (Promise<HttpResponse>) doRequest(request, false);
	}

	/**
	 * Sends a web socket request and returns a promise of a web socket.
	 * <p>
	 * Sent request must not have a body or body stream.
	 * <p>
	 * After receiving a {@link WebSocket}, caller can inspect server response via calling {@link WebSocket#getResponse()}.
	 * If a response does not satisfy a caller, it may close the web socket with an appropriate exception.
	 *
	 * @param request web socket request
	 * @return promise of a web socket
	 */
	@Override
	public Promise<WebSocket> webSocketRequest(HttpRequest request) {
		checkState(WebSocket.ENABLED, "Web sockets are disabled by application settings");
		checkArgument(request.getProtocol() == WS || request.getProtocol() == WSS, "Wrong protocol");
		checkArgument(request.body == null && request.bodyStream == null, "No body should be present");

		//noinspection unchecked
		return (Promise<WebSocket>) doRequest(request, true);
	}

	private @NotNull Promise<?> doRequest(HttpRequest request, boolean isWebSocket) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		if (inspector != null) inspector.onRequest(request);
		String host = request.getUrl().getHost();

		assert host != null;

		return asyncDnsClient.resolve4(host)
				.then(
						dnsResponse -> {
							if (inspector != null) inspector.onResolve(request, dnsResponse);
							if (dnsResponse.isSuccessful()) {
								//noinspection ConstantConditions - dnsResponse is successful (not null)
								return doSend(request, dnsResponse.getRecord().getIps(), isWebSocket);
							} else {
								request.recycleBody();
								return Promise.ofException(new HttpException(new DnsQueryException(dnsResponse)));
							}
						},
						e -> {
							if (inspector != null) inspector.onResolveError(request, e);
							request.recycleBody();
							return Promise.ofException(translateToHttpException(e));
						});
	}

	private Promise<?> doSend(HttpRequest request, InetAddress[] inetAddresses, boolean isWebSocket) {
		InetAddress inetAddress = inetAddresses[(inetAddressIdx++ & Integer.MAX_VALUE) % inetAddresses.length];
		InetSocketAddress address = new InetSocketAddress(inetAddress, request.getUrl().getPort());

		HttpClientConnection keepAliveConnection = takeKeepAliveConnection(address);
		if (keepAliveConnection != null) {
			if (isWebSocket) {
				return keepAliveConnection.sendWebSocketRequest(request);
			} else {
				return keepAliveConnection.send(request);
			}
		}

		boolean isSecure = request.getProtocol().isSecure();
		if (isSecure && sslContext == null) {
			request.recycleBody();
			throw new IllegalArgumentException("Cannot send Secure Request without SSL enabled");
		}

		if (inspector != null) inspector.onConnecting(request, address);

		return AsyncTcpSocketNio.connect(address, connectTimeoutMillis, socketSettings)
				.then(
						asyncTcpSocketImpl -> {
							AsyncTcpSocketNio.Inspector socketInspector = isSecure ? this.socketInspector : socketSslInspector;
							if (socketInspector != null) {
								socketInspector.onConnect(asyncTcpSocketImpl);
								asyncTcpSocketImpl.setInspector(socketInspector);
							}

							String host = request.getUrl().getHost();
							assert host != null;

							AsyncTcpSocket asyncTcpSocket = isSecure ?
									wrapClientSocket(asyncTcpSocketImpl,
											host, request.getUrl().getPort(),
											sslContext, sslExecutor) :
									asyncTcpSocketImpl;

							HttpClientConnection connection = new HttpClientConnection(eventloop, this, asyncTcpSocket, address);

							if (inspector != null) inspector.onConnect(request, connection);

							if (expiredConnectionsCheck == null)
								scheduleExpiredConnectionsCheck();

							if (isWebSocket) {
								return connection.sendWebSocketRequest(request);
							} else {
								return connection.send(request);
							}
						},
						e -> {
							if (inspector != null) inspector.onConnectError(request, address, e);
							request.recycleBody();
							return Promise.ofException(translateToHttpException(e));
						});
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<Void> start() {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		return Promise.complete();
	}

	private @Nullable SettablePromise<Void> closePromise;

	public void onConnectionClosed() {
		if (getConnectionsCount() == 0 && closePromise != null) {
			closePromise.set(null);
			closePromise = null;
		}
	}

	@Override
	public @NotNull Promise<Void> stop() {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");

		SettablePromise<Void> promise = new SettablePromise<>();

		poolKeepAlive.closeAllConnections();
		assert addresses.isEmpty();
		keepAliveTimeoutMillis = 0;
		if (getConnectionsCount() == 0) {
			assert poolReadWrite.isEmpty();
			promise.set(null);
		} else {
			closePromise = promise;
			logger.info("Waiting for {}", this);
		}
		return promise;
	}

	// region jmx
	@JmxAttribute(description = "current number of connections", reducer = JmxReducerSum.class)
	public int getConnectionsCount() {
		return poolKeepAlive.size() + poolReadWrite.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsKeepAliveCount() {
		return poolKeepAlive.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsReadWriteCount() {
		return poolReadWrite.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsKeepAliveExpired() {
		return poolKeepAliveExpired;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsReadWriteExpired() {
		return poolReadWriteExpired;
	}

	@JmxOperation(description = "number of connections per address")
	public String getAddressConnections() {
		if (addresses.isEmpty())
			return "";
		List<String> result = new ArrayList<>();
		result.add("SocketAddress,ConnectionsCount");
		for (Entry<InetSocketAddress, AddressLinkedList> entry : addresses.entrySet()) {
			InetSocketAddress address = entry.getKey();
			AddressLinkedList connections = entry.getValue();
			result.add(address + ", " + connections.size());
		}
		return formatListAsMultilineString(result);
	}

	@JmxAttribute
	public @Nullable AsyncTcpSocketNio.JmxInspector getSocketStats() {
		return BaseInspector.lookup(socketInspector, AsyncTcpSocketNio.JmxInspector.class);
	}

	@JmxAttribute
	public @Nullable AsyncTcpSocketNio.JmxInspector getSocketStatsSsl() {
		return BaseInspector.lookup(socketSslInspector, AsyncTcpSocketNio.JmxInspector.class);
	}

	@JmxAttribute(name = "")
	public @Nullable JmxInspector getStats() {
		return BaseInspector.lookup(inspector, JmxInspector.class);
	}
	// endregion

	@Override
	public String toString() {
		return "AsyncHttpClient" + "{" + "read/write:" + poolReadWrite.size() + " keep-alive:" + poolKeepAlive.size() + "}";
	}
}
