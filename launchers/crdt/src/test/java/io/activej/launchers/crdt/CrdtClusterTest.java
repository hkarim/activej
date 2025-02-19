package io.activej.launchers.crdt;

import io.activej.config.Config;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.fs.LocalActiveFs;
import io.activej.http.AsyncHttpClient;
import io.activej.http.HttpRequest;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.promise.Promises;
import io.activej.promise.jmx.PromiseStats;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import io.activej.types.TypeT;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.stream.IntStream;

import static io.activej.config.converter.ConfigConverters.ofExecutor;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.crdt.util.Utils.toJson;
import static io.activej.http.HttpMethod.PUT;
import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static io.activej.test.TestUtils.assertCompleteFn;

@Ignore("manual demos")
public final class CrdtClusterTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	static class TestNodeLauncher extends CrdtNodeLauncher<String, TimestampContainer<Integer>> {
		private final Config config;

		public TestNodeLauncher(Config config) {
			this.config = config;
		}

		@Override
		protected Module getOverrideModule() {
			return new AbstractModule() {
				@Provides
				Config config() {
					return config;
				}
			};
		}

		@Override
		protected CrdtNodeLogicModule<String, TimestampContainer<Integer>> getBusinessLogicModule() {
			return new CrdtNodeLogicModule<String, TimestampContainer<Integer>>() {
				@Override
				protected void configure() {
					install(new CrdtHttpModule<String, TimestampContainer<Integer>>() {});
				}

				@Provides
				CrdtDescriptor<String, TimestampContainer<Integer>> descriptor() {
					return new CrdtDescriptor<>(
							TimestampContainer.createCrdtFunction(Integer::max),
							new CrdtDataSerializer<>(UTF8_SERIALIZER,
									TimestampContainer.createSerializer(INT_SERIALIZER)),
							String.class,
							new TypeT<TimestampContainer<Integer>>() {}.getType());
				}

				@Provides
				Executor executor(Config config) {
					return config.get(ofExecutor(), "crdt.local.executor");
				}

				@Provides
				ActiveFs fs(Eventloop eventloop, Executor executor, Config config) {
					return LocalActiveFs.create(eventloop, executor, config.get(ofPath(), "crdt.local.path"));
				}
			};
		}
	}

	@Test
	public void startFirst() throws Exception {
		new TestNodeLauncher(Config.create()
				.with("crdt.http.listenAddresses", "localhost:7000")
				.with("crdt.server.listenAddresses", "localhost:8000")
				.with("crdt.cluster.server.listenAddresses", "localhost:9000")
				.with("crdt.local.path", "/tmp/TESTS/crdt")
				.with("crdt.cluster.localPartitionId", "first")
				.with("crdt.cluster.replicationCount", "2")
				.with("crdt.cluster.partitions.second", "localhost:8001")
				//				.with("crdt.cluster.partitions.file", "localhost:8002")
		).launch(new String[0]);
	}

	@Test
	public void startSecond() throws Exception {
		new TestNodeLauncher(Config.create()
				.with("crdt.http.listenAddresses", "localhost:7001")
				.with("crdt.server.listenAddresses", "localhost:8001")
				.with("crdt.cluster.server.listenAddresses", "localhost:9001")
				.with("crdt.local.path", "/tmp/TESTS/crdt")
				.with("crdt.cluster.localPartitionId", "second")
				.with("crdt.cluster.replicationCount", "2")
				.with("crdt.cluster.partitions.first", "localhost:8000")
				//				.with("crdt.cluster.partitions.file", "localhost:8002")
		).launch(new String[0]);
	}

	@Test
	public void startFileServer() throws Exception {
		new CrdtFileServerLauncher<String, TimestampContainer<Integer>>() {
			@Override
			protected CrdtFileServerLogicModule<String, TimestampContainer<Integer>> getBusinessLogicModule() {
				return new CrdtFileServerLogicModule<String, TimestampContainer<Integer>>() {};
			}

			@Override
			protected Module getOverrideModule() {
				return new AbstractModule() {
					@Provides
					Config config() {
						return Config.create()
								.with("crdt.localPath", "/tmp/TESTS/fileServer")
								.with("crdt.server.listenAddresses", "localhost:8002");
					}
				};
			}

			@Provides
			CrdtDescriptor<String, TimestampContainer<Integer>> descriptor() {
				return new CrdtDescriptor<>(
						TimestampContainer.createCrdtFunction(Integer::max),
						new CrdtDataSerializer<>(UTF8_SERIALIZER,
								TimestampContainer.createSerializer(INT_SERIALIZER)),
						String.class,
						new TypeT<TimestampContainer<Integer>>() {}.getType());
			}
		}.launch(new String[0]);
	}

	@Test
	public void uploadWithHTTP() {
		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());

		PromiseStats uploadStat = PromiseStats.create(Duration.ofSeconds(5));

		Type manifest = new TypeT<CrdtData<String, Integer>>() {}.getType();

		Promises.sequence(IntStream.range(0, 1_000_000)
						.mapToObj(i ->
								() -> client.request(HttpRequest.of(PUT, "http://127.0.0.1:7000")
												.withBody(toJson(manifest, new CrdtData<>("value_" + i, i))))
										.toVoid()))
				.whenException(Exception::printStackTrace)
				.whenComplete(uploadStat.recordStats())
				.whenComplete(assertCompleteFn($ -> System.out.println(uploadStat)));

		// RemoteCrdtClient<String, Integer> client = RemoteCrdtClient.create(eventloop, ADDRESS, CRDT_DATA_SERIALIZER);
		//
		// StreamProducer.ofStream(IntStream.range(0, 1000000)
		// 		.mapToObj(i -> new CrdtData<>("key_" + i, i))).streamTo(client.uploadStream())
		// 		.getEndOfStream()
		// 		.thenRun(() -> System.out.println("finished"));
	}

	@Test
	public void uploadWithStreams() {
		CrdtStorageClient<String, Integer> client = CrdtStorageClient.create(Eventloop.getCurrentEventloop(), new InetSocketAddress("localhost", 9000), UTF8_SERIALIZER, INT_SERIALIZER);

		PromiseStats uploadStat = PromiseStats.create(Duration.ofSeconds(5));

		StreamSupplier.ofStream(IntStream.range(0, 1000000)
						.mapToObj(i -> new CrdtData<>("value_" + i, i))).streamTo(StreamConsumer.ofPromise(client.upload()))
				.whenComplete(uploadStat.recordStats())
				.whenComplete(assertCompleteFn($ -> {
					System.out.println(uploadStat);
					System.out.println("finished");
				}));
	}

	@Test
	public void downloadStuff() {
		CrdtStorageClient<String, Integer> client = CrdtStorageClient.create(Eventloop.getCurrentEventloop(), new InetSocketAddress(9001), UTF8_SERIALIZER, INT_SERIALIZER);

		await(client.download().then(supplier -> supplier.streamTo(StreamConsumer.ofConsumer(System.out::println))));
	}
}
