package io.activej.crdt.wal;

import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.GSet;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageFs;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.activej.common.Utils.first;
import static io.activej.crdt.wal.FileWriteAheadLog.EXT_FINAL;
import static io.activej.crdt.wal.FileWriteAheadLog.FRAME_FORMAT;
import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.LONG_SERIALIZER;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public class FileWriteAheadLogTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private final CrdtDataSerializer<Long, GSet<Integer>> serializer = new CrdtDataSerializer<>(LONG_SERIALIZER, new GSet.Serializer<>(INT_SERIALIZER));
	private final CrdtFunction<GSet<Integer>> function = new CrdtFunction<GSet<Integer>>() {
		@Override
		public GSet<Integer> merge(GSet<Integer> first, GSet<Integer> second) {
			return first.merge(second);
		}

		@Override
		public GSet<Integer> extract(GSet<Integer> state, long timestamp) {
			return state;
		}
	};

	private FileWriteAheadLog<Long, GSet<Integer>> wal;
	private CrdtStorage<Long, GSet<Integer>> storage;
	private Executor executor;
	private Path path;

	@Before
	public void setUp() throws Exception {
		executor = Executors.newSingleThreadExecutor();
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		path = temporaryFolder.newFolder().toPath();
		LocalActiveFs storageFs = LocalActiveFs.create(eventloop, executor, temporaryFolder.newFolder().toPath());
		await(storageFs.start());
		storage = CrdtStorageFs.create(eventloop, storageFs, serializer, function);
		WalUploader<Long, GSet<Integer>> uploader = WalUploader.create(eventloop, executor, path, function, serializer, storage);
		wal = FileWriteAheadLog.create(eventloop, executor, path, serializer, uploader);
	}

	@Test
	public void singleFlushSequential() {
		await(wal.start());
		List<CrdtData<Long, GSet<Integer>>> expected = asList(
				new CrdtData<>(1L, GSet.of(1, 2, 3, 6, 9, 10, 11)),
				new CrdtData<>(2L, GSet.of(-12, 0, 2, 3, 100, 200))
		);
		await(wal.put(1L, GSet.of(1, 2, 3)));
		await(wal.put(2L, GSet.of(-12, 0, 200)));
		await(wal.put(1L, GSet.of(1, 6)));
		await(wal.put(2L, GSet.of(2, 3, 100)));
		await(wal.put(1L, GSet.of(9, 10, 11)));

		await(wal.flush());

		List<CrdtData<Long, GSet<Integer>>> actual = await(await(storage.download()).toList());
		assertEquals(expected, actual);
	}

	@Test
	public void singleFlushConsecutive() {
		await(wal.start());
		List<CrdtData<Long, GSet<Integer>>> expected = asList(
				new CrdtData<>(1L, GSet.of(1, 2, 3, 6, 9, 10, 11)),
				new CrdtData<>(2L, GSet.of(-12, 0, 2, 3, 100, 200))
		);
		await(
				wal.put(1L, GSet.of(1, 2, 3)),
				wal.put(2L, GSet.of(-12, 0, 200)),
				wal.put(1L, GSet.of(1, 6)),
				wal.put(2L, GSet.of(2, 3, 100)),
				wal.put(1L, GSet.of(9, 10, 11)),
				wal.flush()
		);

		List<CrdtData<Long, GSet<Integer>>> actual = await(await(storage.download()).toList());
		assertEquals(expected, actual);
	}


	@Test
	public void multipleFlushesSequential() {
		await(wal.start());
		List<CrdtData<Long, GSet<Integer>>> expectedAfterFlush1 = asList(
				new CrdtData<>(1L, GSet.of(1, 2, 3, 6)),
				new CrdtData<>(2L, GSet.of(-12, 0, 200))
		);
		List<CrdtData<Long, GSet<Integer>>> expectedAfterFlush2 = asList(
				new CrdtData<>(1L, GSet.of(1, 2, 3, 6, 9, 10, 11)),
				new CrdtData<>(2L, GSet.of(-12, 0, 2, 3, 100, 200))
		);
		await(wal.put(1L, GSet.of(1, 2, 3)));
		await(wal.put(2L, GSet.of(-12, 0, 200)));
		await(wal.put(1L, GSet.of(1, 6)));
		await(wal.flush());

		List<CrdtData<Long, GSet<Integer>>> actualAfterFlush1 = await(await(storage.download()).toList());
		assertEquals(expectedAfterFlush1, actualAfterFlush1);

		await(wal.put(2L, GSet.of(2, 3, 100)));
		await(wal.put(1L, GSet.of(9, 10, 11)));
		await(wal.flush());

		List<CrdtData<Long, GSet<Integer>>> actualAfterFlush2 = await(await(storage.download()).toList());
		assertEquals(expectedAfterFlush2, actualAfterFlush2);
	}

	@Test
	public void multipleFlushesConsecutive() {
		await(wal.start());
		List<CrdtData<Long, GSet<Integer>>> expected = asList(
				new CrdtData<>(1L, GSet.of(1, 2, 3, 6, 9, 10, 11)),
				new CrdtData<>(2L, GSet.of(-12, 0, 2, 3, 100, 200))
		);
		await(
				wal.put(1L, GSet.of(1, 2, 3)),
				wal.put(2L, GSet.of(-12, 0, 200)),
				wal.put(1L, GSet.of(1, 6)),
				wal.flush(),
				wal.put(2L, GSet.of(2, 3, 100)),
				wal.put(1L, GSet.of(9, 10, 11)),
				wal.flush()
		);

		List<CrdtData<Long, GSet<Integer>>> actual = await(await(storage.download()).toList());
		assertEquals(expected, actual);
	}

	@Test
	public void startupWithRemainingWALFiles() throws IOException {
		List<CrdtData<Long, GSet<Integer>>> expected = asList(
				new CrdtData<>(1L, GSet.of(1, 2, 3, 6, 9, 10, 11)),
				new CrdtData<>(2L, GSet.of(-12, 0, 2, 3, 100, 200))
		);
		craftWALFile(
				new CrdtData<>(1L, GSet.of(1, 2, 3)),
				new CrdtData<>(2L, GSet.of(-12, 0, 200)),
				new CrdtData<>(1L, GSet.of(1, 6))
		);
		craftWALFile(
				new CrdtData<>(2L, GSet.of(2, 3, 100)),
				new CrdtData<>(1L, GSet.of(9, 10, 11))
		);

		Set<Path> remainingWalFiles;
		try (Stream<Path> list = Files.list(path)) {
			remainingWalFiles = list.collect(toSet());
			assertEquals(2, remainingWalFiles.size());
			assertTrue(await(await(storage.download()).toList()).isEmpty());
		}

		await(wal.start());
		try (Stream<Path> list = Files.list(path)) {
			Set<Path> walFilesAfterStart = list.collect(toSet());
			assertEquals(1, walFilesAfterStart.size());
			assertFalse(remainingWalFiles.contains(first(walFilesAfterStart)));
		}

		List<CrdtData<Long, GSet<Integer>>> actual = await(await(storage.download()).toList());
		assertEquals(expected, actual);
	}

	@Test
	public void startupWithMalformedWALFiles() throws IOException {
		List<CrdtData<Long, GSet<Integer>>> expected = asList(
				new CrdtData<>(1L, GSet.of(1, 2, 3, 6, 9, 10, 11)),
				new CrdtData<>(2L, GSet.of(-124, -12, 0, 2, 3, 4, 45, 100, 200))
		);
		Path walFile1 = craftWALFile(
				new CrdtData<>(1L, GSet.of(1, 2, 3)),
				new CrdtData<>(2L, GSet.of(-12, 0, 200)),
				new CrdtData<>(1L, GSet.of(1, 6)),
				new CrdtData<>(1L, GSet.of(23, -53))
		);
		Path walFile2 = craftWALFile(
				new CrdtData<>(2L, GSet.of(2, 3, 100)),
				new CrdtData<>(1L, GSet.of(9, 10, 11)),
				new CrdtData<>(2L, GSet.of(-124, 4, 45)),
				new CrdtData<>(1L, GSet.of(20988, -300, 75)),
				new CrdtData<>(2L, GSet.of(100000, -100000))
		);
		truncateFile(walFile1, 0.75);
		truncateFile(walFile2, 0.75);

		Set<Path> remainingWalFiles;
		try (Stream<Path> list = Files.list(path)) {
			remainingWalFiles = list.collect(toSet());
			assertEquals(2, remainingWalFiles.size());
			assertTrue(await(await(storage.download()).toList()).isEmpty());
		}

		await(wal.start());
		try (Stream<Path> list = Files.list(path)) {
			Set<Path> walFilesAfterStart = list.collect(toSet());
			assertEquals(1, walFilesAfterStart.size());
			assertFalse(remainingWalFiles.contains(first(walFilesAfterStart)));
		}

		List<CrdtData<Long, GSet<Integer>>> actual = await(await(storage.download()).toList());
		assertEquals(expected, actual);
	}

	@Test
	public void startupWithEmptyWALFiles() throws IOException {
		List<CrdtData<Long, GSet<Integer>>> expected = asList(
				new CrdtData<>(1L, GSet.of(-53, 1, 2, 3, 6, 23)),
				new CrdtData<>(2L, GSet.of(-12, 0, 12, 100, 200))
		);
		craftWALFile(
				new CrdtData<>(1L, GSet.of(1, 2, 3)),
				new CrdtData<>(2L, GSet.of(-12, 0, 200)),
				new CrdtData<>(1L, GSet.of(1, 6)),
				new CrdtData<>(1L, GSet.of(23, -53)),
				new CrdtData<>(2L, GSet.of(-12, 12, 100))
		);
		craftWALFile();
		Path walFile3 = craftWALFile();
		truncateFile(walFile3, 0);

		Set<Path> remainingWalFiles;
		try (Stream<Path> list = Files.list(path)) {
			remainingWalFiles = list.collect(toSet());
			assertEquals(3, remainingWalFiles.size());
			assertTrue(await(await(storage.download()).toList()).isEmpty());
		}

		await(wal.start());
		try (Stream<Path> list = Files.list(path)) {
			Set<Path> walFilesAfterStart = list.collect(toSet());
			assertEquals(1, walFilesAfterStart.size());
			assertFalse(remainingWalFiles.contains(first(walFilesAfterStart)));
		}

		List<CrdtData<Long, GSet<Integer>>> actual = await(await(storage.download()).toList());
		assertEquals(expected, actual);
	}

	@SafeVarargs
	private final Path craftWALFile(CrdtData<Long, GSet<Integer>>... mockData) {
		Path file = path.resolve(UUID.randomUUID() + EXT_FINAL);
		await(StreamSupplier.ofChannelSupplier(ChannelSupplier.of(mockData)
						.mapAsync(data -> Promises.delay(Duration.ofMillis(1), data)))
				.transformWith(ChannelSerializer.create(serializer)
						.withAutoFlushInterval(Duration.ZERO))
				.transformWith(ChannelFrameEncoder.create(FRAME_FORMAT))
				.streamTo(ChannelFileWriter.open(executor, file)));
		return file;
	}

	private void truncateFile(Path filename, double percent) throws IOException {
		assert percent >= 0 && percent <= 1;
		long size = Files.size(filename);
		try (FileChannel outChan = FileChannel.open(filename, WRITE)) {
			outChan.truncate((long) (size * percent));
		}
	}
}
