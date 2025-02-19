package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.datastream.TestStreamTransformers.decorate;
import static io.activej.datastream.TestStreamTransformers.oneByOne;
import static io.activej.datastream.TestUtils.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamSplitterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void test1() {
		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3);
		StreamSplitter<Integer, Integer> streamConcat = StreamSplitter.create(
				(item, acceptors) -> {
					for (StreamDataAcceptor<Integer> acceptor : acceptors) {
						acceptor.accept(item);
					}
				});
		StreamConsumerToList<Integer> consumerToList1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumerToList2 = StreamConsumerToList.create();

		await(
				source.streamTo(streamConcat.getInput()),
				streamConcat.newOutput().streamTo(consumerToList1.transformWith(oneByOne())),
				streamConcat.newOutput().streamTo(consumerToList2.transformWith(oneByOne()))
		);

		assertEquals(asList(1, 2, 3), consumerToList1.getList());
		assertEquals(asList(1, 2, 3), consumerToList2.getList());
		assertEndOfStream(source);
		assertEndOfStream(streamConcat.getInput());
		assertSuppliersEndOfStream(streamConcat.getOutputs());
	}

	@Test
	public void testConsumerDisconnectWithError() {
		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4, 5);
		StreamSplitter<Integer, Integer> streamSplitter = StreamSplitter.create(
				(item, acceptors) -> {
					for (StreamDataAcceptor<Integer> acceptor : acceptors) {
						acceptor.accept(item);
					}
				});

		StreamConsumerToList<Integer> consumerToList1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumerToList2 = StreamConsumerToList.create();

		StreamConsumerToList<Integer> badConsumer = StreamConsumerToList.create();
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(
				source.streamTo(streamSplitter.getInput()),
				streamSplitter.newOutput()
						.streamTo(consumerToList1
								.transformWith(oneByOne())),
				streamSplitter.newOutput()
						.streamTo(badConsumer
								.transformWith(decorate(promise ->
										promise.then(item -> Promise.ofException(exception))))
								.transformWith(oneByOne())
						),
				streamSplitter.newOutput()
						.streamTo(consumerToList2
								.transformWith(oneByOne()))
		);

		assertSame(exception, e);
		// assertEquals(3, consumerToList1.getList().size());
		// assertEquals(3, consumerToList1.getList().size());
		// assertEquals(3, toBadList.size());

		assertClosedWithError(source);
		assertClosedWithError(streamSplitter.getInput());
		assertSuppliersClosedWithError(streamSplitter.getOutputs());
	}

	@Test
	public void testSupplierDisconnectWithError() {
		ExpectedException exception = new ExpectedException("Test Exception");
		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.of(1),
				StreamSupplier.of(2),
				StreamSupplier.of(3),
				StreamSupplier.closingWithError(exception)
		);

		StreamSplitter<Integer, Integer> splitter = StreamSplitter.create(
				(item, acceptors) -> {
					for (StreamDataAcceptor<Integer> acceptor : acceptors) {
						acceptor.accept(item);
					}
				});

		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer3 = StreamConsumerToList.create();

		Exception e = awaitException(
				source.streamTo(splitter.getInput()),
				splitter.newOutput().streamTo(consumer1.transformWith(oneByOne())),
				splitter.newOutput().streamTo(consumer2.transformWith(oneByOne())),
				splitter.newOutput().streamTo(consumer3.transformWith(oneByOne()))
		);

		assertSame(exception, e);
		assertEquals(3, consumer1.getList().size());
		assertEquals(3, consumer2.getList().size());
		assertEquals(3, consumer3.getList().size());

		assertClosedWithError(splitter.getInput());
		assertSuppliersClosedWithError(splitter.getOutputs());
	}

	@Test
	public void testNoOutputs() {
		StreamSplitter<Integer, Integer> splitter = StreamSplitter.create(
				(item, acceptors) -> {
					for (StreamDataAcceptor<Integer> acceptor : acceptors) {
						acceptor.accept(item);
					}
				});

		await(StreamSupplier.of(1, 2, 3, 4).streamTo(splitter.getInput()));
	}
}
