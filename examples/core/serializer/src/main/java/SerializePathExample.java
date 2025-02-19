import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;

import java.util.ArrayList;
import java.util.List;

public class SerializePathExample {

	//[START Storage]
	public static class Storage {
		@Serialize
		@SerializeNullable(path = 0)
		@SerializeNullable(path = {0, 1})
		public List<Nested<Integer, String>> listOfNested;
	}
	//[END Storage]

	//[START Nested]
	public static class Nested<T1, T2> {
		@Serialize
		public final T1 first;
		@Serialize
		public final T2 second;

		public Nested(@Deserialize("first") T1 first, @Deserialize("second") T2 second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public String toString() {
			return "Nested{" + first + ", " + second + '}';
		}
	}
	//[END Nested]

	public static void main(String[] args) {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create(Thread.currentThread().getContextClassLoader());
		//[START Serializer]
		BinarySerializer<Storage> serializer = SerializerBuilder.create(definingClassLoader)
				.withAnnotationCompatibilityMode() // Compatibility mode has to be enabled
				.build(Storage.class);
		//[END Serializer]

		Storage storage = new Storage();
		storage.listOfNested = new ArrayList<>();

		Nested<Integer, String> nested1 = new Nested<>(1, "abc");
		storage.listOfNested.add(nested1);

		storage.listOfNested.add(null);

		Nested<Integer, String> nested2 = new Nested<>(5, null);
		storage.listOfNested.add(nested2);

		byte[] buffer = new byte[200];
		serializer.encode(buffer, 0, storage);
		Storage deserializedStorage = serializer.decode(buffer, 0);

		System.out.println(storage.listOfNested);
		System.out.println(deserializedStorage.listOfNested);
	}
}
