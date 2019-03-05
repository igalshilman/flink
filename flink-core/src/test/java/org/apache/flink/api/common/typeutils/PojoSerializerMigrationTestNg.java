package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.ClassRelocator.TestClass;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

import static org.hamcrest.MatcherAssert.assertThat;

public class ClassRelocatorTest {

	public interface TestDataWriter {
		void produceTestData(DataOutputView snapshot, DataOutputView data) throws IOException;
	}

	public interface TestDataReader {
		void read(DataInputView snapshot, ClassLoader userClassLoader, DataInputView data) throws IOException;
	}

	/**
	 * A writer would produce some test data.
	 */
	public static class Writer implements TestDataWriter {


		@TestClass("MyPojo")
		public static class PojoWithTwoFields implements Serializable {

			private static final long serialVersionUID = 1;

			int age;
			String name;

			public PojoWithTwoFields(int age, String name) {
				this.age = age;
				this.name = name;
			}

			public PojoWithTwoFields() {
			}

			public int getAge() {
				return age;
			}

			public void setAge(int age) {
				this.age = age;
			}

			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}
		}

		@Override
		public void produceTestData(DataOutputView snapshot, DataOutputView data) throws IOException {
			TypeSerializer<PojoWithTwoFields> serializer = TypeExtractor.createTypeInfo(PojoWithTwoFields.class)
				.createSerializer(new ExecutionConfig());

			serializer.serialize(new PojoWithTwoFields(1, "hello"), data);
			TypeSerializerSnapshot.writeVersionedSnapshot(snapshot, serializer.snapshotConfiguration());
		}
	}

	public static class Reader implements TestDataReader {

		@TestClass("MyPojo")
		public static class PojoWithOneField implements Serializable {

			private static final long serialVersionUID = 1;

			int age;

			public PojoWithOneField(int age) {
				this.age = age;
			}

			public PojoWithOneField() {
			}

			public int getAge() {
				return age;
			}

			public void setAge(int age) {
				this.age = age;
			}

			@Override
			public String toString() {
				return "MyPojo{" +
					"age=" + age +
					'}';
			}
		}

		@Override
		public void read(DataInputView snapshotIn, ClassLoader userClassLoader, DataInputView dataIn) throws IOException {
			TypeSerializerSnapshot<PojoWithOneField> snapshot = TypeSerializerSnapshot.readVersionedSnapshot(snapshotIn, userClassLoader);

			TypeSerializer<PojoWithOneField> newSerializer = TypeExtractor.createTypeInfo(PojoWithOneField.class)
				.createSerializer(new ExecutionConfig());

			assertThat(snapshot.resolveSchemaCompatibility(newSerializer),
				TypeSerializerMatchers.isCompatibleAfterMigration());

			PojoWithOneField elem = newSerializer.deserialize(dataIn);

			System.out.println(elem.getClass().getName() + " -> " + elem);
		}
	}

	@Test
	public void example() throws IOException, IllegalAccessException, InstantiationException {
		Class<? extends TestDataWriter> writerClass = ClassRelocator.relocate(Writer.class);

		// write some data out
		DataOutputSerializer snapshotOut = new DataOutputSerializer(4096);
		DataOutputSerializer dataOut = new DataOutputSerializer(4096);
		writerClass.newInstance().produceTestData(snapshotOut, dataOut);

		// read the data back
		Class<? extends TestDataReader> readerClass = ClassRelocator.relocate(Reader.class);
		DataInputView snapshotIn = new DataInputDeserializer(snapshotOut.getCopyOfBuffer());
		DataInputView dataIn = new DataInputDeserializer(dataOut.getCopyOfBuffer());
		readerClass.newInstance().read(snapshotIn, readerClass.getClassLoader(), dataIn);
	}

}
