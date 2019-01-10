package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Animal;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Cat;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Dog;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Parrot;

import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Tests migrations for {@link KryoSerializerSnapshot}.
 */
@SuppressWarnings("WeakerAccess")
@RunWith(Parameterized.class)
public class KryoSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<KryoPojosForMigrationTests> {

	public KryoSnapshotMigrationTest(TestSpecification<KryoPojosForMigrationTests> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<Object[]> testSpecifications() {

		final TestSpecification<Animal> genericCase = TestSpecification.<Animal>builder("1.6-kryo: empty config -> empty config", KryoSerializer.class, KryoSerializerSnapshot.class)
			.withSerializerProvider(() -> new KryoSerializer<>(Animal.class, new ExecutionConfig()))
			.withSnapshotDataLocation("flink-1.6-kryo-type-serializer-empty-config-snapshot")
			.withTestData("flink-1.6-kryo-type-serializer-empty-config-data", 2);

		final TestSpecification<Animal> moreRegistrations = TestSpecification.<Animal>builder("1.6-kryo: empty config -> new classes", KryoSerializer.class, KryoSerializerSnapshot.class)
			.withSerializerProvider(() -> {
				ExecutionConfig executionConfig = new ExecutionConfig();
				executionConfig.registerKryoType(DummyClassOne.class);
				executionConfig.registerTypeWithKryoSerializer(DummyClassTwo.class, StringSerializer.class);

				return new KryoSerializer<>(Animal.class, executionConfig);
			})
			.withSnapshotDataLocation("flink-1.6-kryo-type-serializer-empty-config-snapshot")
			.withTestData("flink-1.6-kryo-type-serializer-empty-config-data", 2);

		final TestSpecification<Animal> dfsf = TestSpecification.<Animal>builder("1.6-kryo: registered classes in a different order", KryoSerializer.class, KryoSerializerSnapshot.class)
			.withSerializerProvider(() -> {

				ExecutionConfig executionConfig = new ExecutionConfig();
				executionConfig.registerKryoType(DummyClassOne.class);
				executionConfig.registerKryoType(Dog.class);
				executionConfig.registerKryoType(DummyClassTwo.class);
				executionConfig.registerKryoType(Cat.class);
				executionConfig.registerKryoType(Parrot.class);

				return new KryoSerializer<>(Animal.class, executionConfig);
			})
			.withSnapshotDataLocation("flink-1.6-kryo-type-serializer-snapshot")
			.withTestData("flink-1.6-kryo-type-serializer-data", 2);

		return Arrays.asList(
			new Object[]{genericCase},
			new Object[]{moreRegistrations},
			new Object[]{dfsf}
		);
	}

	public static final class DummyClassOne {

	}

	public static final class DummyClassTwo {

	}
}
