/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.formats.avro.utils.TestDataGenerator;

import org.junit.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.Random;

/**
 * Tests for the {@link AvroSerializer} that test specific avro types.
 */
public class AvroSerializerTest extends SerializerTestBase<User> {

	@Override
	protected TypeSerializer<User> createSerializer() {
		return new AvroSerializer<>(User.class);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<User> getTypeClass() {
		return User.class;
	}

	@Override
	protected User[] getTestData() {
		final Random rnd = new Random();
		final User[] users = new User[20];

		for (int i = 0; i < users.length; i++) {
			users[i] = TestDataGenerator.generateRandomUser(rnd);
		}

		return users;
	}

	@Test
	public void avro16specifc() throws IOException {
		String flink16 = "AAAAAQAAAUis7QAFc3IANm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJv\n" +
			"U2VyaWFsaXplcgAAAAAAAAABAgACTAAMc2NoZW1hU3RyaW5ndAASTGphdmEvbGFuZy9TdHJpbmc7TAAE\n" +
			"dHlwZXQAEUxqYXZhL2xhbmcvQ2xhc3M7eHIANG9yZy5hcGFjaGUuZmxpbmsuYXBpLmNvbW1vbi50eXBl\n" +
			"dXRpbHMuVHlwZVNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhwcHZyAC9vcmcuYXBhY2hlLmZsaW5rLmZvcm1h\n" +
			"dHMuYXZyby5nZW5lcmF0ZWQuQWRkcmVzc+z2o/io4ENqDAAAeHIAK29yZy5hcGFjaGUuYXZyby5zcGVj\n" +
			"aWZpYy5TcGVjaWZpY1JlY29yZEJhc2UCovmsxrc0HQwAAHhw";

		byte[] bytes = Base64.getMimeDecoder().decode(flink16);
		System.out.println(bytes.length);

		DataInputView in = new DataInputDeserializer(bytes);
		TypeSerializer<Address> avro = TypeSerializerSerializationUtil.tryReadSerializer(in, Thread.currentThread().getContextClassLoader());

		TypeSerializerSnapshot<Address> x = avro.snapshotConfiguration();
	}

	@Test
	public void avro16generic() throws IOException {
		String flink16 = "AAAAAQAAAges7QAFc3IANm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJv\n" +
			"U2VyaWFsaXplcgAAAAAAAAABAgACTAAMc2NoZW1hU3RyaW5ndAASTGphdmEvbGFuZy9TdHJpbmc7TAAE\n" +
			"dHlwZXQAEUxqYXZhL2xhbmcvQ2xhc3M7eHIANG9yZy5hcGFjaGUuZmxpbmsuYXBpLmNvbW1vbi50eXBl\n" +
			"dXRpbHMuVHlwZVNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhwdAEBeyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6\n" +
			"IkFkZHJlc3MiLCJuYW1lc3BhY2UiOiJvcmcuYXBhY2hlLmZsaW5rLmZvcm1hdHMuYXZyby5nZW5lcmF0\n" +
			"ZWQiLCJmaWVsZHMiOlt7Im5hbWUiOiJudW0iLCJ0eXBlIjoiaW50In0seyJuYW1lIjoic3RyZWV0Iiwi\n" +
			"dHlwZSI6InN0cmluZyJ9LHsibmFtZSI6ImNpdHkiLCJ0eXBlIjoic3RyaW5nIn0seyJuYW1lIjoic3Rh\n" +
			"dGUiLCJ0eXBlIjoic3RyaW5nIn0seyJuYW1lIjoiemlwIiwidHlwZSI6InN0cmluZyJ9XX12cgAlb3Jn\n" +
			"LmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY1JlY29yZAAAAAAAAAAAAAAAeHA=";

		byte[] bytes = Base64.getMimeDecoder().decode(flink16);
		System.out.println(bytes.length);

		DataInputView in = new DataInputDeserializer(bytes);
		TypeSerializer<Address> avro = TypeSerializerSerializationUtil.tryReadSerializer(in, Thread.currentThread().getContextClassLoader());

		TypeSerializerSnapshot<?> x = avro.snapshotConfiguration();
		x.restoreSerializer();
	}

	@Test
	public void avro17() throws IOException {
		String flink17 = "AAAAAQAAAeKs7QAFc3IANm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJv\n" +
			"U2VyaWFsaXplcgAAAAAAAAACAgADTAAOcHJldmlvdXNTY2hlbWF0AEBMb3JnL2FwYWNoZS9mbGluay9m\n" +
			"b3JtYXRzL2F2cm8vdHlwZXV0aWxzL1NlcmlhbGl6YWJsZUF2cm9TY2hlbWE7TAAGc2NoZW1hcQB+AAFM\n" +
			"AAR0eXBldAARTGphdmEvbGFuZy9DbGFzczt4cgA0b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5\n" +
			"cGV1dGlscy5UeXBlU2VyaWFsaXplcgAAAAAAAAABAgAAeHBzcgA+b3JnLmFwYWNoZS5mbGluay5mb3Jt\n" +
			"YXRzLmF2cm8udHlwZXV0aWxzLlNlcmlhbGl6YWJsZUF2cm9TY2hlbWEAAAAAAAAAAQMAAHhwdwEAeHNx\n" +
			"AH4ABXcBAHh2cgAvb3JnLmFwYWNoZS5mbGluay5mb3JtYXRzLmF2cm8uZ2VuZXJhdGVkLkFkZHJlc3Ps\n" +
			"9qP4qOBDagwAAHhyACtvcmcuYXBhY2hlLmF2cm8uc3BlY2lmaWMuU3BlY2lmaWNSZWNvcmRCYXNlAqL5\n" +
			"rMa3NB0MAAB4cA==";

		byte[] bytes = Base64.getMimeDecoder().decode(flink17);
		System.out.println(bytes.length);

		DataInputView in = new DataInputDeserializer(bytes);
		TypeSerializer<?> avro = TypeSerializerSerializationUtil.tryReadSerializer(in, Thread.currentThread().getContextClassLoader());
		avro.getLength();
	}

	@Test
	public void avro17Initialized() throws IOException {
		String flink17init = "AAAAAQAAAeKs7QAFc3IANm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJv\n" +
			"U2VyaWFsaXplcgAAAAAAAAACAgADTAAOcHJldmlvdXNTY2hlbWF0AEBMb3JnL2FwYWNoZS9mbGluay9m\n" +
			"b3JtYXRzL2F2cm8vdHlwZXV0aWxzL1NlcmlhbGl6YWJsZUF2cm9TY2hlbWE7TAAGc2NoZW1hcQB+AAFM\n" +
			"AAR0eXBldAARTGphdmEvbGFuZy9DbGFzczt4cgA0b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5\n" +
			"cGV1dGlscy5UeXBlU2VyaWFsaXplcgAAAAAAAAABAgAAeHBzcgA+b3JnLmFwYWNoZS5mbGluay5mb3Jt\n" +
			"YXRzLmF2cm8udHlwZXV0aWxzLlNlcmlhbGl6YWJsZUF2cm9TY2hlbWEAAAAAAAAAAQMAAHhwdwEAeHNx\n" +
			"AH4ABXcBAHh2cgAvb3JnLmFwYWNoZS5mbGluay5mb3JtYXRzLmF2cm8uZ2VuZXJhdGVkLkFkZHJlc3Ps\n" +
			"9qP4qOBDagwAAHhyACtvcmcuYXBhY2hlLmF2cm8uc3BlY2lmaWMuU3BlY2lmaWNSZWNvcmRCYXNlAqL5\n" +
			"rMa3NB0MAAB4cA==";

		byte[] bytes = Base64.getMimeDecoder().decode(flink17init);
		System.out.println(bytes.length);

		DataInputView in = new DataInputDeserializer(bytes);
		TypeSerializer<Address> avro = TypeSerializerSerializationUtil.tryReadSerializer(in, Thread.currentThread().getContextClassLoader());
		avro.getLength();
	}

	@Test
	public void streamSerializerWithNestedAvro() throws IOException {
		String flink17init = "AAAAAQAAAq2s7QAFc3IAR29yZy5hcGFjaGUuZmxpbmsuc3RyZWFtaW5nLnJ1bnRpbWUuc3RyZWFtcmVj\n" +
			"b3JkLlN0cmVhbUVsZW1lbnRTZXJpYWxpemVyAAAAAAAAAAECAAFMAA50eXBlU2VyaWFsaXplcnQANkxv\n" +
			"cmcvYXBhY2hlL2ZsaW5rL2FwaS9jb21tb24vdHlwZXV0aWxzL1R5cGVTZXJpYWxpemVyO3hyADRvcmcu\n" +
			"YXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZXV0aWxzLlR5cGVTZXJpYWxpemVyAAAAAAAAAAECAAB4\n" +
			"cHNyADZvcmcuYXBhY2hlLmZsaW5rLmZvcm1hdHMuYXZyby50eXBldXRpbHMuQXZyb1NlcmlhbGl6ZXIA\n" +
			"AAAAAAAAAQIAAkwADHNjaGVtYVN0cmluZ3QAEkxqYXZhL2xhbmcvU3RyaW5nO0wABHR5cGV0ABFMamF2\n" +
			"YS9sYW5nL0NsYXNzO3hxAH4AAnQBAXsidHlwZSI6InJlY29yZCIsIm5hbWUiOiJBZGRyZXNzIiwibmFt\n" +
			"ZXNwYWNlIjoib3JnLmFwYWNoZS5mbGluay5mb3JtYXRzLmF2cm8uZ2VuZXJhdGVkIiwiZmllbGRzIjpb\n" +
			"eyJuYW1lIjoibnVtIiwidHlwZSI6ImludCJ9LHsibmFtZSI6InN0cmVldCIsInR5cGUiOiJzdHJpbmci\n" +
			"fSx7Im5hbWUiOiJjaXR5IiwidHlwZSI6InN0cmluZyJ9LHsibmFtZSI6InN0YXRlIiwidHlwZSI6InN0\n" +
			"cmluZyJ9LHsibmFtZSI6InppcCIsInR5cGUiOiJzdHJpbmcifV19dnIAJW9yZy5hcGFjaGUuYXZyby5n\n" +
			"ZW5lcmljLkdlbmVyaWNSZWNvcmQAAAAAAAAAAAAAAHhw";

		byte[] bytes = Base64.getMimeDecoder().decode(flink17init);
		System.out.println(bytes.length);

		DataInputView in = new DataInputDeserializer(bytes);
		TypeSerializer<?> ser = TypeSerializerSerializationUtil.tryReadSerializer(in, Thread.currentThread().getContextClassLoader());
		ser.getLength();
		TypeSerializerSnapshot<?> x = ser.snapshotConfiguration();
		x.getCurrentVersion();
	}
}
