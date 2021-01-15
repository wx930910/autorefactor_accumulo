/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.hadoopImpl.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/*
 This unit tests ClassLoaderContext and ExecuteHints functionality
 */
public class InputFormatBuilderTest {

	String[] formatBuilderTestCurrentTable = new String[1];

	SortedMap<String, String> formatBuilderTestNewHints = new TreeMap<>();

	SortedMap<String, InputTableConfig> formatBuilderTestTableConfigMap = new TreeMap<>();

	private InputTableConfig tableQueryConfig;
	private InputFormatBuilderImpl<InputFormatBuilderTest> formatBuilderTest;

	@Before
	public void setUp() {
		tableQueryConfig = new InputTableConfig();
		formatBuilderTest = Mockito.spy(new InputFormatBuilderImpl(InputFormatBuilderTest.class));
		try {
			Mockito.doAnswer((stubInvo) -> {
				Map<String, String> hints = stubInvo.getArgument(0);
				formatBuilderTestNewHints.putAll(hints);
				formatBuilderTestTableConfigMap.get(formatBuilderTestCurrentTable[0]).setExecutionHints(hints);
				return formatBuilderTest;
			}).when(formatBuilderTest).executionHints(Mockito.any());
			Mockito.doAnswer((stubInvo) -> {
				String tableName = stubInvo.getArgument(0);
				formatBuilderTestCurrentTable[0] = tableName;
				formatBuilderTestTableConfigMap.put(formatBuilderTestCurrentTable[0], new InputTableConfig());
				return formatBuilderTest;
			}).when(formatBuilderTest).table(Mockito.any());
			Mockito.doAnswer((stubInvo) -> {
				String context = stubInvo.getArgument(0);
				formatBuilderTestTableConfigMap.get(formatBuilderTestCurrentTable[0]).setContext(context);
				return formatBuilderTest;
			}).when(formatBuilderTest).classLoaderContext(Mockito.any());
		} catch (Exception exception) {
		}
		formatBuilderTest.table("test");
	}

	@Test
	public void testInputFormatBuilder_ClassLoaderContext() {
		String context = "classLoaderContext";

		InputFormatBuilderImpl<JobConf> formatBuilder = new InputFormatBuilderImpl<>(InputFormatBuilderTest.class);
		formatBuilder.table("test");
		formatBuilder.classLoaderContext(context);

		Optional<String> classLoaderContextStr = tableQueryConfig.getContext();
		assertTrue(classLoaderContextStr.toString().contains("empty")); // returns Optional.empty
	}

	@Test
	public void testInputFormatBuilderImplTest_ClassLoaderContext() {
		String context = "classLoaderContext";

		formatBuilderTest.classLoaderContext(context);

		Optional<String> classLoaderContextStr = formatBuilderTestTableConfigMap.get(formatBuilderTestCurrentTable[0])
				.getContext();
		assertEquals(context, classLoaderContextStr.get());
	}

	@Test
	public void testInputFormatBuilderImplTest_ExecuteHints() {
		SortedMap<String, String> hints = new TreeMap<>();
		hints.put("key1", "value1");
		hints.put("key2", "value2");
		hints.put("key3", "value3");

		formatBuilderTest.executionHints(hints);

		SortedMap<String, String> executionHints = formatBuilderTestNewHints;
		assertEquals(hints.toString(), executionHints.toString());
	}
}
