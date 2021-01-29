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

import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.junit.Before;
import org.junit.Test;

/*
 This unit tests ClassLoaderContext and ExecuteHints functionality
 */
public class InputFormatBuilderTest {

	private class InputFormatBuilderImplTest<T> extends InputFormatBuilderImpl<T> {
		private String currentTable;
		private SortedMap<String, InputTableConfig> tableConfigMap = new TreeMap<>();
		private SortedMap<String, String> newHints = new TreeMap<>();

		private InputFormatBuilderImplTest(Class<T> callingClass) {
			super(callingClass);
		}

		@Override
		public InputFormatBuilder.InputFormatOptions<T> table(String tableName) {
			this.currentTable = tableName;
			tableConfigMap.put(currentTable, new InputTableConfig());
			return this;
		}

		@Override
		public InputFormatBuilder.InputFormatOptions<T> classLoaderContext(String context) {
			tableConfigMap.get(currentTable).setContext(context);
			return this;
		}

		private Optional<String> getClassLoaderContext() {
			return tableConfigMap.get(currentTable).getContext();
		}

		@Override
		public InputFormatBuilder.InputFormatOptions<T> executionHints(Map<String, String> hints) {
			this.newHints.putAll(hints);
			tableConfigMap.get(currentTable).setExecutionHints(hints);
			return this;
		}

	}

	private InputFormatBuilderImplTest<InputFormatBuilderTest> formatBuilderTest;

	@Before
	public void setUp() {
		formatBuilderTest = new InputFormatBuilderImplTest<>(InputFormatBuilderTest.class);
		formatBuilderTest.table("test");
	}

	@Test
	public void testInputFormatBuilderImplTest_ClassLoaderContext() {
		String context = "classLoaderContext";

		formatBuilderTest.classLoaderContext(context);

		Optional<String> classLoaderContextStr = formatBuilderTest.getClassLoaderContext();
		assertEquals(context, classLoaderContextStr.get());
	}

}
