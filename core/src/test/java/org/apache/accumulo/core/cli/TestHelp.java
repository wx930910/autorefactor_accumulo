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
package org.apache.accumulo.core.cli;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

public class TestHelp {
	public Help mockHelp1() {
		Help mockInstance = Mockito.spy(Help.class);
		try {
			Mockito.doAnswer((stubInvo) -> {
				int status = stubInvo.getArgument(0);
				throw new RuntimeException(Integer.toString(status));
			}).when(mockInstance).exit(Mockito.anyInt());
			Mockito.doAnswer((stubInvo) -> {
				stubInvo.callRealMethod();
				return null;
			}).when(mockInstance).parseArgs(Mockito.any(String.class), Mockito.any(String[].class),
					Mockito.any(Object[].class));
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	@Test
	public void testInvalidArgs() {
		String[] args = { "foo" };
		Help help = mockHelp1();
		try {
			help.parseArgs("program", args);
		} catch (RuntimeException e) {
			assertEquals("1", e.getMessage());
		}
	}

}
