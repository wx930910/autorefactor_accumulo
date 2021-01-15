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
package org.apache.accumulo.core.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ValidatorTest {
	static public Validator<String> mockValidator2(String s) {
		String[] mockFieldVariablePs = new String[1];
		Validator<String> mockInstance = Mockito.spy(Validator.class);
		mockFieldVariablePs[0] = s;
		try {
			Mockito.doAnswer((stubInvo) -> {
				String argument = stubInvo.getArgument(0);
				return (argument != null && argument.matches(mockFieldVariablePs[0]));
			}).when(mockInstance).test(Mockito.any());
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	static public Validator<String> mockValidator1(String s) {
		String[] mockFieldVariableS = new String[1];
		Validator<String> mockInstance = Mockito.spy(Validator.class);
		mockFieldVariableS[0] = s;
		try {
			Mockito.doAnswer((stubInvo) -> {
				String argument = stubInvo.getArgument(0);
				return mockFieldVariableS[0].equals(argument);
			}).when(mockInstance).test(Mockito.any());
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	private Validator<String> v, v2, v3;

	@Before
	public void setUp() {
		v = ValidatorTest.mockValidator1("correct");
		v2 = ValidatorTest.mockValidator1("righto");
		v3 = ValidatorTest.mockValidator2("c.*");
	}

	@Test
	public void testValidate_Success() {
		assertEquals("correct", v.validate("correct"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidate_Failure() {
		v.validate("incorrect");
	}

	@Test
	public void testInvalidMessage() {
		assertEquals("Invalid argument incorrect", v.invalidMessage("incorrect"));
	}

	@Test
	public void testAnd() {
		Validator<String> vand = v3.and(v);
		assertTrue(vand.test("correct"));
		assertFalse(vand.test("righto"));
		assertFalse(vand.test("coriander"));
	}

	@Test
	public void testOr() {
		Validator<String> vor = v.or(v2);
		assertTrue(vor.test("correct"));
		assertTrue(vor.test("righto"));
		assertFalse(vor.test("coriander"));
	}

	@Test
	public void testNot() {
		Validator<String> vnot = v3.not();
		assertFalse(vnot.test("correct"));
		assertFalse(vnot.test("coriander"));
		assertTrue(vnot.test("righto"));
	}
}
