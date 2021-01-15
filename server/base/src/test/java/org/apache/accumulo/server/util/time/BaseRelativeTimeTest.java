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
package org.apache.accumulo.server.util.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.mockito.Mockito;

public class BaseRelativeTimeTest {

	@Test
	public void testMatchesTime() {
		ProvidesTime bt = Mockito.spy(ProvidesTime.class);
		long[] btValue = new long[] { 0 };
		try {
			Mockito.doAnswer((stubInvo) -> {
				return btValue[0];
			}).when(bt).currentTime();
		} catch (Exception exception) {
		}
		ProvidesTime now = Mockito.spy(ProvidesTime.class);
		long[] nowValue = new long[] { 0 };
		try {
			Mockito.doAnswer((stubInvo) -> {
				return nowValue[0];
			}).when(now).currentTime();
		} catch (Exception exception) {
		}
		nowValue[0] = btValue[0] = System.currentTimeMillis();

		BaseRelativeTime brt = new BaseRelativeTime(now);
		assertEquals(brt.currentTime(), nowValue[0]);
		brt.updateTime(nowValue[0]);
		assertEquals(brt.currentTime(), nowValue[0]);
	}

	@Test
	public void testFutureTime() {
		ProvidesTime advice = Mockito.spy(ProvidesTime.class);
		long[] adviceValue = new long[] { 0 };
		try {
			Mockito.doAnswer((stubInvo) -> {
				return adviceValue[0];
			}).when(advice).currentTime();
		} catch (Exception exception) {
		}
		ProvidesTime local = Mockito.spy(ProvidesTime.class);
		long[] localValue = new long[] { 0 };
		try {
			Mockito.doAnswer((stubInvo) -> {
				return localValue[0];
			}).when(local).currentTime();
		} catch (Exception exception) {
		}
		localValue[0] = adviceValue[0] = System.currentTimeMillis();
		// Ten seconds into the future
		adviceValue[0] += 10000;

		BaseRelativeTime brt = new BaseRelativeTime(local);
		assertEquals(brt.currentTime(), localValue[0]);
		brt.updateTime(adviceValue[0]);
		long once = brt.currentTime();
		assertTrue(once < adviceValue[0]);
		assertTrue(once > localValue[0]);

		for (int i = 0; i < 100; i++) {
			brt.updateTime(adviceValue[0]);
		}
		long many = brt.currentTime();
		assertTrue(many > once);
		assertTrue("after much advice, relative time is still closer to local time",
				(adviceValue[0] - many) < (once - localValue[0]));
	}

	@Test
	public void testPastTime() {
		ProvidesTime advice = Mockito.spy(ProvidesTime.class);
		long[] adviceValue = new long[] { 0 };
		try {
			Mockito.doAnswer((stubInvo) -> {
				return adviceValue[0];
			}).when(advice).currentTime();
		} catch (Exception exception) {
		}
		ProvidesTime local = Mockito.spy(ProvidesTime.class);
		long[] localValue = new long[] { 0 };
		try {
			Mockito.doAnswer((stubInvo) -> {
				return localValue[0];
			}).when(local).currentTime();
		} catch (Exception exception) {
		}
		localValue[0] = adviceValue[0] = System.currentTimeMillis();
		// Ten seconds into the past
		adviceValue[0] -= 10000;

		BaseRelativeTime brt = new BaseRelativeTime(local);
		brt.updateTime(adviceValue[0]);
		long once = brt.currentTime();
		assertTrue(once < localValue[0]);
		brt.updateTime(adviceValue[0]);
		long twice = brt.currentTime();
		assertTrue("Time cannot go backwards", once <= twice);
		brt.updateTime(adviceValue[0] - 10000);
		assertTrue("Time cannot go backwards", once <= twice);
	}

}
