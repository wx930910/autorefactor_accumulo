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
package org.apache.accumulo.core.clientImpl.bulk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.util.concurrent.Uninterruptibles;

public class ConcurrentKeyExtentCacheTest {

	static public ConcurrentKeyExtentCache mockConcurrentKeyExtentCache1() {
		ConcurrentSkipListSet<KeyExtent> mockFieldVariableSeen = new ConcurrentSkipListSet<>();
		ConcurrentKeyExtentCache mockInstance = Mockito.spy(new ConcurrentKeyExtentCache(null, null));
		try {
			Mockito.doAnswer((stubInvo) -> {
				Text row = stubInvo.getArgument(0);
				int index = -1;
				for (int i = 0; i < extents.size(); i++) {
					if (extents.get(i).contains(row)) {
						index = i;
						break;
					}
				}
				Uninterruptibles.sleepUninterruptibly(3, TimeUnit.MILLISECONDS);
				return extents.subList(index, extents.size()).stream().limit(73);
			}).when(mockInstance).lookupExtents(Mockito.any(Text.class));
			Mockito.doAnswer((stubInvo) -> {
				KeyExtent e = stubInvo.getArgument(0);
				stubInvo.callRealMethod();
				assertTrue(mockFieldVariableSeen.add(e));
				return null;
			}).when(mockInstance).updateCache(Mockito.any(KeyExtent.class));
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	private static List<KeyExtent> extents = new ArrayList<>();
	private static Set<KeyExtent> extentsSet = new HashSet<>();

	@BeforeClass
	public static void setupSplits() {
		Text prev = null;
		for (int i = 1; i < 255; i++) {
			Text endRow = new Text(String.format("%02x", i));
			extents.add(new KeyExtent(TableId.of("1"), endRow, prev));
			prev = endRow;
		}

		extents.add(new KeyExtent(TableId.of("1"), null, prev));

		extentsSet.addAll(extents);
	}

	private void testLookup(ConcurrentKeyExtentCache tc, Text lookupRow) {
		try {
			KeyExtent extent = tc.lookup(lookupRow);
			assertTrue(extent.contains(lookupRow));
			assertTrue(extentsSet.contains(extent));
		} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void testExactEndRows() {
		Random rand = new SecureRandom();

		ConcurrentKeyExtentCache tc = Mockito.spy(new ConcurrentKeyExtentCache(null, null));
		ConcurrentSkipListSet<KeyExtent> tcSeen = new ConcurrentSkipListSet<>();
		try {
			Mockito.doAnswer((stubInvo) -> {
				Text row = stubInvo.getArgument(0);
				int index = -1;
				for (int i = 0; i < extents.size(); i++) {
					if (extents.get(i).contains(row)) {
						index = i;
						break;
					}
				}
				Uninterruptibles.sleepUninterruptibly(3, TimeUnit.MILLISECONDS);
				return extents.subList(index, extents.size()).stream().limit(73);
			}).when(tc).lookupExtents(Mockito.any(Text.class));
			Mockito.doAnswer((stubInvo) -> {
				KeyExtent e = stubInvo.getArgument(0);
				stubInvo.callRealMethod();
				assertTrue(tcSeen.add(e));
				return null;
			}).when(tc).updateCache(Mockito.any(KeyExtent.class));
		} catch (Exception exception) {
		}

		rand.ints(20000, 0, 256).mapToObj(i -> new Text(String.format("%02x", i))).sequential()
				.forEach(lookupRow -> testLookup(tc, lookupRow));
		assertEquals(extentsSet, tcSeen);

		// try parallel
		ConcurrentKeyExtentCache tc2 = ConcurrentKeyExtentCacheTest.mockConcurrentKeyExtentCache1();
		rand.ints(20000, 0, 256).mapToObj(i -> new Text(String.format("%02x", i))).parallel()
				.forEach(lookupRow -> testLookup(tc2, lookupRow));
		assertEquals(extentsSet, tcSeen);
	}

	@Test
	public void testRandom() {
		ConcurrentKeyExtentCache tc = Mockito.spy(new ConcurrentKeyExtentCache(null, null));
		ConcurrentSkipListSet<KeyExtent> tcSeen = new ConcurrentSkipListSet<>();
		try {
			Mockito.doAnswer((stubInvo) -> {
				Text row = stubInvo.getArgument(0);
				int index = -1;
				for (int i = 0; i < extents.size(); i++) {
					if (extents.get(i).contains(row)) {
						index = i;
						break;
					}
				}
				Uninterruptibles.sleepUninterruptibly(3, TimeUnit.MILLISECONDS);
				return extents.subList(index, extents.size()).stream().limit(73);
			}).when(tc).lookupExtents(Mockito.any(Text.class));
			Mockito.doAnswer((stubInvo) -> {
				KeyExtent e = stubInvo.getArgument(0);
				stubInvo.callRealMethod();
				assertTrue(tcSeen.add(e));
				return null;
			}).when(tc).updateCache(Mockito.any(KeyExtent.class));
		} catch (Exception exception) {
		}

		Random rand = new SecureRandom();
		rand.ints(20000).mapToObj(i -> new Text(String.format("%08x", i))).sequential()
				.forEach(lookupRow -> testLookup(tc, lookupRow));
		assertEquals(extentsSet, tcSeen);

		// try parallel
		ConcurrentKeyExtentCache tc2 = Mockito.spy(new ConcurrentKeyExtentCache(null, null));
		ConcurrentSkipListSet<KeyExtent> tc2Seen = new ConcurrentSkipListSet<>();
		try {
			Mockito.doAnswer((stubInvo) -> {
				Text row = stubInvo.getArgument(0);
				int index = -1;
				for (int i = 0; i < extents.size(); i++) {
					if (extents.get(i).contains(row)) {
						index = i;
						break;
					}
				}
				Uninterruptibles.sleepUninterruptibly(3, TimeUnit.MILLISECONDS);
				return extents.subList(index, extents.size()).stream().limit(73);
			}).when(tc2).lookupExtents(Mockito.any(Text.class));
			Mockito.doAnswer((stubInvo) -> {
				KeyExtent e = stubInvo.getArgument(0);
				stubInvo.callRealMethod();
				assertTrue(tc2Seen.add(e));
				return null;
			}).when(tc2).updateCache(Mockito.any(KeyExtent.class));
		} catch (Exception exception) {
		}
		rand.ints(20000).mapToObj(i -> new Text(String.format("%08x", i))).parallel()
				.forEach(lookupRow -> testLookup(tc2, lookupRow));
		assertEquals(extentsSet, tc2Seen);
	}
}
