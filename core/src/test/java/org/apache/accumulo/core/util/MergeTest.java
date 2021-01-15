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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.Merge.Size;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.mockito.Mockito;

public class MergeTest {

	private static int[] sizes(List<Size> sizes) {
		int[] result = new int[sizes.size()];
		int i = 0;
		for (Size s : sizes) {
			result[i++] = (int) s.size;
		}
		return result;
	}

	@Test
	public void testMergomatic() throws Exception {
		// Merge everything to the last tablet
		int i;
		Merge test = Mockito.spy(Merge.class);
		List<Size>[] testTablets = new List[] { new ArrayList<>() };
		List<List<Size>>[] testMerges = new List[] { new ArrayList<>() };
		Integer[] sizes = new Integer[] { 10, 20, 30 };
		Text start = null;
		for (Integer size : sizes) {
			Text end;
			if (testTablets[0].size() == sizes.length - 1)
				end = null;
			else
				end = new Text(String.format("%05d", testTablets[0].size()));
			KeyExtent extent = new KeyExtent(TableId.of("table"), end, start);
			start = end;
			testTablets[0].add(new Size(extent, size));
		}
		try {
			Mockito.doAnswer((stubInvo) -> {
				Text startMockVariable = stubInvo.getArgument(2);
				Text end = stubInvo.getArgument(3);
				final Iterator<Size> impl = testTablets[0].iterator();
				return new Iterator<Size>() {
					Size next = skip();

					@Override
					public boolean hasNext() {
						return next != null;
					}

					private Size skip() {
						while (impl.hasNext()) {
							Size candidate = impl.next();
							if (startMockVariable != null) {
								if (candidate.extent.getEndRow() != null
										&& candidate.extent.getEndRow().compareTo(startMockVariable) < 0)
									continue;
							}
							if (end != null) {
								if (candidate.extent.getPrevEndRow() != null
										&& candidate.extent.getPrevEndRow().compareTo(end) >= 0)
									continue;
							}
							return candidate;
						}
						return null;
					}

					@Override
					public Size next() {
						Size result = next;
						next = skip();
						return result;
					}

					@Override
					public void remove() {
						impl.remove();
					}
				};
			}).when(test).getSizeIterator(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
			Mockito.doAnswer((stubInvo) -> {
				List<Size> sizesMockVariable = stubInvo.getArgument(2);
				int numToMerge = stubInvo.getArgument(3);
				List<Size> merge = new ArrayList<>();
				for (int iMockVariable = 0; iMockVariable < numToMerge; iMockVariable++) {
					merge.add(sizesMockVariable.get(iMockVariable));
				}
				testMerges[0].add(merge);
				return null;
			}).when(test).merge(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
			Mockito.doNothing().when(test).message(Mockito.any(), Mockito.any());
		} catch (Exception exception) {
		}
		test.mergomatic(null, "table", null, null, 1000, false);
		assertEquals(1, testMerges[0].size());
		assertArrayEquals(new int[] { 10, 20, 30 }, sizes(testMerges[0].get(i = 0)));
		testMerges[0] = new ArrayList<>();
		testTablets[0] = new ArrayList<>();
		start = null;
		sizes = new Integer[] { 1, 2, 100, 1000, 17, 1000, 4, 5, 6, 900 };
		for (Integer size : sizes) {
			Text end;
			if (testTablets[0].size() == sizes.length - 1)
				end = null;
			else
				end = new Text(String.format("%05d", testTablets[0].size()));
			KeyExtent extent = new KeyExtent(TableId.of("table"), end, start);
			start = end;
			testTablets[0].add(new Size(extent, size));
		}
		test.mergomatic(null, "table", null, null, 1000, false);
		assertEquals(2, testMerges[0].size());
		assertArrayEquals(new int[] { 1, 2, 100 }, sizes(testMerges[0].get(i = 0)));
		assertArrayEquals(new int[] { 4, 5, 6, 900 }, sizes(testMerges[0].get(++i)));

		testMerges[0] = new ArrayList<>();
		testTablets[0] = new ArrayList<>();
		start = null;
		sizes = new Integer[] { 1, 2, 100, 1000, 17, 1000, 4, 5, 6, 900 };
		for (Integer size : sizes) {
			Text end;
			if (testTablets[0].size() == sizes.length - 1)
				end = null;
			else
				end = new Text(String.format("%05d", testTablets[0].size()));
			KeyExtent extent = new KeyExtent(TableId.of("table"), end, start);
			start = end;
			testTablets[0].add(new Size(extent, size));
		}
		test.mergomatic(null, "table", null, null, 1000, true);
		assertEquals(3, testMerges[0].size());
		assertArrayEquals(new int[] { 1, 2, 100 }, sizes(testMerges[0].get(i = 0)));
		assertArrayEquals(new int[] { 17, 1000 }, sizes(testMerges[0].get(++i)));
		assertArrayEquals(new int[] { 4, 5, 6, 900 }, sizes(testMerges[0].get(++i)));

		testMerges[0] = new ArrayList<>();
		testTablets[0] = new ArrayList<>();
		start = null;
		sizes = new Integer[] { 1, 2, 1000, 17, 1000, 4, 5, 6, 900 };
		for (Integer size : sizes) {
			Text end;
			if (testTablets[0].size() == sizes.length - 1)
				end = null;
			else
				end = new Text(String.format("%05d", testTablets[0].size()));
			KeyExtent extent = new KeyExtent(TableId.of("table"), end, start);
			start = end;
			testTablets[0].add(new Size(extent, size));
		}
		test.mergomatic(null, "table", new Text("00004"), null, 1000, false);
		assertEquals(1, testMerges[0].size());
		assertArrayEquals(new int[] { 4, 5, 6, 900 }, sizes(testMerges[0].get(i = 0)));

		testMerges[0] = new ArrayList<>();
		testTablets[0] = new ArrayList<>();
		start = null;
		sizes = new Integer[] { 1, 2, 1000, 17, 1000, 4, 5, 6, 900 };
		for (Integer size : sizes) {
			Text end;
			if (testTablets[0].size() == sizes.length - 1)
				end = null;
			else
				end = new Text(String.format("%05d", testTablets[0].size()));
			KeyExtent extent = new KeyExtent(TableId.of("table"), end, start);
			start = end;
			testTablets[0].add(new Size(extent, size));
		}
		test.mergomatic(null, "table", null, new Text("00004"), 1000, false);
		assertEquals(1, testMerges[0].size());
		assertArrayEquals(new int[] { 1, 2 }, sizes(testMerges[0].get(i = 0)));

		testMerges[0] = new ArrayList<>();
		testTablets[0] = new ArrayList<>();
		start = null;
		sizes = new Integer[] { 1, 2, 1000, 17, 1000, 4, 5, 6, 900 };
		for (Integer size : sizes) {
			Text end;
			if (testTablets[0].size() == sizes.length - 1)
				end = null;
			else
				end = new Text(String.format("%05d", testTablets[0].size()));
			KeyExtent extent = new KeyExtent(TableId.of("table"), end, start);
			start = end;
			testTablets[0].add(new Size(extent, size));
		}
		test.mergomatic(null, "table", new Text("00002"), new Text("00004"), 1000, true);
		assertEquals(1, testMerges[0].size());
		assertArrayEquals(new int[] { 17, 1000 }, sizes(testMerges[0].get(i = 0)));

		testMerges[0] = new ArrayList<>();
		testTablets[0] = new ArrayList<>();
		start = null;
		sizes = new Integer[] { 100, 250, 500, 600, 100, 200, 500, 200 };
		for (Integer size : sizes) {
			Text end;
			if (testTablets[0].size() == sizes.length - 1)
				end = null;
			else
				end = new Text(String.format("%05d", testTablets[0].size()));
			KeyExtent extent = new KeyExtent(TableId.of("table"), end, start);
			start = end;
			testTablets[0].add(new Size(extent, size));
		}
		test.mergomatic(null, "table", null, null, 1000, false);
		assertEquals(3, testMerges[0].size());
		assertArrayEquals(new int[] { 100, 250, 500 }, sizes(testMerges[0].get(i = 0)));
		assertArrayEquals(new int[] { 600, 100, 200 }, sizes(testMerges[0].get(++i)));
		assertArrayEquals(new int[] { 500, 200 }, sizes(testMerges[0].get(++i)));
	}

}
