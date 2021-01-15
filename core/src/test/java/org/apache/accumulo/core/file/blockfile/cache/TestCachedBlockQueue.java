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
package org.apache.accumulo.core.file.blockfile.cache;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;

import org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock;
import org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlockQueue;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCachedBlockQueue {

	static public org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock mockCachedBlock1(long heapSize,
			String name, long accessTime) {
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock mockInstance = Mockito
				.spy(new org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock(name,
						new byte[(int) (heapSize - CachedBlock.PER_BLOCK_OVERHEAD)], accessTime, false));
		try {
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	@Test
	public void testQueue() {

		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb1 = TestCachedBlockQueue.mockCachedBlock1(1000,
				"cb1", 1);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb2 = TestCachedBlockQueue.mockCachedBlock1(1500,
				"cb2", 2);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb3 = TestCachedBlockQueue.mockCachedBlock1(1000,
				"cb3", 3);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb4 = TestCachedBlockQueue.mockCachedBlock1(1500,
				"cb4", 4);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb5 = TestCachedBlockQueue.mockCachedBlock1(1000,
				"cb5", 5);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb6 = TestCachedBlockQueue.mockCachedBlock1(1750,
				"cb6", 6);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb7 = TestCachedBlockQueue.mockCachedBlock1(1000,
				"cb7", 7);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb8 = TestCachedBlockQueue.mockCachedBlock1(1500,
				"cb8", 8);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb9 = TestCachedBlockQueue.mockCachedBlock1(1000,
				"cb9", 9);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb10 = TestCachedBlockQueue.mockCachedBlock1(1500,
				"cb10", 10);

		CachedBlockQueue queue = new CachedBlockQueue(10000, 1000);

		queue.add(cb1);
		queue.add(cb2);
		queue.add(cb3);
		queue.add(cb4);
		queue.add(cb5);
		queue.add(cb6);
		queue.add(cb7);
		queue.add(cb8);
		queue.add(cb9);
		queue.add(cb10);

		// We expect cb1 through cb8 to be in the queue
		long expectedSize = cb1.heapSize() + cb2.heapSize() + cb3.heapSize() + cb4.heapSize() + cb5.heapSize()
				+ cb6.heapSize() + cb7.heapSize() + cb8.heapSize();

		assertEquals(queue.heapSize(), expectedSize);

		LinkedList<org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock> blocks = queue.getList();
		assertEquals(blocks.poll().getName(), "cb1");
		assertEquals(blocks.poll().getName(), "cb2");
		assertEquals(blocks.poll().getName(), "cb3");
		assertEquals(blocks.poll().getName(), "cb4");
		assertEquals(blocks.poll().getName(), "cb5");
		assertEquals(blocks.poll().getName(), "cb6");
		assertEquals(blocks.poll().getName(), "cb7");
		assertEquals(blocks.poll().getName(), "cb8");

	}

	@Test
	public void testQueueSmallBlockEdgeCase() {

		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb1 = TestCachedBlockQueue.mockCachedBlock1(1000,
				"cb1", 1);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb2 = TestCachedBlockQueue.mockCachedBlock1(1500,
				"cb2", 2);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb3 = TestCachedBlockQueue.mockCachedBlock1(1000,
				"cb3", 3);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb4 = TestCachedBlockQueue.mockCachedBlock1(1500,
				"cb4", 4);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb5 = TestCachedBlockQueue.mockCachedBlock1(1000,
				"cb5", 5);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb6 = TestCachedBlockQueue.mockCachedBlock1(1750,
				"cb6", 6);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb7 = TestCachedBlockQueue.mockCachedBlock1(1000,
				"cb7", 7);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb8 = TestCachedBlockQueue.mockCachedBlock1(1500,
				"cb8", 8);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb9 = TestCachedBlockQueue.mockCachedBlock1(1000,
				"cb9", 9);
		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb10 = TestCachedBlockQueue.mockCachedBlock1(1500,
				"cb10", 10);

		CachedBlockQueue queue = new CachedBlockQueue(10000, 1000);

		queue.add(cb1);
		queue.add(cb2);
		queue.add(cb3);
		queue.add(cb4);
		queue.add(cb5);
		queue.add(cb6);
		queue.add(cb7);
		queue.add(cb8);
		queue.add(cb9);
		queue.add(cb10);

		org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock cb0 = TestCachedBlockQueue
				.mockCachedBlock1(10 + CachedBlock.PER_BLOCK_OVERHEAD, "cb0", 0);
		queue.add(cb0);

		// This is older so we must include it, but it will not end up kicking
		// anything out because (heapSize - cb8.heapSize + cb0.heapSize < maxSize)
		// and we must always maintain heapSize >= maxSize once we achieve it.

		// We expect cb0 through cb8 to be in the queue
		long expectedSize = cb1.heapSize() + cb2.heapSize() + cb3.heapSize() + cb4.heapSize() + cb5.heapSize()
				+ cb6.heapSize() + cb7.heapSize() + cb8.heapSize() + cb0.heapSize();

		assertEquals(queue.heapSize(), expectedSize);

		LinkedList<org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock> blocks = queue.getList();
		assertEquals(blocks.poll().getName(), "cb0");
		assertEquals(blocks.poll().getName(), "cb1");
		assertEquals(blocks.poll().getName(), "cb2");
		assertEquals(blocks.poll().getName(), "cb3");
		assertEquals(blocks.poll().getName(), "cb4");
		assertEquals(blocks.poll().getName(), "cb5");
		assertEquals(blocks.poll().getName(), "cb6");
		assertEquals(blocks.poll().getName(), "cb7");
		assertEquals(blocks.poll().getName(), "cb8");

	}
}
