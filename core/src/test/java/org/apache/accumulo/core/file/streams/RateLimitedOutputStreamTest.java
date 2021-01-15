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
package org.apache.accumulo.core.file.streams;

import static org.junit.Assert.assertEquals;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;

public class RateLimitedOutputStreamTest {

	@Test
	public void permitsAreProperlyAcquired() throws Exception {
		Random randGen = new SecureRandom();
		RateLimiter rateLimiter = Mockito.spy(RateLimiter.class);
		AtomicLong rateLimiterPermitsAcquired = new AtomicLong();
		try {
			Mockito.doAnswer((stubInvo) -> {
				long permits = stubInvo.getArgument(0);
				rateLimiterPermitsAcquired.addAndGet(permits);
				return null;
			}).when(rateLimiter).acquire(Mockito.anyLong());
		} catch (Exception exception) {
		}
		long bytesWritten = 0;
		try (RateLimitedOutputStream os = new RateLimitedOutputStream(new NullOutputStream(), rateLimiter)) {
			for (int i = 0; i < 100; ++i) {
				byte[] bytes = new byte[Math.abs(randGen.nextInt() % 65536)];
				os.write(bytes);
				bytesWritten += bytes.length;
			}
			assertEquals(bytesWritten, os.position());
		}
		assertEquals(bytesWritten, rateLimiterPermitsAcquired.get());
	}

	public static class NullOutputStream extends FSDataOutputStream {
		public NullOutputStream() {
			super(new CountingOutputStream(ByteStreams.nullOutputStream()), null);
		}
	}

}
