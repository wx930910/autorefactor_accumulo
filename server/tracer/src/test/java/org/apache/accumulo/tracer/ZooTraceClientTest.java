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
package org.apache.accumulo.tracer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.easymock.EasyMock;
import org.junit.Test;
import org.mockito.Mockito;

public class ZooTraceClientTest {

	static public ZooTraceClient mockZooTraceClient1(AtomicBoolean done) {
		AtomicBoolean[] mockFieldVariableDone = new AtomicBoolean[1];
		ZooTraceClient mockInstance = Mockito.spy(ZooTraceClient.class);
		mockFieldVariableDone[0] = done;
		try {
			Mockito.doAnswer((stubInvo) -> {
				mockFieldVariableDone[0].set(true);
				return null;
			}).when(mockInstance).updateHostsFromZooKeeper();
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	@Test
	public void testConnectFailureRetries() throws Exception {
		ZooTraceClient client = EasyMock.createMockBuilder(ZooTraceClient.class)
				.addMockedMethod("updateHostsFromZooKeeper").createStrictMock();
		client.setRetryPause(0L);
		AtomicBoolean done = new AtomicBoolean(false);

		client.updateHostsFromZooKeeper();
		EasyMock.expectLastCall().andThrow(new RuntimeException()).once();
		client.updateHostsFromZooKeeper();
		// Expect the second call to updateHostsFromZooKeeper, but wait for it to fire
		// before
		// verification
		EasyMock.expectLastCall().andDelegateTo(ZooTraceClientTest.mockZooTraceClient1(done));

		EasyMock.replay(client);

		client.setInitialTraceHosts();

		while (!done.get()) {
			// The 2nd call to updateHostsFromZooKeeper is async. Wait for it for fire
			// before verifying it
			// was called.
			Thread.sleep(200);
		}

		EasyMock.verify(client);

	}

}
