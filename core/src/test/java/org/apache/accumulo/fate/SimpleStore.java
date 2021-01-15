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
package org.apache.accumulo.fate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.mockito.Mockito;

/**
 * Transient in memory store for transactions.
 */
public class SimpleStore {

	static public TStore<String> mockTStore1() {
		long[] mockFieldVariableNextId = new long[] { 1 };
		Set<Long> mockFieldVariableReserved = new HashSet<>();
		Map<Long, TStatus> mockFieldVariableStatuses = new HashMap<>();
		TStore<String> mockInstance = Mockito.spy(TStore.class);
		try {
			Mockito.doAnswer((stubInvo) -> {
				long tid = stubInvo.getArgument(0);
				if (!mockFieldVariableReserved.contains(tid))
					throw new IllegalStateException();
				mockFieldVariableStatuses.remove(tid);
				return null;
			}).when(mockInstance).delete(Mockito.anyLong());
			Mockito.doAnswer((stubInvo) -> {
				long tid = stubInvo.getArgument(0);
				TStatus status = stubInvo.getArgument(1);
				if (!mockFieldVariableReserved.contains(tid))
					throw new IllegalStateException();
				if (!mockFieldVariableStatuses.containsKey(tid))
					throw new IllegalStateException();
				mockFieldVariableStatuses.put(tid, status);
				return null;
			}).when(mockInstance).setStatus(Mockito.anyLong(), Mockito.any());
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).getProperty(Mockito.anyLong(), Mockito.any());
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).setProperty(Mockito.anyLong(), Mockito.any(), Mockito.any());
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).waitForStatusChange(Mockito.anyLong(), Mockito.any());
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).reserve();
			Mockito.doAnswer((stubInvo) -> {
				long tid = stubInvo.getArgument(0);
				if (mockFieldVariableReserved.contains(tid))
					throw new IllegalStateException();
				mockFieldVariableReserved.add(tid);
				return null;
			}).when(mockInstance).reserve(Mockito.anyLong());
			Mockito.doAnswer((stubInvo) -> {
				long tid = stubInvo.getArgument(0);
				if (!mockFieldVariableReserved.contains(tid))
					throw new IllegalStateException();
				TStatus status = mockFieldVariableStatuses.get(tid);
				if (status == null)
					return TStatus.UNKNOWN;
				return status;
			}).when(mockInstance).getStatus(Mockito.anyLong());
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).top(Mockito.anyLong());
			Mockito.doAnswer((stubInvo) -> {
				long tid = stubInvo.getArgument(0);
				if (!mockFieldVariableReserved.remove(tid)) {
					throw new IllegalStateException();
				}
				return null;
			}).when(mockInstance).unreserve(Mockito.anyLong(), Mockito.anyLong());
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).pop(Mockito.anyLong());
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).getStack(Mockito.anyLong());
			Mockito.doAnswer((stubInvo) -> {
				return new ArrayList<>(mockFieldVariableStatuses.keySet());
			}).when(mockInstance).list();
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).push(Mockito.anyLong(), Mockito.any());
			Mockito.doAnswer((stubInvo) -> {
				mockFieldVariableStatuses.put(mockFieldVariableNextId[0], TStatus.NEW);
				return mockFieldVariableNextId[0]++;
			}).when(mockInstance).create();
		} catch (Exception exception) {
		}
		return mockInstance;
	}

}
