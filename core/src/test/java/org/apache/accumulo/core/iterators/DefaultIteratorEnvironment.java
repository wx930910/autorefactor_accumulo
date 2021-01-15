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
package org.apache.accumulo.core.iterators;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.iterators.system.MapFileIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.mockito.Mockito;

public class DefaultIteratorEnvironment {

	static public IteratorEnvironment mockIteratorEnvironment1() {
		AccumuloConfiguration[] mockFieldVariableConf = new AccumuloConfiguration[1];
		Configuration mockFieldVariableHadoopConf = new Configuration();
		IteratorEnvironment mockInstance = Mockito.spy(IteratorEnvironment.class);
		mockFieldVariableConf[0] = DefaultConfiguration.getInstance();
		try {
			Mockito.doAnswer((stubInvo) -> {
				return false;
			}).when(mockInstance).isSamplingEnabled();
			Mockito.doAnswer((stubInvo) -> {
				String mapFileName = stubInvo.getArgument(0);
				FileSystem fs = FileSystem.get(mockFieldVariableHadoopConf);
				return new MapFileIterator(fs, mapFileName, mockFieldVariableHadoopConf);
			}).when(mockInstance).reserveMapFileReader(Mockito.any(String.class));
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	static public IteratorEnvironment mockIteratorEnvironment2(AccumuloConfiguration conf) {
		AccumuloConfiguration[] mockFieldVariableConf = new AccumuloConfiguration[1];
		Configuration mockFieldVariableHadoopConf = new Configuration();
		IteratorEnvironment mockInstance = Mockito.spy(IteratorEnvironment.class);
		mockFieldVariableConf[0] = conf;
		try {
			Mockito.doAnswer((stubInvo) -> {
				return false;
			}).when(mockInstance).isSamplingEnabled();
			Mockito.doAnswer((stubInvo) -> {
				String mapFileName = stubInvo.getArgument(0);
				FileSystem fs = FileSystem.get(mockFieldVariableHadoopConf);
				return new MapFileIterator(fs, mapFileName, mockFieldVariableHadoopConf);
			}).when(mockInstance).reserveMapFileReader(Mockito.any(String.class));
		} catch (Exception exception) {
		}
		return mockInstance;
	}
}
