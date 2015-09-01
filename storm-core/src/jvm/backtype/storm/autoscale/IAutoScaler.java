/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.autoscale;

import java.io.File;
import java.util.Map;

import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.StormTopology;

/**
 * Interface for various auto-scaler algorithms. The class should have a default
 * constructor, and will be configured by Config.TOPOLOGY_AUTO_SCALER.
 *
 */
/**
 * @author jweygandt
 *
 */
public interface IAutoScaler {

	/**
	 * Used for topologies with manual scaling
	 */
	static final IAutoScaler NONE = new IAutoScaler() {

		@Override
		public void stopAutoScaler() {
		}

		public boolean isRunning() {
			return false;
		};

		@Override
		public void reLoadAndReStartAutoScaler(Iface nimbus, File datadir, String stormName, String stormId) {
		};

		@Override
		public Object[] modifyConfigAndTopology(String stormId, @SuppressWarnings("rawtypes") Map stormConf,
				StormTopology topology) {
			return new Object[] { stormConf, topology };
		};
	};

	/**
	 * This is typically the result of Nimbus restarting and picking up
	 * existing, running, topologies. There should be a state to restore from.
	 * 
	 * @param nimbus
	 * @param datadir
	 * @param stormName
	 * @param stormId
	 */
	void reLoadAndReStartAutoScaler(Nimbus.Iface nimbus, File datadir, String stormName, String stormId);

	/**
	 * This should shut down any activity for the stormId from the prior
	 * startAutoScaler() call.
	 */
	void stopAutoScaler();

	boolean isRunning();

	/**
	 * Modify the config and/or topology. Please make a deepCopy of topology,
	 * and the Map is an IPersistentMap.
	 * 
	 * @param stormId
	 * @param stormConf
	 * @param topology
	 * @return
	 * @throws InvalidTopologyException
	 */
	Object[] modifyConfigAndTopology(String stormId, @SuppressWarnings("rawtypes") Map stormConf,
			StormTopology topology) throws InvalidTopologyException;

}
