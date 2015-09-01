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
package backtype.storm.autoscale.throughput;

import backtype.storm.ConfigValidation;

/**
 * These configuration parameters extend the TOPOLOGY configuration entities for
 * the Throughput Based Autoscaler.
 *
 */
public class TPASConfigExtensions {

	/**
	 * The number of cores for each supervisor.
	 */
	public static final String TOPOLOGY_TPAS_SUPERVISOR_NCORES = "topology.tpas.supervisor.ncores";
	public static final Object TOPOLOGY_TPAS_SUPERVISOR_NCORES_SCHEMA = ConfigValidation.IntegerValidator;

	/**
	 * Each spout shall provide configuration to enable the autoscaler to work.
	 */
	public static final String TOPOLOGY_TPAS_SPOUT_CONFIG = "topology.tpas.spoutconfig";
	public static final Object TOPOLOGY_TPAS_SPOUT_CONFIG_SCHEMA = ConfigValidation.MapOfStringToMapValidator;

	/**
	 * The maximum expected data rate for the spout (required).
	 */
	public static final String TOPOLOGY_TPAS_SPOUT_CONFIG_TARGET_TPS = "spoutconfig.targettps";
	public static final Object TOPOLOGY_TPAS_SPOUT_CONFIG_TARGET_TPS_SCHEMA = String.class;

	/**
	 * The throughput for a single instance of a spout (required). Today we
	 * don't have good autoscaling for the spout, it is believed that with
	 * sufficient testing, this number can be determined.
	 * 
	 * In time this should be replaced with a different mechanism. When that
	 * happens this parameter will become deprecated and no longer required in a
	 * backwards compatible manner.
	 */
	public static final String TOPOLOGY_TPAS_SPOUT_CONFIG_THROUGHPUT = "spoutconfig.throughput";
	public static final Object TOPOLOGY_TPAS_SPOUT_CONFIG_THROUGHPUT_SCHEMA = ConfigValidation.IntegerValidator;
}
