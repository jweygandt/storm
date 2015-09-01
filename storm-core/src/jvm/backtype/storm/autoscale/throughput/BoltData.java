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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;

public class BoltData extends CompData {
	private static final long serialVersionUID = 1L;

	private transient double computedParallelism = 0.0;
	private transient Map<String, Long> targetOutputTpsPerStreamId = new HashMap<>();

	private String id;
	private int actualParallelism = 1;

	public Object readResolve() {
		targetOutputTpsPerStreamId = new HashMap<>();
		return super.readResolve();
	}

	public BoltData(String id) {
		this.id = id;
	}

	public void commitUpdate() {
	}

	public double getComputedParallelism() {
		if (getSummarySize() == 0)
			throw new RuntimeException("No metrics");
		return computedParallelism;
	}

	public void setActualParallelism(int actualParallelism) {
		this.actualParallelism = actualParallelism;
	}

	public int getActualParallelism() {
		return actualParallelism;
	}

	@Override
	public void updateInternals(PersistentData data) {
		if (getSummarySize() == 0)
			throw new RuntimeException("No summaries:" + id);

		long totalProcessed = 0;
		for (ExecutorSummary summary : getSummaries()) {
			Map<GlobalStreamId, Long> executed = summary.get_stats().get_specific().get_bolt().get_executed()
					.get(PersistentData.metricTimeWindowSecs + "");
			for (Long cnt : executed.values()) {
				totalProcessed += cnt;
			}
		}

		// for each inbound stream compute the parallelism needed independently
		// and sum it up
		double pismSum = 0.0;
		double bulkSfSum = 0.0;
		for (ExecutorSummary summary : getSummaries()) {
			Map<GlobalStreamId, Double> executed_ms = summary.get_stats().get_specific().get_bolt().get_execute_ms_avg()
					.get(PersistentData.metricTimeWindowSecsStr);
			Map<GlobalStreamId, Long> executed = summary.get_stats().get_specific().get_bolt().get_executed()
					.get(PersistentData.metricTimeWindowSecsStr);
			for (GlobalStreamId key : executed_ms.keySet()) {
				String upstreamCid = key.get_componentId();
				String upstreamSid = key.get_streamId();
				StreamData upstreamSd = data.getComponent(upstreamCid).getStreamData(upstreamSid);
				double upstreamTargetTps = upstreamSd.getPerInstanceTargetTps();

				double ms = executed_ms.get(key);
				double streamPism = upstreamTargetTps / getSummarySize() / (1000.0 / ms);
				pismSum += streamPism;

				double cnt = executed.get(key);
				double rate = cnt / data.getWindowtime();
				// .................weighting ................scalefactor
				double localSf = (cnt / totalProcessed) * (upstreamTargetTps / rate);
				bulkSfSum += localSf;
				System.out.println("Bolt: " + id + ":" + upstreamCid + ":" + upstreamSid + ", upstreamTargetTps="
						+ upstreamTargetTps + ", ms=" + ms + ", streamPism=" + streamPism + ", cnt=" + cnt
						+ ", localSf=" + localSf);
			}
		}
		computedParallelism = Math.max(computedParallelism, pismSum);
		System.out.println("Bolt: " + id + ", processed=" + totalProcessed + ", pismSum=" + pismSum + ", sf="
				+ bulkSfSum + ", actualPism=" + actualParallelism + ", compPism=" + computedParallelism);

		// Compute output info
		Map<String, Long> emittedTotals = new HashMap<>();
		for (ExecutorSummary summary : getSummaries()) {
			Map<String, Long> emittedByStreamId = summary.get_stats().get_emitted()
					.get(PersistentData.metricTimeWindowSecsStr);
			for (Entry<String, Long> ent : emittedByStreamId.entrySet()) {
				String streamId = ent.getKey();
				Long emitted = ent.getValue();
				Long total = emittedTotals.get(streamId);
				emittedTotals.put(streamId, emitted + (total == null ? 0 : total));
			}
		}

		double bulkSf = bulkSfSum / getSummarySize();
		for (Entry<String, Long> ent : emittedTotals.entrySet()) {
			String streamId = ent.getKey();
			Long emitted = ent.getValue();
			Long targetEmitted = targetOutputTpsPerStreamId.get(streamId);
			targetOutputTpsPerStreamId.put(streamId, Math.max((targetEmitted == null ? 0 : targetEmitted),
					(long) (emitted * bulkSf / data.getWindowtime())));
		}
		System.out.println("  Stream Data: " + targetOutputTpsPerStreamId);
	}

	@Override
	public StreamData getStreamData(String streamId) {
		return new StreamData(targetOutputTpsPerStreamId.get(streamId));
	}
}