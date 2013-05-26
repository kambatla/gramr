package org.gramr.kernel;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import org.gramr.common.RankedAdjList;

public abstract class MatVec extends Configured implements Tool {
	public static final String thresholdConfValue = "MATVEC_THRESHOLD_VALUE";
	public static final Log LOG = LogFactory.getLog(MatVec.class);

	public static enum converge {
		noConverge
	};

	protected static class MatVecCombiner
			extends
			Reducer<IntWritable, MatVecMapOutputValue, IntWritable, MatVecMapOutputValue> {

		public void reduce(IntWritable key,
				Iterable<MatVecMapOutputValue> values, Context context)
				throws IOException, InterruptedException {
			float rank = 0.0f;
			boolean hasOutLinks = false;

			for (MatVecMapOutputValue val : values) {
				if (val.isAdjList()) {
					context.write(key, val);
				} else {
					hasOutLinks = true;
					rank += val.getRank();
				}
			}

			if (hasOutLinks) {
				context.write(key, MatVecMapOutputValue.createEdgeValue(rank));
			}
		}
	}

	protected static class MatVecReducer extends
			Reducer<IntWritable, MatVecMapOutputValue, NullWritable, Text> {
		private boolean thresholdCheck = false;
		private float threshold;

		public void setup(Context context) {

			threshold = context.getConfiguration().getFloat(thresholdConfValue,
					-1f);
			if (threshold != -1f && threshold >= 0) {
				thresholdCheck = true;
			}
		}

		public void reduce(IntWritable key,
				Iterable<MatVecMapOutputValue> values, Context context)
				throws IOException, InterruptedException {
			float rank = 0.0f, oldRank = 0.0f;
			int[] dst = null;

			for (MatVecMapOutputValue val : values) {
				if (val.isAdjList()) {
					oldRank = val.getRank();
					dst = val.getDst();
				} else {
					rank += val.getRank();
				}
			}

			context.write(NullWritable.get(), new Text(RankedAdjList
					.createRankedAdjList(key.get(), rank, dst).toString()));

			if (thresholdCheck) {
				if (Math.abs(rank - oldRank) > threshold) {
					LOG.info("Node = " + key.get() + " Old Rank = " + oldRank
							+ " Rank = " + rank);
					context.getCounter(converge.noConverge).increment(1);
				}
			} else {
				context.getCounter(converge.noConverge).increment(1);
			}
		}
	}

	protected static boolean checkConvergence(Job job) throws IOException {
		return job.getCounters().findCounter(converge.noConverge).getValue() == 0;
	}
	
	protected abstract int printUsage();
}
