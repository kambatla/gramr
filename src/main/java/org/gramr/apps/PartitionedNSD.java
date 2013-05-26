package org.gramr.apps;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

import org.gramr.kernel.PartitionedMatVec;
import org.gramr.kernel.SortRankedGraphByRank;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionedNSD extends Configured implements Tool {
	public static final Log LOG = LogFactory.getLog(PartitionedNSD.class);

	public void printUsage() {
		LOG.error("Usage: org.gramr.apps.PartitionedNSD <matrix> <partitionSize> globalIterations");
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			System.exit(0);
		}

		String inputGraph = args[0];
		String partitionSize = args[1];
		String numIterations = args[2];
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(new Path(inputGraph))) {
			LOG.error("No such graph exists");
			return -1;
		}

		String[] matvecArgs = new String[6];
		matvecArgs[0] = inputGraph;
		matvecArgs[1] = inputGraph + "-pm";
		matvecArgs[2] = "-subgraph";
		matvecArgs[3] = partitionSize;
		matvecArgs[4] = "-outer";
		matvecArgs[5] = numIterations;

		ToolRunner.run(conf, new PartitionedMatVec(), matvecArgs);

		String[] sortVecArgs = new String[2];
		sortVecArgs[0] = inputGraph + "-pm/iter" + numIterations;
		sortVecArgs[1] = inputGraph + "-pm/sorted";
		ToolRunner.run(conf, new SortRankedGraphByRank(), sortVecArgs);

		// String[] selfSimilarityArgs = new String[2];
		// selfSimilarityArgs[0] = sortVecArgs[1];
		// selfSimilarityArgs[1] = inputGraph + "-pm/similarNodes";
		// ToolRunner.run(conf, new SelfSimilarityGraphMatching(),
		// selfSimilarityArgs);

		return 0;
	}

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		
		try {
			ToolRunner.run(new Configuration(), new PartitionedNSD(), args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		long endTime = System.currentTimeMillis();

		try {
			DataOutputStream out = new DataOutputStream(new FileOutputStream(
					"partitioned-nsd-timings", true));
			out.writeBytes(new Date() + " partitioned-nsd-" + args[0] + " "
					+ (endTime - startTime) / 1000 + "\n");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
