package org.gramr.kernel;

import org.gramr.io.RandomInputFormat;
//import io.RankedAdjListOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.gramr.common.RankedAdjList;

public class GraphGenerator extends Configured implements Tool {
	public static final Log LOG = LogFactory.getLog(GraphGenerator.class);

	public static class GenerateRandomRankedAdjLists extends
			Mapper<Text, Text, NullWritable, Text> {
		Configuration conf;
		Random rand;
		int numNodes, degree, partitionSize;
		float ratio;

		@Override
		public void setup(Context context) {
			rand = new Random();
			conf = context.getConfiguration();
			numNodes = conf.getInt("NUM_NODES", 0);
			degree = conf.getInt("DEGREE", 0);
			partitionSize = conf.getInt("GRAPH_PARTITION_SIZE", 65536);
			ratio = conf.getFloat("BORDER_EDGES_RATIO", 0.1f);
		}

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			int start = Integer.parseInt(key.toString()) * partitionSize;
			int end = start + partitionSize - 1;

			if (end > numNodes - 1)
				end = numNodes - 1;

			LOG.info("Inside map " + start + " to " + end);

			for (int src = start; src <= end; src++) {
				int numEdges = rand.nextInt(2 * degree);
				if (numEdges == 0) {
					context.write(NullWritable.get(), new Text(RankedAdjList
							.createRankedAdjList(src, 1.0f, (int[]) null)
							.toString()));
					continue;
				}

				int numBorderEdges = (int) Math.floor(ratio * numEdges);
				TreeSet<Integer> local = new TreeSet<Integer>();
				while (local.size() < numEdges - numBorderEdges) {
					int newNode = start + rand.nextInt(end - start + 1);
					if (newNode != src) {
						local.add(newNode);
					}
				}

				TreeSet<Integer> external = new TreeSet<Integer>();
				while (external.size() < numBorderEdges) {
					int tempDst = rand.nextInt(numNodes);
					if (tempDst < start || tempDst > end) {
						external.add(tempDst);
					}
				}

				local.addAll(external);
				context.write(NullWritable.get(), new Text(RankedAdjList
						.createRankedAdjList(src, 1.0f, local).toString()));
			}
		}
	}

	private int printUsage() {
		LOG
				.info("GraphGenerator <output-dir> <# nodes> <degree> <ratio> <partition size>");
		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage();
			System.exit(0);
		}

		String output = args[0];
		int numNodes = Integer.parseInt(args[1]);
		int degree = Integer.parseInt(args[2]);
		float borderEdgesRatio = Float.parseFloat(args[3]);
		int partitionSize = Integer.parseInt(args[4]);

		// set partition size to at least twice the degree
		if (partitionSize < 2 * degree) {
			partitionSize = 2 * degree;
		}

		// find number of maps
		int numMaps = numNodes / partitionSize;
		if (numNodes % partitionSize != 0)
			numMaps += 1;

		Configuration conf = new Configuration(getConf());
		conf.setInt("NUM_NODES", numNodes);
		conf.setInt("DEGREE", degree);
		conf.setInt("GRAPH_PARTITION_SIZE", partitionSize);
		conf.setFloat("BORDER_EDGES_RATIO", borderEdgesRatio);
		conf.set("OUTPUT", output);
		conf.setInt("RANDOM_INPUT_FORMAT_NUM_MAPS", numMaps);

		Job job = null;
		try {
			job = new Job(conf, "Random Graph Generator");
			job.setJarByClass(GraphGenerator.class);

			job.setInputFormatClass(RandomInputFormat.class);
			job.setMapperClass(GenerateRandomRankedAdjLists.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(output));

			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
		} finally {
			if (job != null) {
				FileUtil.fullyDelete(new File(job.getConfiguration().get(
						"hadoop.tmp.dir")));
			}
		}

		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new GraphGenerator(), args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}