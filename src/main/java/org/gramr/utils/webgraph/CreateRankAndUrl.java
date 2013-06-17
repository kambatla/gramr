package org.gramr.utils.webgraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class CreateRankAndUrl extends Configured implements Tool {

  public static class ReadRankAndUrl extends
      Mapper<LongWritable, Text, IntWritable, RankAndUrl> {

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int lineNum = (int) key.get();
      String line = value.toString();
      RankAndUrl rankAndUrl;
      if (line.contains("http")) {
        // it is a url
        rankAndUrl = new RankAndUrl(lineNum, 1.0f, line);
      } else {
        rankAndUrl = RankAndUrl.parse(value.toString());
        lineNum = rankAndUrl.getSrc();
      }
      context.write(new IntWritable(lineNum), rankAndUrl);
    }
  }

  public static class JoinRankAndUrl extends
      Reducer<IntWritable, RankAndUrl, NullWritable, Text> {
    public void reduce(IntWritable src, Iterable<RankAndUrl> lists,
                       Context context) throws IOException, InterruptedException {
      RankAndUrl out = null;
      for (RankAndUrl list : lists) {
        if (out == null)
          out = list;
        else if (list.getUrl() == null) {
          out.setRank(list.getRank());
        } else {
          out.setUrl(list.getUrl());
        }
      }
      context.write(NullWritable.get(), new Text(out.toString()));
    }
  }

  private static int printUsage() {
    System.out
        .println("Usage: org.gramr.kernel.CreateRankAndUrl <Urls file> " +
            "<Sorted rank vector> <RankAndUrl output dir>");
    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      printUsage();
      System.exit(0);
    }

    Configuration conf = new Configuration(getConf());

    String urlsFile = args[0];
    String ranksFile= args[1];
    String output = args[2];

    Job job = null;
    try {
      job = new Job(conf, "Create RankedAdjListUrls");
      job.setJarByClass(CreateRankAndUrl.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setMapperClass(ReadRankAndUrl.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(RankAndUrl.class);
      job.setReducerClass(JoinRankAndUrl.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(urlsFile));
      FileInputFormat.addInputPath(job, new Path(ranksFile));
      FileOutputFormat.setOutputPath(job, new Path(output));

      job.setNumReduceTasks(new JobClient(new JobConf(conf)).getClusterStatus
          ().getMaxReduceTasks());
      job.waitForCompletion(true);
    } finally {
      if (job != null) {
        FileUtil.fullyDelete(new File(job.getConfiguration().get(
            "hadoop.tmp.dir")));
      }
    }

    return 0;
  }

  public static void main(String[] args) {
    try {
      ToolRunner.run(new Configuration(), new CreateRankAndUrl(),
          args);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

}
