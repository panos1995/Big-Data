package mapreduce;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FinalMapJob extends Configured implements Tool {
	
	static class MyMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}
		

		// The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			//tokenize the input from the looping job
			String tokens[] = value.toString().split(",");
			
			String article_key = key.toString();
			
			
			//Only get the first token which is the page rank
			context.write(new Text(article_key), new Text(tokens[0]) );
		}
		}
	static class MyReducer extends Reducer<DoubleWritable, Text, Text, Text> {
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}
		protected void reduce(DoubleWritable key, Text values, Context context) throws IOException, InterruptedException {
			context.write(new Text(values.toString()), new Text(key.toString()));
		}
	}
	public int run(String[] args) throws Exception {
		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
		Job job = Job.getInstance(getConf());

		// 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(MyLoopingMapReduceJob.class);

		// 2. Set mapper and reducer classes
		// ...
		job.setMapperClass(MyLoopingMapReduceJob.MyMapper.class);
		job.setReducerClass(MyLoopingMapReduceJob.MyReducer.class);


		// 3. Set final output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 4. Get #loops from input (args[])
		int numLoops = 0; // Change this!

		boolean succeeded = false;
		
			succeeded = job.waitForCompletion(true);

		
		return (succeeded ? 0 : 1);
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyLoopingMapReduceJob(), args));
	}

}
