package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyLoopingMapReduceJob extends Configured implements Tool {

	// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}

		// The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString(); //Convert from Text to simple String
			StringTokenizer tokenizer = new StringTokenizer(line,","); // tokenize the value on our special character ","
			List<String> elements = new ArrayList<String>();
			// iterate through StringTokenizer tokens

			//Add all elements to a String list
			while(tokenizer.hasMoreTokens()) {

				// add tokens to AL
				elements.add(tokenizer.nextToken());

				//[0] pagerank
				//[1] outlink1
				//[2] outlink2
				//[N] outlinkN
			}
			//Parse the [0] element which is the page rank
			double pagerank = 1.0*Double.parseDouble(elements.get(0).toString());
			Text KEY = null;
			Text VALUE = null;
			//remove the pagerank from list in order to run the for loop
			elements.remove(0);
			//pass the outlinks again to the reducer
			if(elements.size()!=0) {
				context.write(new Text(key), new Text("!"+String.join(",", elements)));

				//Calculate pagerank contribution for a specific element
				for(String element : elements) {
					KEY= new Text(element);
					//we added an extra element pagerank
					double b = pagerank/(elements.size());
					String tmp= Double.toString(b);

					VALUE = new Text(tmp);
					context.write(KEY, VALUE );

				}
			}




		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}


		// The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		// Make sure that the output key/value classes also match those set in your job's configuration (see below).
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String outlinks ="";
			double sumPageRank =0.0;
			double score;
			for(Text v : values) {

				String b = v.toString();
				if(b.startsWith("!")) {
					//truncate the ! symbol
					outlinks=b.substring(1,b.length());
				}else {
					score = Double.parseDouble(v.toString());
					//add each links contribution to the sum
					sumPageRank = sumPageRank + score;
				}

			}
			//Page rank calculation for all outlinks
			double PageRank = 0.15 + 0.85*(sumPageRank);
			String out = Double.toString(PageRank)+","+outlinks;

			//pass the key and as a value the page rank,{outlinks} for input to the MyLoopingMapReduceJob again
			context.write(key, new Text(out));



		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}
	//we keep this configuration if we need to test only the second job.
	// Your main Driver method. Note: everything in this method runs locally at the client.
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
		for (int i = 0; i < numLoops; i++) {
			// 5. Set input and output format, mapper output key and value classes, and final output key and value classes
			//    As this will be a looping job, make sure that you use the output directory of one job as the input directory of the next!
			// ...

			// 6. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
			// ...

			// 7. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
			succeeded = job.waitForCompletion(true);

			if (!succeeded) {
				// 8. The program encountered an error before completing the loop; report it and/or take appropriate action
				// ...
				break;
			}
		}
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyLoopingMapReduceJob(), args));
	}
}
