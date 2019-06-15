package mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.ISO8601;

public class MySimpleMapReduceJob extends Configured implements Tool {


	// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public String article_title="";
		public long timestamp =0;
		public int  summary_changed = 0;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}

		// The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			long input_date=0 ;

			try {
				input_date = ISO8601.toTimeMS((context.getConfiguration().get("Date").toString()));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			int rounds = Integer.parseInt(context.getConfiguration().get("rounds"));



			String[] tokens = line.split("\\s+");
			//Empty Line//
			if(tokens.length!=0) {
				if(tokens[0].equals("REVISION")) {
					article_title = tokens[3];
					try {
						timestamp = ISO8601.toTimeMS(tokens[4]);
					} catch (ParseException e) {

						timestamp=0;
						e.printStackTrace();

					}
				}else if(tokens[0].equals("MAIN") && timestamp <= input_date) {
					/* We are at the main line but we must check two things;
					 * 1) check for the date, reducer will keep the max date but we can prevent some extra traffic
					 * at our system if we remove some dates before
					 * 2) if list is empty
					 * 3) we check for loops here
					 *
					 */
					ArrayList<String> out_links = new ArrayList<>();
					//We are at main line so with this if we check if we have outlinks.
					if(!(tokens.length==1)){
						for(int i=1; i<tokens.length;i++) {
							//only one edge and not self-loops
							if ((!(out_links.contains(tokens[i]))) && (!(tokens[i].equals(article_title))))
								out_links.add(tokens[i]);
						}
						if(!(out_links.size()==0)) {
						/* SO we write the context Key,Values -> (Article_title, [timestamp,link0,...linkn]*/
						String returned_string = String.valueOf(timestamp)+",";
						for(int i=0; i<out_links.size()-1; i++) {
							returned_string = returned_string + out_links.get(i)+",";
						}
						returned_string = returned_string + out_links.get(out_links.size()-1);
						word.set(article_title);
						context.write(word,new Text(returned_string));
						}
						// no out links we set the input_date//
					}else{ 
						word.set(article_title);
						System.out.println(article_title);
						context.write(word,new Text(String.valueOf(timestamp))); }
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
	public static class Reduce extends Reducer<Text, Text,Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			long max =0;
			String outlinks_of_max ="";
			//taking the values//
			for (Text value : values) {
				String line = value.toString();
				String [] tokens = line.split(",");

				long timestamp = Long.parseLong(tokens[0]);
				String outlinks = "";

				if(timestamp> max) {
					max = timestamp;
					//Keep the tokens of the maximum//
					for(int i=1;i<tokens.length-1;i++) {
						outlinks=outlinks+tokens[i]+",";

					}

					outlinks=outlinks+tokens[tokens.length-1];
					outlinks_of_max = outlinks;
					
				}
				
			}
			
			
			context.write(key,new Text("1,"+outlinks_of_max));
		}
	}

	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
        Job job = Job.getInstance(getConf());

    // 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(MySimpleMapReduceJob.class);

        Configuration conf = this.getConf();
        conf.set("Date", args[3]);
        conf.set("rounds", args[2]);

        job.setJarByClass(MySimpleMapReduceJob.class);

        job.setMapperClass(MySimpleMapReduceJob.MyMapper.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(MySimpleMapReduceJob.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setMapOutputValueClass(Text.class);
        //job.setMapOutputValueClass(ArrayWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path (args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);

    }

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MySimpleMapReduceJob(), args));
	}
}
