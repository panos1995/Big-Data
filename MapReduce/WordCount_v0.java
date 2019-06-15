package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount_v0 extends Configured implements Tool {
	public int run(String[] args) throws Exception {


		//JobControl jobControl = new JobControl("jobChain");
	    Configuration conf1 = this.getConf();
	    conf1.set("Date", args[3]);
        conf1.set("rounds", args[2]);
        int rounds = Integer.parseInt(conf1.get("rounds"));
        for(int i=0;i<rounds;i++) {
        	System.out.println(i);
        }
	    Job job1 = Job.getInstance(conf1);  
	    job1.setJarByClass(WordCount_v0.class);
	    job1.setJobName("Filtering Job");
	    
	    job1.setMapperClass(MySimpleMapReduceJob.MyMapper.class);
	    job1.setReducerClass(MySimpleMapReduceJob.Reduce.class);
	    
	    job1.setInputFormatClass(TextInputFormat.class);
	    job1.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp0"));
        if (!job1.waitForCompletion(true)) {
      	  System.exit(1);
      	}
          
        //run this job for how many rounds
        for(int j=0;j<rounds;j++) {
        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(WordCount_v0.class);
        job2.setJobName("Looping Page Rank Job");
        
        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"+String.valueOf(j)));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/temp"+String.valueOf(j+1)));
        System.out.println(j);
        job2.setMapperClass(MyLoopingMapReduceJob.MyMapper.class);
        job2.setReducerClass(MyLoopingMapReduceJob.MyReducer.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        if (!job2.waitForCompletion(true)) {
        	  System.exit(1);
        	}
        }

        
        Configuration conf3 = getConf();
        Job job3 = Job.getInstance(conf3);
        job3.setJarByClass(WordCount_v0.class);
        job3.setJobName("Removal of Outlinks/Printing to a final job");
        
        
        FileInputFormat.setInputPaths(job3, new Path(args[1] + "/temp"+rounds));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/final_final"));
        
        job3.setMapperClass(FinalMapJob.MyMapper.class);
        job3.setNumReduceTasks(0);
        job3.setMapOutputKeyClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);

      
        return (job3.waitForCompletion(true) ? 0 : 1);   
          
	}

	

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount_v0(), args));
	}
}
