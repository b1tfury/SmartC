import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;




public class SmartCluster {
	private static Logger logger = Logger.getLogger(SmartCluster.class);

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String [] keys = {"A","B","C","D","E","G"};
			String [] values = value.toString().split("[ABCDEG]");
			for(int i =0;i<6;i++){
				context.write(new Text(keys[i]),new Text(values[i+1]+"sahil"));
			}
			
		}
	}
	public static class Reduce extends Reducer<Text, Iterable<Text>, Text, Text>{
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			String set = null;
			System.out.println(">>>"+value.toString());
			logger.info("reducer values"+value);
			for(Text val : value){
				String [] values = val.toString().split("ab,");
				for(int i =0;i<values.length;i++){
					if(set!=null){
						set = set +","+values[i];
					}
					else
						set = set + values[i];
				}
			
			logger.info(set);
			context.write(key, new Text(set));
		}
			}
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "SmartCluster");
		job.setJarByClass(SmartCluster.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println(job.waitForCompletion(true)? 0:1);
	}

}
