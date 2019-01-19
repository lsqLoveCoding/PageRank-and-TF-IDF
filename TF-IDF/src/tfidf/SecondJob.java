package tfidf;

import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class SecondJob {
 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		conf.set("yarn.resourcemanager.hostname", "slave2");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SecondJob.class);
		
		job.setMapperClass(SecondMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(SecondReduce.class);
		
//		job.setNumReduceTasks(4);
		job.setReducerClass(SecondReduce.class);
		
		Path inPath = new Path("/user/tfidf/output/result1");
		FileInputFormat.addInputPath(job, inPath);
		
		Path outPath = new Path("/user/tfidf/output/result2");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
		//提交job，等待完成！
		boolean flag = job.waitForCompletion(true);
		if (flag) {
			System.out.println("Job success!");
		}
	}
}