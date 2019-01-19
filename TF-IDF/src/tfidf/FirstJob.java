package tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FirstJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://localhost:9000");
			conf.set("yarn.resourcemanager.hostname", "slave2");
			//实例化job
			Job job = Job.getInstance(conf);
			//job入口
			job.setJarByClass(FirstJob.class);
			//设置map相关信息，包括自定义的map类，map输出key的类型，输出value的类型
			job.setMapperClass(FirstMap.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			//设置job的shuffle过程操作信息，包括分区，排序，分组
			job.setPartitionerClass(FirstPartition.class);
			job.setCombinerClass(FirstReduce.class);
//			job.setSortComparatorClass();
//			job.setGroupingComparatorClass();
			//设置reduce相关信息，包括reduce任务个数，自定义的reduce类
			job.setNumReduceTasks(4);
			job.setReducerClass(FirstReduce.class);
			//设置要处理的文件
			Path inPath = new Path("/user/tfidf/input/data.txt");
			FileInputFormat.addInputPath(job, inPath);
			//设置最终结果保存的路径
			Path outPath = new Path("/user/tfidf/output/result1");
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