package tfidf;
 
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class ThirdJob {
 
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		conf.set("yarn.resourcemanager.hostname", "slave2");
		//采用服务器环境测试
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(ThirdJob.class);
		
		job.setMapperClass(ThirdMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(ThirdReduce.class);
		//把常用文件数据加载到内存中，提高获取速度
		job.addCacheFile(new Path("/user/tfidf/output/result1/part-r-00003").toUri());//文件总数
		job.addCacheFile(new Path("/user/tfidf/output/result2/part-r-00000").toUri());//含有特定词的文件数
		
		Path inPath = new Path("/user/tfidf/output/result1");
		FileInputFormat.addInputPath(job, inPath);
		
		Path outPath = new Path("/user/tfidf/output/result3");
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