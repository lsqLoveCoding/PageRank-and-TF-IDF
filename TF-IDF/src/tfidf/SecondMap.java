package tfidf;
 
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 * 计算包含特定词的文件数
 * 以上一次结果输出作为输入
 */
public class SecondMap extends Mapper<LongWritable, Text, Text, IntWritable> {
 
	/**
	 * 对除了最后一个分区文件内的每行数据进行统计
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//过滤掉不要处理的文件
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		
		if (!fileSplit.getPath().getName().contains("part-r-00003")) {
			String[] words = value.toString().trim().split("\t");
			if (words.length>=2) {
				String ss = words[0].trim();
				String[] split = ss.split("_");
				if (split.length>=2) {
					String w = split[0];
					context.write(new Text(w), new IntWritable(1));
				}
			}
		}	
	}
}