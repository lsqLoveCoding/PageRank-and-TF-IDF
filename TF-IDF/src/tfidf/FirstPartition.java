package tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
 
public class FirstPartition extends HashPartitioner<Text, IntWritable> {
 
	/**
	 * 返回值作为分区号，即分区文件的序号
	 * 分区个数与reduce任务个数相同
	 * 假设有4个reduce任务，则有4个分区，分区文件序号从0开始。
	 */
	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		if (key.equals(new Text("count"))) {
			return 3;//将键为count的数据放在序号为3的分区文件中
		}else{//其他数据对键进行hash取模，放到前三个分区文件
			return super.getPartition(key, value, numReduceTasks-1);
		}
	}
}