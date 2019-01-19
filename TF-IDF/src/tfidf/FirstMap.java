package tfidf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;

/**
 * 计算TF和总文件数D
 * 
 * @author hitlsq
 *
 */
public class FirstMap extends Mapper<LongWritable, Text, Text, IntWritable> {
 
	/**
	 * 对读取的每一行进行分词
	 * 每行处理后输出<词,1>的数据和<count,1>
	 */
	@Override
	protected void map(LongWritable key, Text line,Context context)
			throws IOException, InterruptedException {
		
		String[] words = line.toString().trim().split("\t");
		if (words.length>=2) {
			String id = words[0].trim();
			String cotent = words[1].trim();
			
			StringReader sr = new StringReader(cotent);
			IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
			Lexeme word;
			while ((word = ikSegmenter.next())!= null) {
				String w = word.getLexemeText();
				context.write(new Text(w+"_"+id), new IntWritable(1));
			}
			context.write(new Text("count"),new IntWritable(1));
		}else{
			System.out.println(line.toString()+"------------------");
		}
	}
}