package tfidf;
 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 * 对每个文件中每个词进行TF-IDF计算
 * @author lenovo
 *
 */
public class ThirdMap extends Mapper<LongWritable, Text, Text, Text> {
	private Map<String, Integer> df = new HashMap<String, Integer>();//包含特定词的文件数
	private Map<String, Integer> count = new HashMap<String, Integer>();//文件总数
 
	/**
	 * 在map函数执行前执行
	 * 从内存中将数据取出，置于map容器中
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		//获取内存中的文件
		URI[] cacheFiles = context.getCacheFiles();
		//文件内容加载到容器内
		if (cacheFiles != null) {
			for (int i = 0; i < cacheFiles.length; i++) {
				URI uri = cacheFiles[i];
				if (uri.getPath().endsWith("part-r-00003")) {//文件总数
					//读取内容
					Path path = new Path(uri.getPath());
					System.out.println(uri.getPath()+"-"+path.getName());
					BufferedReader buffer = new BufferedReader(new FileReader(path.getName()));
					String line = buffer.readLine();
					if (line.startsWith("count")) {
						String[] split = line.split("\t");
						count.put(split[0], Integer.valueOf(split[1].trim()));
					}
					buffer.close();
				}else if(uri.getPath().endsWith("part-r-00000")){//含有特定词的文件数
					//读取内容
					Path path = new Path(uri.getPath());
					BufferedReader buffer = new BufferedReader(new FileReader(path.getName()));
					
					String line ;
					while((line = buffer.readLine()) != null){
						String[] split = line.split("\t");
						df.put(split[0], Integer.parseInt(split[1].trim()));
					}
					buffer.close();	
				}
				
			}
		}
		
	}
 
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		if (!fileSplit.getPath().getName().contains("part-r-00003")) {
			String[] split = value.toString().trim().split("\t");
			if (split.length>=2) {
				int TF = Integer.parseInt(split[1]);//词频，每个词在所在文件中出现的次数
				
				String[] ss = split[0].split("_");
				if (ss.length >=2) {
					String word = ss[0];
					String id = ss[1];
					
					double TF_IDF = TF * Math.log(count.get("count")/df.get("word"));
					
					NumberFormat f = NumberFormat.getInstance();
					f.setMaximumFractionDigits(5);
					
					context.write(new Text(id), new Text(word+"_"+f.format(TF_IDF)));
				}
			}
		}			
	}
}