package cn.com.zenmaster.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 单词计数
 * 优化后的代码
 * @author TianYu
 *
 */
public class WordCount {

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		//将每次需要 new 的对象单独提取出来，并将其类设置成静态的，则创建的对象也是静态的
		private Text word = new Text();
		private IntWritable one = new IntWritable(1);
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			//得到一行输入的数据
			String line = value.toString();
			
			//通过空格来切割数据
			String[] words = line.split(" ");
			
			//遍历并输入
			for(String w : words) {
				word.set(w);
				context.write(word, one);
			}
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
		//将每次需要 new 的对象单独提取出来，并将其类设置成静态的，则创建的对象也是静态的
		private LongWritable sum = new LongWritable(1);
		
		@Override
		protected void reduce(Text inputKey, Iterable<IntWritable> inputValue, Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			//计数
			Integer count = 0;
			
			for(IntWritable value : inputValue) {
				count += value.get();
			}
			sum.set(count);
			context.write(inputKey, sum);
		}
	}
	
	public static void main(String[] args) {
		try {
			if(args == null || args.length != 2) {
				System.err.println("Usage: <InputPath> <OutputPath>");
				System.exit(-1);
			}
			//创建配置对象
			Configuration conf = new Configuration();
			
			//创建 Job 对象
			Job job = Job.getInstance(conf, "WordCount");
			
			//设置 Job 运行 Job 的主类
			job.setJarByClass(WordCount.class);
			
			//设置 Mapper 类
			job.setMapperClass(WordCountMapper.class);
			
			//设置 Reducer 类
			job.setReducerClass(WordCountReducer.class);
			
			//设置 map 输出的 <key,value> 类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			//设置 reduce 输出的 <key,value> 类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			//设置输入文件和输出文件夹路径
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			boolean result =job.waitForCompletion(true);
			if(!result) {
				System.out.println("任务失败");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
