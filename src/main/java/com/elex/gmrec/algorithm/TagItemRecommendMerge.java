package com.elex.gmrec.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;
import com.elex.gmrec.comm.RandomUtils;

public class TagItemRecommendMerge extends Configured implements Tool {

	/**功能：将TagRecommendMixer和ItemCF形成的推荐结果去重合并并随机选择size个gid输出
	 * 输入1：Constants.CFRECPARSE，ItemCf解析后的推荐结果
	 * 输入2：Constants.TAGITEMRECMERGE，TagCf和UserTopNTag合并后的色推荐结果
	 * 输出：uid,json数组
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TagItemRecommendMerge(), args);
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"TagItemRecommendMerge");
		job.setJarByClass(TagItemRecommendMerge.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		Path itemCfRecParse = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.CFRECPARSE);
		FileInputFormat.addInputPath(job, itemCfRecParse);	
		Path tagCfRecFinal = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFRECFINAL);
		FileInputFormat.addInputPath(job, tagCfRecFinal);
		
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path output = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.TAGITEMRECMERGE);
		HdfsUtils.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		String[] list;
				
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			list = value.toString().split("\\s");
			if(list.length == 2){
				context.write(new Text(list[0]), new Text(list[1]));
			}			
		}
			
	}
	
	
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		Set<String> unionRec = new HashSet<String>();
		List<String> result = new ArrayList<String>();
		int index[];
		

		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int size = Integer.parseInt(PropertiesUtils.getUserRecNumber());
			unionRec.clear();
			try {
				for (Text line : values) {
					if(!line.toString().equals("")){
						JSONArray json= new JSONArray(line.toString());
						for (int i = 0; i < json.length(); i++) {
							JSONObject obj = (JSONObject) json.get(i);
							unionRec.add(obj.keys().next().toString());
						}
					}
					
				}
			} catch (JSONException e) {				
				e.printStackTrace();
			}
			
	
			
			if (unionRec.size() > 0) {	
				
				result.clear();
				Iterator<String> ite = unionRec.iterator();
				while(ite.hasNext()){
					result.add(ite.next());
				}

				size = result.size() > size ? size : result.size();
				index = RandomUtils.randomArray(0, result.size() - 1, size);
				
				
				StringBuffer sb = new StringBuffer(200);
				sb.append(key.toString() + "\t");
				sb.append("[");
				for (int i=0;i<index.length;i++) {
					sb.append("{");
					sb.append("\"" + result.get(index[i]) + "\":" + "\"0\"");
					sb.append("}");
					sb.append(",");
				}

				context.write(null,new Text(sb.substring(0, sb.toString().length() - 1)+ "]"));
			}
			
						
		}				
		
	}
}
