package com.elex.gmrec.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;
import com.elex.gmrec.comm.RandomUtils;
import com.elex.gmrec.etl.IDMapping;

public class TagRecommendMixer extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TagRecommendMixer(), args);
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"TagCfRec");
		job.setJarByClass(TagRecommendMixer.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		Path rating = new Path(PropertiesUtils.getRatingFolder()+Constants.MERGEFOLDER);
		FileInputFormat.addInputPath(job, rating);	
		Path tagcfout = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFOUTPUT);
		FileInputFormat.addInputPath(job, tagcfout);
		Path userTopTag = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.USERTOPTAG);
		FileInputFormat.addInputPath(job, userTopTag);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path output = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFRECFINAL);
		HdfsUtils.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		String[] list;
		Map<Integer,String> uidMap;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			uidMap = IDMapping.getUidIntStrMap();
		}

		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(pathName.contains(Constants.MERGEFOLDER)){
				list = value.toString().split(",");
				context.write(new Text(list[0]), new Text("01_"+list[1]));
			}else if(pathName.contains(Constants.TAGCFOUTPUT)){
				list = value.toString().split("\\s");
				context.write(new Text(uidMap.get(Integer.parseInt(list[0].trim()))), new Text("02_"+parseTagCFRec(list[1])));
			}else if(pathName.contains(Constants.USERTOPTAG)){
				list = value.toString().split("\\s");
				context.write(new Text(list[0]), new Text("02_"+parseUserTagTopN(list[1])));
			}
		}
		
		protected String parseTagCFRec(String recStr){
			String itemStr = recStr.trim().replace("[", "").replace("]", "");
			String[] itemArr = itemStr.split(",");
			StringBuffer sb = new StringBuffer(200);
			for (int i = 0; i < itemArr.length; i++) {
				String[] item = itemArr[i].split(":");
				sb.append(item[0]);
				if(i!=itemArr.length-1){
					sb.append(",");
				}
				
			}		
			return sb.toString();
		}
		
		protected String parseUserTagTopN(String topN){
			
			String[] itemArr = topN.split(",");
			StringBuffer sb = new StringBuffer(200);
			for (int i = 0; i < itemArr.length; i++) {
				String[] item = itemArr[i].split(":");
				sb.append(item[0]);
				if(i!=itemArr.length-1){
					sb.append(",");
				}
				
			}		
			return sb.toString();
		}
	}
	
	
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		Map<String,List<String>> tagTopN;
		Set<String> hasPlaySet = new HashSet<String>();
		List<String> recSet = new ArrayList<String>();	
		Set<String> result = new HashSet<String>();
		int index[];
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			
			tagTopN = TagCF.getTagTopNMap();
		}

		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int size = Integer.parseInt(PropertiesUtils.getCfNumOfRec());
			hasPlaySet.clear();
			recSet.clear();
			
			for(Text line:values){
				if(line.toString().startsWith("01_")){
					hasPlaySet.add(line.toString().substring(3, line.toString().length()));					
				}else if(line.toString().startsWith("02_")){
					String[] list = line.toString().substring(3, line.toString().length()).split(",");
					for(String tagId:list ){
						recSet.addAll(tagTopN.get(tagId));					
					}					
				}				
			}
			
			recSet.removeAll(hasPlaySet);
			
			if (recSet.size() > 0) {
				
				size = recSet.size() > size ? size : recSet.size();
				index = RandomUtils.randomArray(0, recSet.size() - 1, size);

				result.clear();
				for (int i = 0; i < size; i++) {
					result.add(recSet.get(index[i]));
				}

				Iterator<String> ite = result.iterator();
				StringBuffer sb = new StringBuffer(200);
				sb.append(key.toString() + "\t");
				sb.append("[");
				while (ite.hasNext()) {
					sb.append("{");
					sb.append("\"" + ite.next() + "\":" + "0");
					sb.append("}");
					sb.append(",");
				}

				context.write(null,new Text(sb.substring(0, sb.toString().length() - 1)+ "]"));
			}
			
						
		}				
		
	}
}
