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
	 * 功能：先将TagCf的输出和Constants.USERTOPTAG的数据去重合并，然后取对应tag的topN个gid再去重合并还要过滤掉已经玩过的gid，最后随机去取user.rec.number个gid输出。
	 * 输入1：Constants.MERGEFOLDER，格式为uid，gid，打分
	 * 输入2：Constants.TAGCFOUTPUT，格式为uid，tagid，预测打分
	 * 输入3：Constants.USERTOPTAG，格式为uid，tagid，次数
	 * 输出：Constants.TAGCFRECFINAL，格式为uid，json数组对象，每个对象的key为gid，value为0（为了和gmrec-1.0的格式契合）
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
		Map<Integer,String> uidMap;//用户索引号和用户id对应表，key为整形的用户索引号，value为用户id
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			uidMap = IDMapping.getUidIntStrMap();
		}

		/*
		 * 由于三种输入的格式不一样，需要分别对待和处理，办法是：
		 * 对输入1进行加“01_”的前缀标识，将输入2和输入3的value解析为同样形式并加“02_”标识。
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(pathName.contains(Constants.MERGEFOLDER)){
				list = value.toString().split(",");
				context.write(new Text(list[0]), new Text("01_"+list[1]));
			}else if(pathName.contains(Constants.TAGCFOUTPUT)){
				list = value.toString().split("\\s");
				//由于tagCf的输出中uid为整形索引号，在此需要还原
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
		Map<String,List<String>> tagTopN;//每个tag对应的topN个游戏组成的List
		Set<String> hasPlaySet = new HashSet<String>();//读入带有"01_"标识的数据(原始打分数据)，用户已玩过的游戏列表
		Set<String> recSet = new HashSet<String>();	//所有可以给用户推荐的游戏列表
		List<String> result = new ArrayList<String>();//将resSet转为List，用直接转换的方式会重复生成列表对象，占用内存
		int index[];//随机选择result的size个下标构成的数组
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			
			tagTopN = TagCF.getTagTopNMap();
		}

		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int size = Integer.parseInt(PropertiesUtils.getUserRecNumber());
			hasPlaySet.clear();
			recSet.clear();
			
			for(Text line:values){
				if(line.toString().startsWith("01_")){
					hasPlaySet.add(line.toString().substring(3, line.toString().length()));					
				}else if(line.toString().startsWith("02_")){
					String[] list = line.toString().substring(3, line.toString().length()).split(",");
					for(String tagId:list ){
						if(tagTopN.get(tagId)!=null){
							if(tagTopN.get(tagId).size()>size){
								recSet.addAll(RandomUtils.randomTopN(size, tagTopN.get(tagId)));								
							}else{
								recSet.addAll(tagTopN.get(tagId));
							}							
						}											
					}					
				}				
			}
			
			recSet.removeAll(hasPlaySet);
			
			if (recSet.size() > 0) {
				
				size = recSet.size() > size ? size : recSet.size();
				index = RandomUtils.randomArray(0, recSet.size() - 1, size);

				result.clear();
				Iterator<String> ite = recSet.iterator();
				while(ite.hasNext()){
					result.add(ite.next());
				}
				
				StringBuffer sb = new StringBuffer(200);
				sb.append(key.toString() + "\t");
				sb.append("[");
				for (int i = 0; i < index.length; i++) {
					sb.append("{");
					sb.append("\"" + result.get(index[i]) + "\":" + "0");
					sb.append("}");
					sb.append(",");
				}								

				context.write(null,new Text(sb.substring(0, sb.toString().length() - 1)+ "]"));
			}
			
						
		}				
		
	}
}
