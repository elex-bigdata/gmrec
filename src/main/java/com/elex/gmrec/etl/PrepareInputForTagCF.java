package com.elex.gmrec.etl;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;

public class PrepareInputForTagCF extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PrepareInputForTagCF(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		int result = 1;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "prepareInputForTagCF");
		job.setJarByClass(PrepareInputForTagCF.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		Path in = new Path(PropertiesUtils.getRatingFolder() + Constants.MERGEFOLDER);
		FileInputFormat.addInputPath(job, in);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, "hasgid", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tagtopN", TextOutputFormat.class, Text.class, Text.class);
		Path output = new Path(PropertiesUtils.getGmRecRootFolder() + Constants.TAGCFIN);
		HdfsUtils.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);

		result = job.waitForCompletion(true) ? 0 : 1;
		if(result == 0){
			moveTagRankInpupt();
			moveUserTagTopN();
			return 0;
		}else{
			return 1;
		}
		
	}
	
	public static void moveTagRankInpupt() throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		FileStatus[] oldFiles = fs.listStatus(new Path(PropertiesUtils.getGmRecRootFolder() + Constants.TAGRANK),new RankFileFilter());
		
		for (int i = 0; i < oldFiles.length; i++) {
			fs.delete(oldFiles[i].getPath(), true);
		}
				
		FileStatus[] files = fs.listStatus(new Path(PropertiesUtils.getGmRecRootFolder() + Constants.TAGCFIN),new RankFileFilter());
		
		for (int i = 0; i < files.length; i++) {
			HdfsUtils.backupFile(fs,conf,files[i].getPath().toString(), PropertiesUtils.getGmRecRootFolder() + Constants.TAGRANK+files[i].getPath().getName());
			fs.delete(files[i].getPath(), true);			
		}
	}
	
	public static void moveUserTagTopN() throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		FileStatus[] oldFiles = fs.listStatus(new Path(PropertiesUtils.getGmRecRootFolder() + Constants.TAGTOPNIN),new TopNFilter());
		
		for (int i = 0; i < oldFiles.length; i++) {
			fs.delete(oldFiles[i].getPath(), true);
		}
				
		FileStatus[] files = fs.listStatus(new Path(PropertiesUtils.getGmRecRootFolder() + Constants.TAGCFIN),new TopNFilter());
		
		for (int i = 0; i < files.length; i++) {
			HdfsUtils.backupFile(fs,conf,files[i].getPath().toString(), PropertiesUtils.getGmRecRootFolder() + Constants.TAGTOPNIN+files[i].getPath().getName());
			fs.delete(files[i].getPath(), true);			
		}
	}
		

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private HTable gm;
		private Configuration configuration;
		private Map<String,String> GMTagMap = new HashMap<String,String>();
		private String[] vList;
		private String[] tagList;
		private String tags;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			configuration = HBaseConfiguration.create();
			gm = new HTable(configuration, "gm_gidlist");
			gm.setAutoFlush(false);
			Scan s = new Scan();
			s.setCaching(500);
			s.addColumn(Bytes.toBytes("gm"), Bytes.toBytes("tagids"));
			ResultScanner rs = gm.getScanner(s);
			for (Result r : rs) {
				if (!r.isEmpty()) {
					KeyValue kv = r.getColumnLatest(Bytes.toBytes("gm"), Bytes.toBytes("tagids"));
					GMTagMap.put(Bytes.toString(Bytes.tail(r.getRow(),r.getRow().length - 1)),Bytes.toString(kv.getValue()));
				}
			}
			gm.close();
		}


		

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			vList = value.toString().split(",");
			
			if (vList.length == 3) {
				
				tags = GMTagMap.get(vList[1]);
				if(tags!=null){
					tagList = tags.split(":");
					for(String tag:tagList){
						context.write(new Text(vList[0]), new Text(tag+","+vList[2]+","+vList[1]));
					}
				}
							
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		private Map<String,TagAction> userTagActionMap = new HashMap<String,TagAction>();
		private String[] tv;
		private String tag;
		private DecimalFormat df = new DecimalFormat("#.###");
		private MultipleOutputs<Text, Text> mos;  
		private MultipleOutputs<Text, Text> tagTopN;
		private Entry<String, TagAction> entry;
		private String tagId;
		private int times;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);// 初始化mos
			tagTopN = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> ite, Context context)throws IOException, InterruptedException {
			userTagActionMap.clear();	
			int size = PropertiesUtils.getUserTagTopN();
			
			for (Text value : ite) {
				tv = value.toString().split(",");
				if(tv.length==3){
					TagAction ta = userTagActionMap.get(tv[0])!=null?userTagActionMap.get(tv[0]):new TagAction();
					ta.setTimes(1);
					ta.setRate(Double.parseDouble(tv[1]));
					userTagActionMap.put(tv[0], ta);
					mos.write("hasgid", null,new Text(key.toString()+","+tv[0]+","+tv[2])); 
				}				
			}
			
			
			//==============================================================
            List<Map.Entry<String,TagAction>> list = new ArrayList<Map.Entry<String,TagAction>>(userTagActionMap.entrySet());
			
			Collections.sort(list,new Comparator<Map.Entry<String,TagAction>>() {
	            //降序排列
				@Override
				public int compare(Entry<String, TagAction> o1,
						Entry<String, TagAction> o2) {
					return Integer.valueOf(o2.getValue().getTimes()).compareTo(Integer.valueOf(o1.getValue().getTimes()));
				}
	            
	        });
			
			size = list.size()>size?size:list.size();
			
			Iterator<Entry<String, TagAction>> topN = list.subList(0, size).iterator();
			StringBuffer sb = new StringBuffer(200);
			
			while(topN.hasNext()){
				entry = topN.next();
				tagId = entry.getKey();
				times = entry.getValue().getTimes();
				sb.append(tagId+":"+times).append(",");
				
			}
				
			tagTopN.write("tagtopN",new Text(key.toString()), new Text(sb.substring(0,sb.toString().length()-1)));
			
			//===============================================================
			
			Iterator<String> tagIte = userTagActionMap.keySet().iterator();
			
			while(tagIte.hasNext()){
				tag = tagIte.next();
				context.write(null, new Text(key.toString()+","+tag+","+df.format(userTagActionMap.get(tag).getRealRate())));
			}
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,InterruptedException {
			mos.close();// 释放资源
			tagTopN.close();
		}
	}
	
	static class RankFileFilter implements PathFilter{

		@Override
		public boolean accept(Path path) {
			String name = path.getName();
		     return name.startsWith("hasgid");
		}
		
	}
	
	
	static class TopNFilter implements PathFilter{

		@Override
		public boolean accept(Path path) {
			String name = path.getName();
		     return name.startsWith("tagtopN");
		}
		
	}
}
