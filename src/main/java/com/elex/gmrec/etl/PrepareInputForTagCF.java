package com.elex.gmrec.etl;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
		Path output = new Path(PropertiesUtils.getGmRecRootFolder() + Constants.TAGCFIN);
		HdfsUtils.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
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
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);// 初始化mos
		}

		@Override
		protected void reduce(Text key, Iterable<Text> ite, Context context)throws IOException, InterruptedException {
			userTagActionMap.clear();	
			
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
			
			Iterator<String> tagIte = userTagActionMap.keySet().iterator();
			
			while(tagIte.hasNext()){
				tag = tagIte.next();
				context.write(null, new Text(key.toString()+","+tag+","+df.format(userTagActionMap.get(tag).getRealRate())));
			}
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,InterruptedException {
			mos.close();// 释放资源
		}
	}
}
