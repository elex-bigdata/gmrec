 package com.elex.gmrec.etl;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;


public class Rating extends Configured implements Tool  {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Rating(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String output;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
        conf = HBaseConfiguration.create(conf);
        Job job = Job.getInstance(conf,"DayRatingETL");
        job.setJarByClass(Rating.class);
        long now;
        long before;
        
        if(!PropertiesUtils.getIsInit()){
        	now =System.currentTimeMillis();
            before = now - Long.valueOf(24L*60L*60L*1000L);           
        }else{
        	now = sdf.parse(PropertiesUtils.getInitEndDate()).getTime();
        	before = sdf.parse(PropertiesUtils.getInitStartDate()).getTime();        	
        }                       
        
        List<Scan> scans = new ArrayList<Scan>();  
               
		Scan hbScan = new Scan();
		hbScan.setStartRow(Bytes.add(Bytes.toBytes("hb"), Bytes.toBytes(before)));
		hbScan.setStopRow(Bytes.add(Bytes.toBytes("hb"), Bytes.toBytes(now)));
		hbScan.setCaching(500);
		hbScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("gm_user_action"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("gt"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("l"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("cl"));
		scans.add(hbScan);
		
		Scan upScan = new Scan();
		upScan.setStartRow(Bytes.add(Bytes.toBytes("up"), Bytes.toBytes(before)));
		upScan.setStopRow(Bytes.add(Bytes.toBytes("up"), Bytes.toBytes(now)));
		upScan.setCaching(500);
		upScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("gm_user_action"));
		upScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("gt"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("l"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("cl"));
		scans.add(upScan);
		
		Scan doScan = new Scan();
		doScan.setStartRow(Bytes.add(Bytes.toBytes("do"), Bytes.toBytes(before)));
		doScan.setStopRow(Bytes.add(Bytes.toBytes("do"), Bytes.toBytes(now)));
		doScan.setCaching(500);
		doScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("gm_user_action"));
		doScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("gt"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("l"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("cl"));
		scans.add(doScan);
		
		
		TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class,Text.class, Text.class, job);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		if(PropertiesUtils.getIsInit()){
			output = PropertiesUtils.getRatingFolder()+Constants.INITFOLDER;
			HdfsUtils.delFile(fs, output);
			FileOutputFormat.setOutputPath(job, new Path(output));
		}else{
			output = PropertiesUtils.getRatingFolder()+"/"+sdf.format(new Date(before)).substring(0, 11).replace("-", "").trim();
			HdfsUtils.delFile(fs, output);
			FileOutputFormat.setOutputPath(job, new Path(output));
		}
		
		job.setOutputFormatClass(TextOutputFormat.class);
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class MyMapper extends TableMapper<Text, Text> {
		private HTable gm;
		private Configuration configuration;
		private String gid;
		private String uid;
		private String[] ugid;
		private String gmType = null;
		private String actionType = "";
		private String lang = "";
		private Text uidKey = new Text();
		private Text mixValue =new Text();
		private Set<String> allGM = new HashSet<String>();
		private Date dayTime = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			configuration = HBaseConfiguration.create();
			gm = new HTable(configuration, "gm_gidlist");
			gm.setAutoFlush(false);
			Scan s = new Scan();
			s.setCaching(500);
			s.addColumn(Bytes.toBytes("gm"), Bytes.toBytes("gt"));
			ResultScanner rs = gm.getScanner(s);
			for (Result r : rs) {
				if(!r.isEmpty()){
					allGM.add(Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-1)));											
				}
			}
		}

		

		@Override
		protected void map(ImmutableBytesWritable key, Result r,Context context) throws IOException, InterruptedException {
			
			if (!r.isEmpty()) {
				ugid = Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-10)).split("\u0001");
				if(ugid.length==2){
					uid = ugid[1];
					gid = ugid[0];
				}
				
				actionType = Bytes.toString(Bytes.head(r.getRow(), 2));
				dayTime = new Date(Bytes.toLong(Bytes.tail(Bytes.head(r.getRow(), 10), 8)));
				for (KeyValue kv : r.raw()) {										
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "gt".equals(Bytes.toString(kv.getQualifier()))) {
						gmType = Bytes.toString(kv.getValue());
					}
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "l".equals(Bytes.toString(kv.getQualifier()))) {
						lang = Bytes.toString(kv.getValue());
					}
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "cl".equals(Bytes.toString(kv.getQualifier()))) {
						lang = Bytes.toString(kv.getValue());
					}
				}
			}
			
			gmType=gmType==null?"m":gmType.substring(0, 1);
			
			if(!allGM.contains(gid)){
				Put put = new Put(Bytes.add(Bytes.toBytes(gmType), Bytes.toBytes(gid)));
				put.add(Bytes.toBytes("gm"), Bytes.toBytes("gt"), Bytes.toBytes(gmType));
				gm.put(put);
			}
			
			uidKey.set(Bytes.toBytes(sdf.format(dayTime)+","+uid+","+lang));
			mixValue.set(Bytes.toBytes(gid+","+actionType));
			context.write(uidKey, mixValue);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			gm.flushCommits();
			gm.close();
		}

	}
	
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		Map<String,Integer> gmHbMap = new HashMap<String,Integer>();
		Map<String,Boolean> gmUpDownMap = new HashMap<String,Boolean>();
		Set<String> myGids = new HashSet<String>();
		Iterator<String> ite;
		String[] actions;
		String[] dayUidLang;
		String gid;
		String uid;
		String day;
		String lang = "";
		DecimalFormat df = new DecimalFormat("#.###");
		@Override
		protected void reduce(Text dayUid, Iterable<Text> vList,Context context) throws IOException, InterruptedException {
			gmHbMap.clear();
			gmUpDownMap.clear();
			myGids.clear();
			double rate = 0;
			dayUidLang = dayUid.toString().split(",");
			
			if(dayUidLang.length==3){
				day=dayUidLang[0];
				uid=dayUidLang[1];
				lang = dayUidLang[2].length()>2?dayUidLang[2].substring(0, 2):dayUidLang[2];
				if(lang.contains(",")){
					lang.replace("", "");
				}
		
			}
			
			
			for(Text v:vList){
				actions = v.toString().split(",");
				if(actions.length==2){
					if(!"".equals(actions[1])){
						if("hb".equals(actions[1])){
							gmHbMap.put(actions[0],gmHbMap.get(actions[0])==null?0:gmHbMap.get(actions[0])+1);
						}else if("up".equals(actions[1])){
							gmUpDownMap.put(actions[0], true);
						}else if("do".equals(actions[1])){
							gmUpDownMap.put(actions[0], false);
						}
						
					}
				}
			}
			
			myGids.addAll(gmHbMap.keySet());
			myGids.addAll(gmUpDownMap.keySet());
			
			ite = myGids.iterator();
			while(ite.hasNext()){
				gid = ite.next();
				if(gmHbMap.get(gid)!=null){
					rate = ((double)(gmHbMap.get(gid)*5)/(double)PropertiesUtils.getSatisfyMinute())*10D;					
				}else if(gmUpDownMap.get(gid)){
					rate = 10D;
				}else if(!gmUpDownMap.get(gid)){
					rate = 0D;
				}
				
				rate=rate>10?10:rate;
				context.write(null,new Text(uid.toString()+","+gid+","+df.format(rate)+","+day+","+lang));
			}
			
			
		}
		
	}

}
