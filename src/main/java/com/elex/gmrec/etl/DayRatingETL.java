package com.elex.gmrec.etl;

import java.io.IOException;
import java.util.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import com.elex.gmrec.comm.PropertiesUtils;


public class DayRatingETL extends Configured implements Tool  {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new DayRatingETL(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Configuration conf = new Configuration();
        conf = HBaseConfiguration.create(conf);
        Job job = Job.getInstance(conf,"DayRatingETL");
        job.setJarByClass(DayRatingETL.class);
        long now;
        long before;
        byte[] startRow=null;
        byte[] stopRow=null;
        
        if(!PropertiesUtils.getIsInit()){
        	now =System.currentTimeMillis();
            before = now - Long.valueOf(24L*60L*60L*1000L);           
        }else{
        	now = sdf.parse(PropertiesUtils.getInitEndDate()).getTime();
        	before = sdf.parse(PropertiesUtils.getInitStartDate()).getTime();        	
        }
        
        startRow = Bytes.add(Bytes.toBytes("aa"), Bytes.toBytes(before));
        stopRow = Bytes.add(Bytes.toBytes("zz"), Bytes.toBytes(now));
        //startRow = Bytes.add(Bytes.toBytes("hb"), Bytes.toBytes(before));
        //stopRow = Bytes.add(Bytes.toBytes("hb"), Bytes.toBytes(now));
               
		Scan s = new Scan();
		
		s.setStartRow(startRow);
		s.setStopRow(stopRow);
		s.setCaching(500);
		s.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("gid"));
		s.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("gt"));
		
		TableMapReduceUtil.initTableMapperJob("gm_user_action", s, MyMapper.class,Text.class, Text.class, job);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
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
				}
			}
			
			gmType=gmType==null?"m":gmType.substring(0, 1);
			
			if(!allGM.contains(gid)){
				Put put = new Put(Bytes.add(Bytes.toBytes(gmType), Bytes.toBytes(gid)));
				put.add(Bytes.toBytes("gm"), Bytes.toBytes("gt"), Bytes.toBytes(gmType));
				gm.put(put);
			}
			
			uidKey.set(Bytes.toBytes(sdf.format(dayTime)+","+uid));
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
		String gid;
		String uid;
		String day;
		DecimalFormat df = new DecimalFormat("#.###");
		@Override
		protected void reduce(Text dayUid, Iterable<Text> vList,Context context) throws IOException, InterruptedException {
			gmHbMap.clear();
			gmUpDownMap.clear();
			myGids.clear();
			double rate = 0;
			day=dayUid.toString().split(",")[0];
			uid=dayUid.toString().split(",")[1];
			
			
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
				
				context.write(null,new Text(uid.toString()+","+gid+","+df.format(rate)+","+day));
			}
			
			
		}
		
	}

}
