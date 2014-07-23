package com.elex.gmrec.comm;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class PropertiesUtils {

	private static Properties pop = new Properties();
	static{
		InputStream is = null;
		try{
			is = PropertiesUtils.class.getClassLoader().getResourceAsStream("config.properties");
			pop.load(is);
		}catch(Exception e){
			e.printStackTrace();
			
		}finally{
			try {
				if(is!=null)is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static int getMinutePerPopint(){
		return Integer.parseInt(pop.getProperty("minutePerPopint"));
	}
	
	public static double getTagRankTopRate(){
		return Double.parseDouble(pop.getProperty("tag.rank.top.rate"));
	}
	
	public static boolean getIsInit(){
		return pop.getProperty("isInit").equals("T")?true:false;
	}
	
	public static String getInitStartDate(){
		return pop.getProperty("initStartDate");
	}
	
	public static String getInitEndDate(){
		return pop.getProperty("initEndDate");
	}
	
	public static int getSatisfyMinute(){
		return Integer.parseInt(pop.getProperty("satisfyMinute"));
	}
	
	public static int getUserTagTopN(){
		return Integer.parseInt(pop.getProperty("user.tag.topN"));
	}
	
	
	public static String getRatingFolder(){
		return pop.getProperty("ratingFolder");
	}
	
	public static String getGmRecRootFolder(){
		return pop.getProperty("ratingFolder").substring(0,pop.getProperty("ratingFolder").indexOf("/", 1));
	}
	
	public static int getMergeDays(){
		return Integer.parseInt(pop.getProperty("mergeDays"));
	}
	
	public static String getCfNumOfRec(){
		return pop.getProperty("cf.numOfRec");
	}
	
	public static String getTagCfNumOfRec(){
		return pop.getProperty("tagcf.numOfRec");
	}
	
	public static String getCfSimilarityClassname(){
		return pop.getProperty("cf.SimilarityClassname");
	}
	
	public static String getMinSupport(){
		return pop.getProperty("fi.minSupport");
	}
	
	public static String getNumberFrequentItem(){
		return pop.getProperty("fi.numberFrequentItem");
	}

	public static double getConfidence() {
		
		return Double.parseDouble(pop.getProperty("arule.confidence"));
	}
	
	public static String getTopN(){
		return pop.getProperty("cfsim.topN");
	}
	
	public static String getThreshold(){
		return pop.getProperty("cfsim.threshold");
	}
	
	public static String getTagFile(){
		return pop.getProperty("tagFile");
	}

}
