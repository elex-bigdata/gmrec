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
	
	
	public static String getRatingFolder(){
		return pop.getProperty("ratingFolder");
	}
	
	public static String getGmRecRootFolder(){
		return pop.getProperty("ratingFolder").substring(0,pop.getProperty("ratingFolder").indexOf("/", 1));
	}
	
	public static int getMergeDays(){
		return Integer.parseInt(pop.getProperty("mergeDays"));
	}
}
