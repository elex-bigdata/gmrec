import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

import com.elex.gmrec.comm.PropertiesUtils;
import com.elex.gmrec.etl.RatingMergeETL;


public class Test {

	/**
	 * @param args
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ParseException, IOException {
		/*long now;
        long before;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        now = sdf.parse(PropertiesUtils.getInitEndDate()).getTime();
    	before = sdf.parse(PropertiesUtils.getInitStartDate()).getTime();
    	System.out.println(now+","+before);
    	System.out.println(sdf.format(new Date(now)));*/
		
		/*Set<String> set = new HashSet<String>();
		String gid = "tom_cat_water_gun_war";
		String gmType="m";
		byte[] key = Bytes.add(Bytes.toBytes(gmType), Bytes.toBytes(System.currentTimeMillis()), Bytes.toBytes(gid));
		String ngid = Bytes.toString(Bytes.tail(key, key.length-9));
		System.out.println(ngid);
		set.add(ngid);
		if(set.contains(gid)){
			System.out.println("yes");
		}*/
		
		/*DecimalFormat df = new DecimalFormat("#.###");
		System.out.println(df.format(287.98576D));*/
		
		/*SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(sdf.format(new Date(System.currentTimeMillis())).substring(0, 11).replace("-", ""));*/
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path[] dirs = new RatingMergeETL().getMergeInputFolders(fs);
		for(Path dir:dirs){
			System.out.println(dir.toString());
		}
	}

}
