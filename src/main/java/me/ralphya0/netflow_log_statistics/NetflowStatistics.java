package me.ralphya0.netflow_log_statistics;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.io.IOException;

import java.io.Serializable;
import java.net.URI;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;


	
class RawRecord implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String date;
	String duration;
	String protocol;
	String srcIp;
	String srcPort;
	String dstIp;
	String dstPort;
	String packets;
	String bytes;
	String flows;
	
	RawRecord(String dt,String dura,String pro,String sip,String spt,String dip,String dpt,String packs,String sz,String flws){
		date = dt;
		duration = dura;
		protocol = pro;
		srcIp = sip;
		srcPort = spt;
		dstIp = dip;
		dstPort = dpt;
		packets = packs;
		bytes = sz;
		flows = flws;
	}
}

class Statistics implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String keyIp ; 
	String timestamp; //timestamp of current record
	long fPackets;
	long fBytes;
	long bPackets;
	long bBytes;
	int minFpktLen;
	int maxFpktLen;
	int meanLenFsm;
	double stdLenFqsm;
	int minBpktLen;
	int maxBpktLn;
	int meanLenBsm;
	double stdLenBqsm;
	double Ftotalduration;//
	double Favgduration;
	int FcountdurationMore1;
	int FcountdurationLess1;
	double Btotalduration;//
	double Bavgduration;
	int BcountdurationMore1;
	int BcountdurationLess1;
	long fLess100;
	long bLess100;
	long fLess500;
	long bLess500;
	long fLess1000;
	long bLess1000;
	long fBig1000;
	long bBig1000;
	long FavePktPerSecond;
	long BavePktPerSecond;
	int InnerOneToMulty;
	int OutOneToMulty;
	public Statistics(String ip,String time,long l1,long l2,long l3,long l4,int int1,int int2,int int3,
			double d1,int int4,int int5,int int6,double d2,double d3,double d4,int int7,int int8,double d5,double d6,
			int int9,int int10,long l5,long l6,long l7,long l8,long l9,long l10,long l11,long l12,long l13,long l14,
			int int11,int int12){
		keyIp = ip;
		timestamp = time; 
		fPackets = l1;
		fBytes = l2;
		bPackets = l3;
		bBytes = l4;
		minFpktLen = int1;
		maxFpktLen = int2;
		meanLenFsm = int3;
		stdLenFqsm = d1;
		minBpktLen = int4;
		maxBpktLn = int5;
		meanLenBsm = int6;
		stdLenBqsm = d2;
		Ftotalduration = d3;//
		Favgduration = d4;
		FcountdurationMore1 = int7;
		FcountdurationLess1 = int8;
		Btotalduration = d5;//
		Bavgduration = d6;
		BcountdurationMore1 = int9;
		BcountdurationLess1 = int10;
		fLess100 = l5;
		bLess100 = l6;
		fLess500 = l7;
		bLess500 = l8;
		fLess1000 = l9;
		bLess1000 = l10;
		fBig1000 = l11;
		bBig1000 = l12;
		FavePktPerSecond = l13;
		BavePktPerSecond = l14;
		InnerOneToMulty = int11;
		OutOneToMulty = int12;
	}
	
	public String toString(){
		return keyIp + ',' + timestamp + ',' + fPackets + ',' + fBytes + ',' + bPackets + ',' + bBytes
										+ ',' + minFpktLen + ',' + maxFpktLen + ',' + meanLenFsm + ',' + stdLenFqsm
										+ ',' + minBpktLen + ',' + maxBpktLn + ',' + meanLenBsm + ',' + stdLenBqsm
										+ ',' + Ftotalduration + ',' + Favgduration + ',' + FcountdurationMore1 + ',' + FcountdurationLess1
										+ ',' + Btotalduration + ',' + Bavgduration + ',' + BcountdurationMore1 + ',' + BcountdurationLess1
										+ ',' + fLess100 + ',' + bLess100 + ',' + fLess500 + ',' + bLess500 + ',' + fLess1000
										+ ',' + bLess1000 + ',' + fBig1000 + ',' + bBig1000 + ',' + FavePktPerSecond + ',' + BavePktPerSecond
										+ ',' + InnerOneToMulty + ',' + OutOneToMulty ;
		
	}
	
}

public class NetflowStatistics 
{
	
	
	static List<String> alreadyUsed = new ArrayList<String>();
	
	static List<String> pendingFiles = new ArrayList<String>();
	
    public static void main( String[] args ) throws NumberFormatException, IOException, ClassNotFoundException, SQLException 
    {
    	
    	if(args.length < 4){
    		System.exit(-1);
    	}
    	
    	
    	Connection conn = null;
    	
    	final String master = "yarn-standalone";
    	
    	//test mode or normal mode
    	String runMode = args[0].trim();
    	//where is the netflow logs been hold , hdfs or local file system ?
    	String fileMode = args[1].trim();
    	//url or path to log files
    	String targetDir = args[2].trim();
    	
    	//how to save processing results, local file system or mysql ?
    	String saveMode = args[3].trim();
    	//mysql url or directory path to save results 
    	String arg1 = args[4].trim();
    	//log file for this program which will be used to restore from break point 
    	String logPath = args[5].trim();
    	String arg2 = "";
    	String arg3 = "";
    	String arg4 = "";
    	if(saveMode.equals("mysql")){
    		
    		//mysql users name
    		arg2 = args[6].trim();
    		//password
    		arg3 = args[7].trim();
    		//table name
    		arg4 = args[8].trim();
    		
    		Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(arg1,arg2,arg3);
    	}
    	
    	
    	//try to recover according to program log file 
    	if(logPath != null && logPath.length() > 0){
    		
    		if(new File(logPath).exists()){
    			  		
	    		BufferedReader br = new BufferedReader(new FileReader(logPath));
	    		//format of program log file:
	    		//2014-06-06 12:32:32 > file # xxxxxxx # has been successfully processed(the result is stored into database) within 2 second(s).
	    		String l = "";
	    		while((l = br.readLine()) != null && l.trim().length() > 0){
	    			String [] temp2 = l.split("#");
	    			if(temp2 != null ){
	    				String temp3 = temp2[1].trim();
	    				if(temp3 != null && temp3.length() > 0 && !alreadyUsed.contains(temp3)){
	    					alreadyUsed.add(temp3);
	    				}
	    			}
	    		}
	    		if(br != null)
	    			br.close();
	    		//delete all the incomplete results
	    		if(alreadyUsed.size() > 0){
	    			StringBuilder sql = new StringBuilder();
	    			for(String t : alreadyUsed){
	    				sql.append("'" + t + "',");
	    			}
	    			if(sql.lastIndexOf(",") == sql.length() - 1){
	    				sql.deleteCharAt(sql.length() - 1);
	    				
	    			}
	    			
	    				String delete = "delete from " + arg4 + " where filename not in(" + sql.toString() + ")";
	    				PreparedStatement pss = conn.prepareStatement(delete);
	    				pss.executeUpdate();
	    				pss.close();
	    			
	    		}
    		}
    	}
    	
    	//+++++++++++++++++++++++++++++++++++++++++++++++++++
    	JavaSparkContext sc = new JavaSparkContext(master,
    			"spark-netflow-analysis",
    			System.getenv("SPARK_HOME"),
    			JavaSparkContext.jarOfClass(NetflowStatistics.class));
    	
    	int ct = 0;
    	//waiting and processing new netflow logs until program is killed by user
    	while(true){
    		if(runMode.equals("test") && ++ct >= 3){
    			break;
    		}
    		
    		if(pendingFiles.size() == 0){
    			
    			if(fileMode.equals("local")){
    				File items = new File(targetDir);
    				File[] files = items.listFiles();
    				for(File ff : files){
    					String name = ff.getName();
    					
    					if(ff.isFile() && name.substring(0, 6).equals("nfcapd") && !alreadyUsed.contains(name)){    						
    						pendingFiles.add(name);
    					}
    				}
    				
    			}
    			else if(fileMode.equals("hdfs")){
    				//acquire file list
    				Configuration con = new Configuration();
    				FileSystem fs = FileSystem.get(URI.create(targetDir), con);
    				FileStatus [] lls = fs.listStatus(new Path(targetDir));
    				int szz = lls.length;
    				for(int i = 0;i < szz;i ++){
    					String nn = lls[i].getPath().getName();
    					
    					if(lls[i].isFile() && nn.substring(0, 6).equals("nfcapd") && !alreadyUsed.contains(nn)){
    						pendingFiles.add(nn);
    					}
    				}
    				fs.close();
    			}
    			
    		}
    		
    		if(pendingFiles.size() > 0){
	    			Date begin = new Date();
	    			String fname = pendingFiles.get(0);
    			   			
    				pendingFiles.remove(0);
    				
        			alreadyUsed.add(fname);
        			String filePath = fileMode.equals("local") ? "file:" + targetDir + "/" + fname : targetDir + "/" + fname; 
        			JavaRDD<String> textLine = sc.textFile(filePath);
        			JavaRDD<String> filt = textLine.filter(new Function<String,Boolean>(){
        				
    					@Override
    					public Boolean call(String arg0) throws Exception {
    						
    						return arg0.contains("202.113.") || arg0.contains("59.67.");
    						
    					}
        				
        			});
        			
        			JavaPairRDD<String,RawRecord> pairs = filt.flatMap(new PairFlatMapFunction<String,String,RawRecord>(){

						@Override
						public Iterable<Tuple2<String, RawRecord>> call(String arg0)
								throws Exception {
							
							String [] ls1 = arg0.split(" ");
							String [] ls2 = new String[9];
							int lz = ls1.length;
							int ctt = 0;
							for(int i = 0;i < lz;i ++){
								String tp = ls1[i].trim();
								if(tp != null && tp.length() > 0 && !tp.contains("->")){
									if(tp.equals("M"))
										ls2[ctt - 1] = String.valueOf((long)(Double.parseDouble(ls2[ctt - 1]) * 1024));
									else if(tp.equals("G")){
										ls2[ctt - 1] = String.valueOf((long)(Double.parseDouble(ls2[ctt - 1]) * 1024 * 1024));
									}
									else{
										ls2[ctt] = ls1[i];
										ctt ++;
									}

								}
							}
							
							String [] ls3 = ls2[4].split(":");
							String [] ls4 = ls2[5].split(":");
	
							RawRecord rc = new RawRecord(ls2[0] + " " + ls2[1],ls2[2],ls2[3],ls3[0],ls3[1],ls4[0],ls4[1],ls2[6],ls2[7],ls2[8]);
							String inId ;
							String outId;
							String [] ls5 = ls2[1].split(":");
							String outerip;
							String innerip ;
							if(rc.srcIp.contains("202.113.") || rc.srcIp.contains("59.67.")){
								innerip = rc.srcIp;
								outerip = rc.dstIp;
							}
							else{
								innerip = rc.dstIp;
								outerip = rc.srcIp;
							}
							double duration = Double.parseDouble(rc.duration);
							double seconds = Double.parseDouble(ls5[2]);
							int min = Integer.parseInt(ls5[1]);
							
							int sum = (int)(duration + seconds);
							int minInc = sum / 60;
							int newMin = min + minInc;
							
							//construct record id
							if(newMin < 60){
								if(newMin < 10){
									inId = innerip + "#" + ls2[0] + "-" + ls5[0] + ":0" + newMin;
									outId = outerip + "#" + ls2[0] + "-" + ls5[0] + ":0" + newMin;
								}
								else{
									inId = innerip + "#" + ls2[0] + "-" + ls5[0] + ":" + newMin;
									outId = outerip + "#" + ls2[0] + "-" + ls5[0] + ":" + newMin;
								}
							}
							else{
								int houInc = newMin / 60;
								newMin = newMin % 60;
								String strMin = newMin < 10 ? "0" + String.valueOf(newMin) : String.valueOf(newMin);
								int hou = Integer.parseInt(ls5[0]);
								int newHou = hou + houInc;
								if(newHou < 24){
									if(newHou < 10){
										inId = innerip + "#" + ls2[0] + "-0" + newHou + ":" + strMin;
										outId = outerip + "#" + ls2[0] + "-0" + newHou + ":" + strMin;
									}
									else{
										inId = innerip + "#" + ls2[0] + "-" + newHou + ":" + strMin;
										outId = outerip + "#" + ls2[0] + "-" + newHou + ":" + strMin;
									}
								}
								else{
									int dayInc = newHou / 24;
									newHou = newHou % 24;
									String strHou = newHou < 10 ? "0" + String.valueOf(newHou) : String.valueOf(newHou);
									String [] ls6 = ls2[0].split("-");
									int year = Integer.parseInt(ls6[0]);
									int month = Integer.parseInt(ls6[1]);
									int day = Integer.parseInt(ls6[2]);
									int newDay = dayInc + day;
									if(newDay <= 27){
										String strDay = newDay < 10 ? "0" + String.valueOf(newDay) : String.valueOf(newDay);
										String monStr = month < 10 ? "0" + String.valueOf(month) : String.valueOf(month);
										inId = innerip + "#" + year + "-" + monStr + "-" + strDay + "-" + strHou + ":" 
												+ strMin;
										outId = outerip + "#" + year + "-" + monStr + "-" + strDay + "-" + strHou + ":" 
												+ strMin;
									}
									else{
										
										if(month == 2 && ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)){
											newDay = newDay % 27;
										}
										else if(month == 2 && newDay > 28)
											newDay = newDay % 28;
										else if((month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 || month == 12) 
												&& newDay > 31){
											newDay = newDay % 31;
											if(month == 12){
												year ++;
												month = 0;
											}
										}
										else if((month == 2 || month == 4 || month == 6 || month == 9 || month == 11) && newDay > 30)
											newDay = newDay % 30;
										
											
										month ++;
										String strDay = newDay < 10 ? "0" + String.valueOf(newDay) : String.valueOf(newDay);
										String monStr = month < 10 ? "0" + String.valueOf(month) : String.valueOf(month);
										inId = innerip + "#" + year + "-" + monStr + "-" + strDay + "-" + strHou + ":" + strMin;
										outId = outerip + "#" + year + "-" + monStr + "-" + strDay + "-" + strHou + ":" + strMin;
									}
								}
							}
							
							return Arrays.asList(new Tuple2<String,RawRecord>(inId,rc),new Tuple2<String,RawRecord>(outId,rc));
						}
        			
        			}	);
        			
        			
        			//group records generated above 
        			JavaPairRDD<String,List<RawRecord>> grouped = pairs.groupByKey();
        			
        			JavaRDD<Statistics> result = grouped.map(new Function<Tuple2<String,List<RawRecord>>,Statistics>(){

						@Override
						public Statistics call(
								Tuple2<String, List<RawRecord>> arg0)
								throws Exception {
							List<RawRecord> list = arg0._2();
							int size = list.size();
							String [] keySp = arg0._1().split("#");
							String keyIp = keySp[0];
							

							long fPackets = 0;
							long fBytes = 0;
							long bPackets = 0;
							long bBytes = 0;
							int minFpktLen = -1;
							int maxFpktLen = -1;

							int meanLenFsm = 0;
							double stdLenFqsm = 0;
							int minBpktLen = -1;
							int maxBpktLn = -1;
							int meanLenBsm = 0;
							double stdLenBqsm = 0;
							double Ftotalduration = 0;//
							double Favgduration = 0;
							int FcountdurationMore1 = 0;
							int FcountdurationLess1 = 0;
							double Btotalduration = 0;//
							double Bavgduration = 0;
							int BcountdurationMore1 = 0;
							int BcountdurationLess1 = 0;
							long fLess100 = 0;
							long bLess100 = 0;
							long fLess500 = 0;
							long bLess500 = 0;
							long fLess1000 = 0;
							long bLess1000 = 0;
							long fBig1000 = 0;
							long bBig1000 = 0;
							long FavePktPerSecond = 0;
							long BavePktPerSecond = 0;
							int InnerOneToMulty = 0;
							int OutOneToMulty = 0;
							
							int fCount = 0;
							int bCount = 0;
							//++++++++
							
							
							for(int i = 0;i < size; i ++){
								RawRecord item = list.get(i);
								
								int _bytes = Integer.parseInt(item.bytes);
								int _packets = Integer.parseInt(item.packets);
								double _duration = Double.parseDouble(item.duration);
								
								if(item.srcIp.equals(keyIp)){
									InnerOneToMulty ++;
										
									fCount ++;
									fPackets += _packets;
									fBytes += _bytes;
									
									
									if(minFpktLen == -1)
										minFpktLen = _bytes;
									else if(_bytes < minFpktLen) 
										minFpktLen = _bytes;
									
									if(maxFpktLen == -1)
										maxFpktLen = _bytes;
									else if(_bytes > maxFpktLen)
										maxFpktLen = _bytes;
									
									Ftotalduration += _duration;
									
									if(_duration > 1)
										FcountdurationMore1 ++;
									else
										FcountdurationLess1 ++;
									
									if(_bytes < 100)
										fLess100 ++;
									else if(_bytes < 500)
										fLess500 ++;
									else if(_bytes < 1000)
										fLess1000 ++;
									else
										fBig1000 ++;
									
								}
								else if(item.dstIp.equals(keyIp)){
									
									OutOneToMulty ++;
									
									bCount ++;
									bPackets += _packets;
									bBytes += _bytes;
									
									if(minBpktLen == -1)
										minBpktLen = _bytes;
									else if(_bytes < minBpktLen) 
										minBpktLen = _bytes;
									
									if(maxBpktLn == -1)
										maxBpktLn = _bytes;
									else if(_bytes > maxBpktLn)
										maxBpktLn = _bytes;
									
									Btotalduration += _duration;
									
									if(_duration > 1)
										BcountdurationMore1 ++;
									else
										BcountdurationLess1 ++;
									
									if(_bytes < 100)
										bLess100 ++;
									else if(_bytes < 500)
										bLess500 ++;
									else if(_bytes < 1000)
										bLess1000 ++;
									else
										bBig1000 ++;
								}
								
								
							}
							
							if(minFpktLen == -1)
								minFpktLen = 0;
							if(minBpktLen == -1)
								minBpktLen = 0;
							
							if(maxFpktLen == -1)
								maxFpktLen = 0;
							if(maxBpktLn == -1)
								maxBpktLn = 0;
							
							
							if(fCount != 0)
								meanLenFsm = (int) (fBytes / fCount);
							
							if(bCount != 0)
								meanLenBsm = (int) (bBytes / bCount);
							
							
							long fSum = 0;
							long bSum = 0;
							for(int i = 0;i < size;i ++){
								RawRecord item = list.get(i);
								
								int _bytes = Integer.parseInt(item.bytes);

								
								if(item.srcIp.equals(keyIp)){
									fSum += Math.pow((_bytes - meanLenFsm), 2);
								}
								else if(item.dstIp.equals(keyIp)){
									bSum += Math.pow((_bytes - meanLenBsm), 2);
								}
							}
							if(fCount != 0){
								stdLenFqsm = Math.sqrt(fSum / fCount);
								Favgduration = Ftotalduration / fCount;
							}
							
							if(bCount != 0){
								stdLenBqsm = Math.sqrt(bSum / bCount);
								Bavgduration = Btotalduration / bCount;
							}
							
							
							FavePktPerSecond = fBytes / 60;
							BavePktPerSecond = bBytes / 60;
							
							return new Statistics(keyIp,keySp[1],fPackets,fBytes,bPackets,bBytes,minFpktLen,
									maxFpktLen,meanLenFsm,stdLenFqsm,minBpktLen,maxBpktLn,meanLenBsm,
									stdLenBqsm,Ftotalduration,Favgduration,FcountdurationMore1,FcountdurationLess1,
									Btotalduration,Bavgduration,BcountdurationMore1,BcountdurationLess1,fLess100,bLess100,
									fLess500,bLess500,fLess1000,bLess1000,fBig1000,bBig1000,FavePktPerSecond,BavePktPerSecond,
									InnerOneToMulty,OutOneToMulty);
						}
        			
        			});
        			
        			if(saveMode.equals("file")){
        				//write results into local file on disk
        				String t1 = fname.substring(0, fname.lastIndexOf('.'));
        				String t2 = t1.substring(t1.lastIndexOf('.') + 1, t1.length());
        				String infilename = "inner_" + t2 + ".txt";
        				String outfilename = "outer_" + t2 + ".txt";
        				List<Statistics> collection = result.collect();
        				int sz1 = collection.size();
        				StringBuilder isb = new StringBuilder();
        				StringBuilder osb = new StringBuilder();
        				for(int i = 0;i < sz1;i ++){
        					Statistics ss = collection.get(i);
        					if(ss.keyIp.contains("202.113.") || ss.keyIp.contains("59.67."))
        						isb.append(ss.toString() + '\n');
        					else
        						osb.append(ss.toString() + '\n');
        				}
        				BufferedWriter bw = new BufferedWriter(new FileWriter(arg1 + infilename));
        				bw.write(isb.toString());
        				bw.close();
        				BufferedWriter bw2 = new BufferedWriter(new FileWriter(arg1 + outfilename));
        				bw2.write(osb.toString());
        				bw2.close();
        				continue;
        			}
        			else if(saveMode.equals("mysql")){
        				//insert results into mysql
        				List<Statistics> collection = result.collect();
        				int sz1 = collection.size();
        				String sql = "";
						sql = "insert into " + arg4 + "(filename,keyIp, tstamp, fPackets, fBytes, bPackets, bBytes" 
								+ ", minFpktLen , maxFpktLen , meanLenFsm, stdLenFqsm"
								+ ", minBpktLen, maxBpktLn, meanLenBsm, stdLenBqsm"
								+ ", Ftotalduration, Favgduration, FcountdurationMore1, FcountdurationLess1"
								+ ", Btotalduration, Bavgduration, BcountdurationMore1, BcountdurationLess1"
								+ ", fLess100, bLess100, fLess500, bLess500, fLess1000"
								+ ", bLess1000, fBig1000, bBig1000, FavePktPerSecond, BavePktPerSecond"
								+ ", InnerOneToMulty, OutOneToMulty) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        				
						conn.setAutoCommit(false);
        				PreparedStatement ps = conn.prepareStatement(sql);
        				
        				for(int i = 0;i < sz1;i ++){
        					Statistics ss = collection.get(i);
        					ps.setString(1, fname);
        					ps.setString(2, ss.keyIp);
        					ps.setString(3, ss.timestamp);
        					ps.setLong(4, ss.fPackets);
        					ps.setLong(5, ss.fBytes);
        					ps.setLong(6, ss.bPackets);
        					ps.setLong(7, ss.bBytes);
        					ps.setInt(8, ss.minFpktLen);
        					ps.setInt(9, ss.maxFpktLen);
        					ps.setDouble(10, ss.meanLenFsm);
        					ps.setDouble(11, ss.stdLenFqsm);
        					ps.setInt(12, ss.minBpktLen);
        					ps.setInt(13, ss.maxBpktLn);
        					ps.setDouble(14, ss.meanLenBsm);
        					ps.setDouble(15, ss.stdLenBqsm);
        					ps.setDouble(16, ss.Ftotalduration);
        					ps.setDouble(17, ss.Favgduration);
        					ps.setInt(18, ss.FcountdurationMore1);
        					ps.setInt(19, ss.FcountdurationLess1);
        					ps.setDouble(20, ss.Btotalduration);
        					ps.setDouble(21, ss.Bavgduration);
        					ps.setInt(22, ss.BcountdurationMore1);
        					ps.setInt(23, ss.BcountdurationLess1);
        					ps.setLong(24, ss.fLess100);
        					ps.setLong(25, ss.bLess100);
        					ps.setLong(26, ss.fLess500);
        					ps.setLong(27, ss.bLess500);
        					ps.setLong(28, ss.fLess1000);
        					ps.setLong(29, ss.bLess1000);
        					ps.setLong(30, ss.fBig1000);
        					ps.setLong(31, ss.bBig1000);
        					ps.setLong(32, ss.FavePktPerSecond);
        					ps.setLong(33, ss.BavePktPerSecond);
        					ps.setInt(34, ss.InnerOneToMulty);
        					ps.setInt(35, ss.OutOneToMulty);
        					
        					ps.addBatch();
        					
        				}
        				
        				ps.executeBatch();
        				
        				conn.commit();
        				
        				ps.close();
        				
        				//update program log file --this means that a netflow log file is been processed successfully
        	    		//2014-06-06 12:32:32 > file # xxxxxxx # has been successfully processed(the result is stored into database) within 2 second(s).
        				Date end = new Date();
        				StringBuilder logs = new StringBuilder();
        				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        				String t = format.format(end);
        				logs.append(t + " > file # " + fname + " # has been successfully processed(the result is store into database) within " + (end.getTime() - begin.getTime())/1000 + " second(s)." + '\n');
        				BufferedWriter rt = new BufferedWriter(new FileWriter(logPath,true));
        				rt.write(logs.toString());
        				rt.close();
        				continue;
        			}
    		}
    		else{
    			break ;
    			/*try {
    			Thread.sleep(60000);
    			continue;
    			} catch (InterruptedException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    			}*/
    		}
    	}
    	
    	if(conn != null){
    		conn.close();
    	}
    }
}

