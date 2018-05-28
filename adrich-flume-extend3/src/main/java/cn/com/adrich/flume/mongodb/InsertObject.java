package cn.com.adrich.flume.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class InsertObject {

	private DBObject logClick = null;
	
	private Document logClickDt = null;
	
	private Document logDeviceDt = null;

	public Document getLogDeviceDt() {
		return logDeviceDt;
	}

	public void setLogDeviceDt(Document logDeviceDt) {
		this.logDeviceDt = logDeviceDt;
	}

	public InsertObject() {
		logClick = new BasicDBObject();
		logClick.put("logType", "log_click");
		logClick.put("isBanner", "banner");
		logClick.put("sspCode", "_004");
		logClick.put("siteName", "ee7717af");
		logClick.put("materialID", "10_00041jJrVbDCPYW2EtxqUUTu1");
		logClick.put("isPC", "IsAPPMark");
		logClick.put("userID", "e71d101fdbfb542fedf7ebd34c79c691_004");
		logClick.put("siteNameURL", "ee7717af");
		logClick.put("requestTime", "1506055842141");
		logClick.put("cityName", "foshan");
		//logClick.put("requestID", "6b83eb2954d6b017_004");
		logClick.put("planID", "10_0008j5djvRUFgVx7wG4Q6aqii");
		logClick.put("optTime", 1523586591461L);
		logClick.put("impAdID", "-8329255953968915228_004");
		logClick.put("log_time", 2018041310);
		//logClick.put("requestTimeID", "6b83eb2954d6b017_004_1523586591461");
		
		
		logClickDt = new Document();
		logClickDt.put("logType", "log_click");
		logClickDt.put("isBanner", "banner");
		logClickDt.put("sspCode", "_004");
		logClickDt.put("siteName", "ee7717af");
		logClickDt.put("materialID", "10_00041jJrVbDCPYW2EtxqUUTu1");
		logClickDt.put("isPC", "IsAPPMark");
		logClickDt.put("userID", "e71d101fdbfb542fedf7ebd34c79c691_004");
		logClickDt.put("siteNameURL", "ee7717af");
		logClickDt.put("requestTime", "1506055842141");
		logClickDt.put("cityName", "foshan");
		logClickDt.put("requestID", "6b83eb2954d6b017_004");
		logClickDt.put("planID", "10_0008j5djvRUFgVx7wG4Q6aqii");
		logClickDt.put("optTime", 1523586591461L);
		logClickDt.put("impAdID", "-8329255953968915228_004");
		logClickDt.put("log_time", 2018041310);
		logClickDt.put("requestTimeID", "6b83eb2954d6b017_004_1523586591461");
		

	};
	
	public InsertObject(String Device) {
		if(Device!="")
		logDeviceDt = new Document();
		logDeviceDt.put("scid_dmpcode", "key002");//分片片键；hash，唯一索引
		logDeviceDt.put("sspcode", "_0004");//Ssp标识
		logDeviceDt.put("dmp_code", "DMP0001");//DMP标识，移动流量用
		logDeviceDt.put("dmp_cid", "123456789_123456789_123456789_12");
		logDeviceDt.put("dcid", "123456789_123456789_123456789_12");//Dsp Cookie ID，普通索引
		logDeviceDt.put("scid", "123456789_123456789_123456789_12");//Ssp Cookie ID
		//int
		logDeviceDt.put("cookie_mapping_in", 55);//1:cookie mapping插入，   
		logDeviceDt.put("isApp", "123456789_123456789_123456789_12");//是否为移动流量，如"IsPCMark"; "IsAPPMark";

		//array of string 
		logDeviceDt.put("ip", Arrays.asList(new String[]{"10.0.0.101","10.0.0.103","10.0.0.104","10.0.0.102","10.0.0.105"}));// 增量，去重  数组（solr中ip；mongoDB中ip属性的值是city）
		List<String> ips = new ArrayList<String>(100);
		for(int i=0;i<5;i++)ips.add("10.0.0."+i);
		logDeviceDt.put("ip",ips);
		logDeviceDt.put("city", Arrays.asList(new String[]{"北京","上海","呼和浩特","呼和浩特1","呼和浩特","呼和浩特2"}));
		logDeviceDt.put("keywords", Arrays.asList(new String[]{"三个字","只能五个字","最多五个字","只能3个字","最多2个字"}));//关键字      增量，去重  数组   暂时为空
		logDeviceDt.put("userAttributeList", Arrays.asList(new String[]{"男","20-30岁","女","40-50岁","70-80岁"}));//原始用户属性和兴趣
		
		logDeviceDt.put("did", "123456789_123456789_123456789_12");//IMEI，不同SSP有的 MD5 哈希 ,有的没有
		logDeviceDt.put("dpid", "123456789_123456789_123456789_12");//原始的平台相关 ID，Android ID 或 iOS 的 UDID，或者 windows phone 的 id，不同SSP有的 MD5 哈希 ,有的没有
		logDeviceDt.put("idfa", "123456789_123456789_123456789_12");//广告标示符，适用于对外， iOS 的 IDFA 字 段 ，如： ”1E2DFA89-496A-47FD-9941-D F1FC4E6484A”
		logDeviceDt.put("make", "123456789_123456789_123456789_12");//设备生产商，如”Apple” 
		logDeviceDt.put("model", "123456789_123456789_123456789_12");//设备型号，如”iPhone” 
		logDeviceDt.put("os", "123456789_123456789_123456789_12");//操作系统,1:IOS,2Andoid,3Windows Phone,0,其它 
		logDeviceDt.put("devicetype", "123456789_123456789_123456789_12");//目前包括以下四种情况： ANDROID、IOS、WP 、OTHERS
		logDeviceDt.put("macmd5", "123456789_123456789_123456789_123456789_123456789_123456789_1234");//MD5 哈希的 MAC 地址
		logDeviceDt.put("mac", "123456789_123456789_123456789_123456789_123456789_123456789_1234");//MAC 地址，如“F0B4799A8CC9”
		//Number
		logDeviceDt.put("updatetime", new Date().getTime());//更新时间

	};

	public Document getLogClickDt() {

		return logClickDt;
	};
	
	public DBObject getLogClick() {

		return logClick;
	};
}