package cn.com.adrich.flume.mongodb;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class InsertObject {

	private DBObject logClick = null;
	
	private Document logClickDt = null;

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

	public Document getLogClickDt() {

		return logClickDt;
	};
	
	public DBObject getLogClick() {

		return logClick;
	};
}