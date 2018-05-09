package cn.com.adrich.flume.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

public enum MongodbCrud {

	;

	public static final String MONGODB_WIN = "log_win";
	public static final String MONGODB_EXPOS = "log_expos";
	public static final String MONGODB_CLICK = "log_click";
	public static final String MONGODB_SUMMARY = "log_summary";
	public static final String MONGODB_EXCEPTION = "log_exception_logs";
	private static final Logger logger = LoggerFactory.getLogger(MongodbCrud.class);

	/**
	 * MongoDB错误代码：主键重复
	 */
	public static final String ERROR_CODE_DUPLICATE_KEY = "E11000";

	public static final String MONGODB_URI_KEY = "mongodb.uri";
	public static final String MONGODB_DATABASENAME = "mongodb.databasename";

	private static MongoClient MONGO_CLIENT;
	private static MongoDatabase DATABASE;
	private static Map<String, MongoCollection<Document>> COLLECTION_MAP;

	// 读取配置文件、初始化客户端、数据库、集合连接
	static {

		InputStream resourceStream = null;
		try {
			// 读取配置文件
			resourceStream = MongodbCrud.class.getResourceAsStream("/mongodb-config.properties");

			Properties p = new Properties();
			p.load(resourceStream);

			// MongoDB 连接字符串
			String uri = p.getProperty(MONGODB_URI_KEY);
			MongoClientURI mongoClientURI = new MongoClientURI(uri);
			MONGO_CLIENT = new MongoClient(mongoClientURI);

			// 要连接的数据库
			String databaseName = p.getProperty(MONGODB_DATABASENAME);
			DATABASE = MONGO_CLIENT.getDatabase(databaseName);

			// 将库中已存在的集合放入 Map 中缓存
			COLLECTION_MAP = new ConcurrentHashMap<String, MongoCollection<Document>>();
			MongoIterable<String> collectionNames = DATABASE.listCollectionNames();
			for (String collectionName : collectionNames) {
				MongoCollection<Document> collection = DATABASE.getCollection(collectionName);
				COLLECTION_MAP.put(collectionName, collection);
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (resourceStream != null) {
				try {
					resourceStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public synchronized static void closeMongoClient() {

		if (MONGO_CLIENT != null) {
			MONGO_CLIENT.close();
		}
	}

	/**
	 * 从集合缓存中获取指定的集合连接。
	 * 
	 * @param dmpCollection
	 *            要获取的集合。
	 * @return 集合连接。
	 */
	private static MongoCollection<Document> getCollection(String collectionName) {

		MongoCollection<Document> collection = COLLECTION_MAP.get(collectionName);
		if (collection == null) {
			collection = DATABASE.getCollection(collectionName);
			COLLECTION_MAP.put(collectionName, collection);
		}
		return collection;
	}

	/**
	 * 根据查询条件查询指定集合文档数量。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param dmpCollection
	 *            要查询的集合。
	 * @param filterMap
	 *            查询条件。
	 * @return 指定集合文档数量。
	 */
	public static long countByAndEquals(String collectionName, Map<String, Object> filterMap) {
		MongoCollection<Document> collection = getCollection(collectionName);
		Bson filter = buildFilterByAndEquals(filterMap);
		long count = 0;
		if (filter == null) {
			count = collection.count();
		} else {
			count = collection.count(filter);
		}
		return count;
	}

	/**
	 * 根据查询条件查询指定集合文档是否存在。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param dmpCollection
	 *            要查询的集合。
	 * @param filterMap
	 *            查询条件。
	 * @return 指定集合文档是否存在。
	 */
	public static boolean existsByAndEquals(String collectionName, Map<String, Object> filterMap) {
		long count = countByAndEquals(collectionName, filterMap);
		return count > 0;
	}

	/**
	 * 根据查询条件查询指定集合文档多条内容。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param dmpCollection
	 *            要查询的集合。
	 * @param filterMap
	 *            查询条件。
	 * @return 查询结果游标。
	 */
	public static FindIterable<Document> findByAndEquals(String collectionName, Map<String, Object> filterMap) {
		MongoCollection<Document> collection = getCollection(collectionName);
		Bson filter = buildFilterByAndEquals(filterMap);
		FindIterable<Document> found = null;
		if (filter == null) {
			found = collection.find();
		} else {
			found = collection.find(filter);
		}
		return found;
	}

	/**
	 * 根据查询条件查询指定集合文档单条内容，如果查出多条内容，将返回第一条。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param dmpCollection
	 *            要查询的集合。
	 * @param filterMap
	 *            查询条件。
	 * @return 查询结果文档。
	 */
	public static Document findOneByAndEquals(String collectionName, Map<String, Object> filterMap) {
		FindIterable<Document> found = findByAndEquals(collectionName, filterMap);
		if (found != null) {
			MongoCursor<Document> cursor = found.iterator();
			try {
				if (cursor.hasNext()) {
					Document first = cursor.next();
					return first;
				}
			} finally {
				cursor.close();
			}
		}
		return null;
	}

	/**
	 * 根据查询条件 Map 构建查询条件对象。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param filterMap
	 *            查询条件 Map。
	 * @return 查询条件对象。
	 */
	private static Bson buildFilterByAndEquals(Map<String, Object> filterMap) {
		Bson filter = null;
		if (filterMap != null) {
			for (Entry<String, Object> entry : filterMap.entrySet()) {
				String fieldName = entry.getKey();
				Object value = entry.getValue();
				Bson filterTemp = Filters.eq(fieldName, value);
				if (filter == null) {
					filter = filterTemp;
					continue;
				}
				filter = Filters.and(filter, filterTemp);
			}
		}
		return filter;
	}

	/**
	 * 根据更新 Map 构建更新对象。
	 * 
	 * @param updateMap
	 *            更新 Map。
	 * @return 更新对象。
	 */
	private static Bson buildUpdate(Map<String, Object> updateMap) {
		List<Bson> updates = null;
		if (updateMap != null) {
			updates = new ArrayList<Bson>();
			for (Entry<String, Object> entry : updateMap.entrySet()) {
				String fieldName = entry.getKey();
				Object value = entry.getValue();
				Bson updateTemp = Updates.set(fieldName, value);
				updates.add(updateTemp);
			}
		}
		Bson update = null;
		if (updates != null) {
			update = Updates.combine(updates);
		}
		return update;
	}

	/**
	 * 更新指定集合的符合条件的单条文档。
	 * 
	 * @param dmpCollection
	 *            要更新的集合。
	 * @param filterMap
	 *            更新条件。
	 * @param update
	 *            更新数据。
	 * @return 更新结果。
	 */
	public static UpdateResult updateOneByAndEquals(String collectionName, Map<String, Object> filterMap, Document document) {
		MongoCollection<Document> collection = getCollection(collectionName);
		Bson filter = buildFilterByAndEquals(filterMap);
		Bson update = buildUpdate(document);
		UpdateResult updateResult = collection.updateOne(filter, update);
		return updateResult;
	}

	/**
	 * 更新指定集合的符合条件的单条文档。
	 * 
	 * @param dmpCollection
	 *            要更新的集合。
	 * @param filterMap
	 *            更新条件。
	 * @param update
	 *            更新数据。
	 * @return 更新结果。
	 */
	public static UpdateResult updateOneByAndEqualsUpsert(String collectionName, Map<String, Object> filterMap, Document document) {

		long begin = System.currentTimeMillis();

		MongoCollection<Document> collection = getCollection(collectionName);
		Bson filter = buildFilterByAndEquals(filterMap);
		Bson update = buildUpdate(document);
		UpdateOptions uo = new UpdateOptions();
		uo.upsert(true);
		UpdateResult updateResult = null;
		try {
			updateResult = collection.updateOne(filter, update, uo);
		} catch (Exception e) {
			//如果出错，则尝试在调用一次。
			//在高并发的情况下，会出现主键重复的错误
			try {
				updateResult = collection.updateOne(filter, update, uo);
			} catch (Exception e1) {
				throw e1;
			}
		} finally {
			if (logger.isDebugEnabled()) {
				long cost = System.currentTimeMillis() - begin;
				if (cost > 100) {
					logger.debug("MongoDB updateOneByAndEqualsUpsert slowly cost : {}ms!", cost);
				}
			}
		}
		return updateResult;
	}

	/**
	 * 向指定集合中插入单条文档。
	 * 
	 * @param dmpCollection
	 *            要插入文档的集合。
	 * @param document
	 *            要插入的文档。
	 */
	public static void insertOne(String collectionName, Document document) {

		long begin = System.currentTimeMillis();

		MongoCollection<Document> collection = getCollection(collectionName);
		collection.insertOne(document);

		if (logger.isDebugEnabled()) {
			long cost = System.currentTimeMillis() - begin;
			if (cost > 100) {
				logger.debug("MongoDB insertOne slowly cost : {}ms!", cost);
			}
		}
	}

	public static void main(String[] args) throws Throwable {

		//		int N = 200;
		//		CountDownLatch startSignal = new CountDownLatch(1);
		//		CountDownLatch doneSignal = new CountDownLatch(N);

		//	     for (int i = 0; i < N; ++i) // create and start threads
		//	       new Thread(new Worker(startSignal, doneSignal)).start();

		//	     doSomethingElse();            // don't let run yet
		//	     startSignal.countDown();      // let all threads proceed
		//	     doSomethingElse();
		//	     doneSignal.await();           // wait for all to finish

		//		document.put("userid","userid - abc123ddfassadf");
		//		document.put("opttime","1234567890");
		//		document.put("sspcode","005");
		//		
		////		insertOne(LogConstantsForExpos.MONGODB_WIN,document);
		//		Map<String,Object> filterMap = new HashMap<String,Object>();
		//		filterMap.put("requestID", "abc123ddfassadf");
		//		Document documentInDB = MongodbCrud.findOneByAndEquals(LogConstantsForExpos.MONGODB_WIN, filterMap);
		//		System.out.println(documentInDB);
		//		MongodbCrud.updateOneByAndEquals(LogConstantsForExpos.MONGODB_WIN, filterMap, document);
		//		documentInDB = MongodbCrud.findOneByAndEquals(LogConstantsForExpos.MONGODB_WIN, filterMap);
		//		System.out.println(documentInDB);
/*		String collectionName = "ttt";
		Document document = new Document();
		document.put("_id", "123");*/
		try {
			
			long begin = System.currentTimeMillis();
			List<DBObject> dbObjects = new ArrayList<DBObject>();
			for (int i = 0; i < 1000000; i++) {
				Document dt = new Document();
				dt.putAll((new InsertObject()).getLogClickDt());
				dt.put("random", i);
				insertOne(MONGODB_EXPOS, dt);
				insertSummary(dt, "exposcount");
				/*				insertOne(MONGODB_CLICK, dt);
				insertSummary(dt, "clickcount");
				insertOne(MONGODB_WIN, dt);
				insertSummary(dt, "wincount");*/
/*				if(i%10000==0)
					Thread.sleep(1000);*/
			}

			System.out.println("All spend Time:"+(System.currentTimeMillis()-begin));
			//insertOne(collectionName, document);
			
			
		} catch (Exception e) {
			if (e instanceof MongoWriteException) {
				String message = e.getMessage();
				if (message == null) {
					message = "";
				}
				if (message.startsWith("E11000")) {
					System.out.println("no problem");
				} else {
					e.printStackTrace();
				}
			} else {
				e.printStackTrace();
			}
		}
	}

	static class Worker implements Runnable {
		private final CountDownLatch startSignal;
		private final CountDownLatch doneSignal;

		Worker(CountDownLatch startSignal, CountDownLatch doneSignal) {
			this.startSignal = startSignal;
			this.doneSignal = doneSignal;
		}

		public void run() {
			try {
				startSignal.await();
				doWork();
				doneSignal.countDown();
			} catch (InterruptedException ex) {
			} // return;
		}

		void doWork() {
			String rid = "abc123ddfassadf";
			Document document = new Document();
			document.put("_id", rid);
			document.put("requestID", rid);
			String collectionName = "log_summary";
			updateOneByAndEqualsUpsert(collectionName, document, document);

		}
	}
	
	/**
	 * 插入汇总信息
	 * 
	 * @param loginfo
	 */
	private static void insertSummary(Document doc, String countType) {
		String requestID = doc.getString("requestID");
		int timeInt = doc.getInteger("log_time");
		String sspCode = doc.getString("sspCode");
		String orderID = doc.getString("orderID");
		String planID = doc.getString("planID");

		Document document = new Document();
		document.put("requestTimeID", requestID);
		document.put("sspCode", sspCode);
		document.put("orderID", orderID);
		document.put("planID", planID);
		document.put("log_time", timeInt);
		document.put("log_timehour", timeInt/100);
		document.put(countType, 1);
		
		Map<String, Object> filterMap = new HashMap<String, Object>();
		filterMap.put("requestTimeID", requestID);
		MongodbCrud.updateOneByAndEqualsUpsert(MONGODB_SUMMARY, filterMap, document);

//		try {
//			MongodbCrud.updateOneByAndEqualsUpsert(LogConstantsForExpos.MONGODB_SUMMARY, filterMap, document);
//		} catch (Exception e) {
//			insertSummaryLast(document);
//		}
	}

}
