package cn.com.adrich.flume.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

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

import cn.com.adrich.flume.mongodb.MongoDBBatchCURD.UpdateField.Operator;

public enum MongoDBBatchCURD {

	;
	private static final Logger logger = LoggerFactory.getLogger(MongoDBBatchCURD.class);

	public static final String MONGODB_WIN = "log_win";
	public static final String MONGODB_EXPOS = "log_expos";
	public static final String MONGODB_CLICK = "log_click";
	public static final String MONGODB_SUMMARY = "log_summary";
	public static final String MONGODB_EXCEPTION = "log_exception_logs";

	public static int batchProcessReqCount = 5000;
	public static int blockQueueFactor = 8;
	public static int threadCountInSameTime = 8;
	public static int reqSleep = 1000;
	public static int threadSleep = 1000;
	// 四个阻塞队列
	private static BlockingQueue<Document> Expos_Queue = new LinkedBlockingQueue<Document>(
			batchProcessReqCount * blockQueueFactor);
	private static BlockingQueue<Document> Click_Queue = new LinkedBlockingQueue<Document>(
			batchProcessReqCount * blockQueueFactor);
	private static BlockingQueue<Document> Win_Queue = new LinkedBlockingQueue<Document>(
			batchProcessReqCount * blockQueueFactor);
	// summary数据是win，click，expos的3倍以上；upsert比insert慢5-6倍，设定upsert队列扩大20倍
	private static BlockingQueue<Document> Summary_Queue = new LinkedBlockingQueue<Document>(
			20 * batchProcessReqCount * blockQueueFactor);
	private static Map<String, BlockingQueue<Document>> COLLECTION_MQ_MAP;

	// 批量插入我们要求线程间排队
	private volatile static CountDownLatch winSignal = new CountDownLatch(threadCountInSameTime / 4);
	private volatile static CountDownLatch clickSignal = new CountDownLatch(threadCountInSameTime / 4);
	private volatile static CountDownLatch exposSignal = new CountDownLatch(threadCountInSameTime / 4);
	private volatile static CountDownLatch summarySignal = new CountDownLatch(
			threadCountInSameTime - (threadCountInSameTime / 4) * 3);

	/**
	 * MongoDB错误代码：主键重复
	 */
	public static final String ERROR_CODE_DUPLICATE_KEY = "E11000";

	// 配置
	public static final String MONGODB_URI_KEY = "mongodb.uri";
	public static final String MONGODB_DATABASENAME = "mongodb.databasename";
	public static final String MONGODB_PROCESSREQ_COUNT = "mongodb.batchProcessReqCount";
	public static final String MONGODB_BQUEUE_FACTOR = "mongodb.blockQueueFactor";
	public static final String MONGODB_THREAD_COUNT = "mongodb.threadCountInSameTime";
	public static final String MONGODB_THREAD_REQSLEEP = "mongodb.reqSleep";
	public static final String MONGODB_THREAD_SLEEP = "mongodb.threadSleep";

	private static MongoClient MONGO_CLIENT;
	private static MongoDatabase DATABASE;
	private static Map<String, MongoCollection<Document>> COLLECTION_MAP;

	// 记录各队列消耗线程数
	private static Map<String, Integer> COLLECTION_THREAD_COUNT;

	// 读取配置文件、初始化客户端、数据库、集合连接
	static {

		InputStream resourceStream = null;
		try {
			// 读取配置文件
			resourceStream = MongoDBBatchCURD.class.getResourceAsStream("/mongodb-config.properties");

			Properties p = new Properties();
			p.load(resourceStream);

			// MongoDB 连接字符串
			String uri = p.getProperty(MONGODB_URI_KEY);
			MongoClientURI mongoClientURI = new MongoClientURI(uri);
			MONGO_CLIENT = new MongoClient(mongoClientURI);

			// 要连接的数据库
			String databaseName = p.getProperty(MONGODB_DATABASENAME);
			DATABASE = MONGO_CLIENT.getDatabase(databaseName);

			// 批量处理请求数量
			batchProcessReqCount = Integer.parseInt(p.getProperty(MONGODB_PROCESSREQ_COUNT));
			// 阻塞队列长度因子 ；队列长度 = batchProcessReqCount * blockQueueFactor
			blockQueueFactor = Integer.parseInt(p.getProperty(MONGODB_BQUEUE_FACTOR));
			// 同一时间最大线程数
			threadCountInSameTime = Integer.parseInt(p.getProperty(MONGODB_THREAD_COUNT));

			// 测试用，请求发送延迟
			reqSleep = Integer.parseInt(p.getProperty(MONGODB_THREAD_REQSLEEP));
			// 工作线程批处理延迟
			threadSleep = Integer.parseInt(p.getProperty(MONGODB_THREAD_SLEEP));

			// 将库中已存在的集合放入 Map 中缓存
			COLLECTION_MAP = new ConcurrentHashMap<String, MongoCollection<Document>>();

			MongoIterable<String> collectionNames = DATABASE.listCollectionNames();
			COLLECTION_MQ_MAP = new ConcurrentHashMap<String, BlockingQueue<Document>>();
			for (String collectionName : collectionNames) {
				MongoCollection<Document> collection = DATABASE.getCollection(collectionName);
				COLLECTION_MAP.put(collectionName, collection);

			}

			// 四个阻塞队列
			// 竞价，点击，曝光做批量插入
			COLLECTION_MQ_MAP.put(MONGODB_WIN, Win_Queue);
			COLLECTION_MQ_MAP.put(MONGODB_CLICK, Click_Queue);
			COLLECTION_MQ_MAP.put(MONGODB_EXPOS, Expos_Queue);
			// 汇总队列，队列做事前汇总，做批量 [单个upsert]，对upsert文档字段需求，做set和setOnInsert区分
			COLLECTION_MQ_MAP.put(MONGODB_SUMMARY, Summary_Queue);

			COLLECTION_THREAD_COUNT = new ConcurrentHashMap<String, Integer>();
			COLLECTION_THREAD_COUNT.put(MONGODB_WIN, 0);
			COLLECTION_THREAD_COUNT.put(MONGODB_CLICK, 0);
			COLLECTION_THREAD_COUNT.put(MONGODB_EXPOS, 0);
			COLLECTION_THREAD_COUNT.put(MONGODB_SUMMARY, 0);

			ExecutorService service = Executors.newCachedThreadPool(); // 缓存线程池

			// 创建队列监视线程
			service.execute(new QueueWatcher(service));

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
	 * 根据更新 Map 构建更新Summary对象。
	 * 
	 * @param updateMap
	 *            更新 Map。
	 * @return 更新对象。
	 * @author zyh
	 */
	private static Bson buildUpdate2(Map<String, Object> updateMap) {

		List<Bson> updates = null;
		if (updateMap != null) {
			updates = new ArrayList<Bson>();
			for (Entry<String, Object> entry : updateMap.entrySet()) {
				String typeName = entry.getKey();
				UpdateField typeValue = (UpdateField) entry.getValue();
				Bson updateTemp = null;

				switch (typeValue.operator) {
				case setOnInsert: {
					updateTemp = Updates.setOnInsert(typeValue.fieldName, typeValue.value);
					if (updateTemp != null)
						updates.add(updateTemp);
					break;
				}
				case set: {
					updateTemp = Updates.set(typeValue.fieldName, typeValue.value);
					if (updateTemp != null)
						updates.add(updateTemp);
					break;
				}
				case addEachToSet: {
					List<String> value = new ArrayList<String>();
					value.add(typeValue.value.toString());
					updateTemp = Updates.addEachToSet(typeValue.fieldName, value);
					if (updateTemp != null)
						updates.add(updateTemp);
					break;
				}
				case addToSet:{
					updateTemp = Updates.addToSet(typeValue.fieldName, typeValue.value);
					if (updateTemp != null)
						updates.add(updateTemp);
					break;
				}
				default:
					break;
				}
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
	public static UpdateResult updateOneByAndEquals(String collectionName, Map<String, Object> filterMap,
			Document document) {
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
	public static UpdateResult updateOneByAndEqualsUpsert(String collectionName, Map<String, Object> filterMap,
			Document document) {

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
			// 如果出错，则尝试在调用一次。
			// 在高并发的情况下，会出现主键重复的错误
			try {
				updateResult = collection.updateOne(filter, update, uo);
			} catch (Exception e1) {
				// throw e1;
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

		int reqCount = 1000000;

		try {
			long begin = System.currentTimeMillis();
			List<DBObject> dbObjects = new ArrayList<DBObject>();
			for (int i = 0; i < reqCount; i++) {
				Document dt = new Document();
				dt.putAll((new InsertObject()).getLogClickDt());
				dt.put("requestTimeID", i + "");
				dt.put("requestID", i + "");
				insertMany(MONGODB_WIN, dt);
				insertSummary(dt, "wincount");

				insertMany(MONGODB_EXPOS, dt);
				insertSummary(dt, "exposcount");

				insertMany(MONGODB_CLICK, dt);
				insertSummary(dt, "clickcount");

				if (i % batchProcessReqCount == 0)
					Thread.sleep(reqSleep);
			}
			System.out.println("[" + new Date(System.currentTimeMillis()) + "] AllReq spend Time:"
					+ (System.currentTimeMillis() - begin));

			
			 getCountDownLatch(MONGODB_CLICK).await();
			 getCountDownLatch(MONGODB_EXPOS).await();
			 getCountDownLatch(MONGODB_WIN).await();
			 
			getCountDownLatch(MONGODB_SUMMARY).await();
			System.out.println("[" + new Date(System.currentTimeMillis()) + "] All spend Time:"
					+ (System.currentTimeMillis() - begin));
			// insertOne(collectionName, document);

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

	/**
	 * 插入log_summary
	 * 
	 * @param doc
	 * @param countType
	 * @author zyh
	 */
	private static void insertSummary(Document doc, String countType) {

		String requestID = doc.getString("requestID");
		int timeInt = doc.getInteger("log_time");
		String sspCode = doc.getString("sspCode");
		String orderID = doc.getString("orderID");
		String planID = doc.getString("planID");

		Document document = new Document();
		Document setOnInsert = new Document();
		Document set = new Document();

		setOnInsert.put("requestTimeID", requestID);
		setOnInsert.put("sspCode", sspCode);
		setOnInsert.put("orderID", orderID);
		setOnInsert.put("planID", planID);
		setOnInsert.put("log_time", timeInt);
		setOnInsert.put("log_timehour", timeInt / 100);
		setOnInsert.put("wincount", 0);
		setOnInsert.put("exposcount", 0);
		setOnInsert.put("clickcount", 0);
		setOnInsert.remove(countType);

		if ("exposcount".equals(countType)) {
			setOnInsert.remove("log_time");
			setOnInsert.remove("log_timehour");
			set.put("log_time", timeInt);
			set.put("log_timehour", timeInt / 100);
		}
		set.put(countType, 1);

		document.put("$id", "requestTimeID");
		document.put("$onInsert", setOnInsert);
		document.put("$set", set);

		upsertMany(MONGODB_SUMMARY, document);
	}

	/**
	 * 插入log_summary
	 * 
	 * @param doc
	 * @param countType
	 * @author zyh
	 */
	private static void insertSummary2(Document doc, String countType) {

		Map<String, Object> upfieldList = new HashMap<String, Object>(12);

		String requestID = doc.getString("requestID");
		int timeInt = doc.getInteger("log_time");
		String sspCode = doc.getString("sspCode");
		String orderID = doc.getString("orderID");
		String planID = doc.getString("planID");

		// 过滤条件字段
		upfieldList.put("$filter", "requestTimeID");

		upfieldList.put("requestTimeID", new UpdateField(Operator.setOnInsert, "requestTimeID", requestID));
		upfieldList.put("sspCode", new UpdateField(Operator.setOnInsert, "sspCode", sspCode));
		upfieldList.put("orderID", new UpdateField(Operator.setOnInsert, "orderID", orderID));
		upfieldList.put("planID", new UpdateField(Operator.setOnInsert, "planID", planID));
		upfieldList.put("log_time", new UpdateField(Operator.setOnInsert, "log_time", timeInt));
		upfieldList.put("log_timehour", new UpdateField(Operator.setOnInsert, "log_timehour", timeInt / 100));
		upfieldList.put("wincount", new UpdateField(Operator.setOnInsert, "wincount", 0));
		upfieldList.put("exposcount", new UpdateField(Operator.setOnInsert, "exposcount", 0));
		upfieldList.put("clickcount", new UpdateField(Operator.setOnInsert, "clickcount", 0));

		if ("exposcount".equals(countType)) {
			upfieldList.put("log_time", new UpdateField(Operator.set, "log_time", timeInt));
			upfieldList.put("log_timehour", new UpdateField(Operator.set, "log_timehour", timeInt / 100));
		}

		upfieldList.put(countType, new UpdateField(Operator.set, countType, 1));

		upsertMany(MONGODB_SUMMARY, new Document(upfieldList));
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
	 * 批量插入文档
	 * 
	 * @param collectionName
	 * @param document
	 * @author zyh
	 */
	public static void insertMany(String collectionName, Document document) {
		long begin = System.currentTimeMillis();
		BlockingQueue<Document> queue = COLLECTION_MQ_MAP.get(collectionName);
		try {
			queue.put(document);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (logger.isDebugEnabled()) {
			long cost = System.currentTimeMillis() - begin;
			if (cost > 100) {
				logger.debug("MongoDB insertOne slowly cost : {}ms!", cost);
			}
		}
	}

	/**
	 * 批量更新文档
	 * 
	 * @param collectionName
	 * @param document
	 * @author zyh
	 */
	public static void upsertMany(String collectionName, Document document) {
		insertMany(collectionName, document);
	}

	/**
	 * 批处理工作线程
	 */
	static class BatchWorker implements Runnable {
		private final CountDownLatch typeSignal;
		private String collectionName;
		private ThreadLocal<Integer> countThreadLocal;
		private List<Document> batchList = null;
		private Map<String, Document> summaryMap = null;

		BatchWorker(String collectionName, CountDownLatch typeSignal) {
			COLLECTION_THREAD_COUNT.put(collectionName, COLLECTION_THREAD_COUNT.get(collectionName) + 1);
			this.collectionName = collectionName;
			this.typeSignal = typeSignal;
			if (MONGODB_SUMMARY.equals(collectionName))
				summaryMap = new HashMap<String, Document>(batchProcessReqCount);

			batchList = new ArrayList<Document>(batchProcessReqCount);
		}

		public void run() {

			MongoCollection<Document> collection = getCollection(this.collectionName);
			BlockingQueue<Document> queue = COLLECTION_MQ_MAP.get(collectionName);

			while (true) {
				try {

					doWork(collection, queue);

				} finally {
					if (batchList != null)
						batchList.clear();
					if (summaryMap != null)
						summaryMap.clear();
					if (queue.isEmpty())
						typeSignal.countDown();
				}
			}
		}

		void doWork(MongoCollection<Document> collection, BlockingQueue<Document> queue) {

			int size = batchProcessReqCount;
			long begin = System.currentTimeMillis();

			for (int i = 0; i < batchProcessReqCount; i++) {
				try {
					// summary插入前批量汇总，一边出队一边汇总，问题：影响出队效率
					/*
					 * if (summaryMap != null) { Document doc1 = queue.take();
					 * String keyName = doc1.getString("$id");//主键，更新过滤条件
					 * Document onInsert = (Document)
					 * doc1.get("$onInsert");//插入字段 String keyValue =
					 * onInsert.getString(keyName); //汇总后的放入map
					 * summaryMap.put(keyValue, UnionSummary(doc1,
					 * summaryMap.get(keyValue))); } else
					 */
					batchList.add(queue.take());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			// 批量处理在出队后处理
			if (summaryMap != null && batchList != null) {
				for (Document doc1 : batchList) {
					String keyName = doc1.getString("$filter");// 过滤条件；单条件
					UpdateField keyValue = (UpdateField) doc1.get(keyName);
					// 汇总后的放入map
					summaryMap.put(keyValue.value.toString(), UnionSummary(doc1, summaryMap.get(keyValue.value)));
				}

				size = summaryMap.values().toArray().length;
				UpdateOptions upsert = new UpdateOptions();
				if (size != 0) {
					for (Document summary : summaryMap.values()) {

						String keyName = summary.getString("$filter");
						UpdateField keyValue = (UpdateField) summary.get(keyName);
						collection.updateOne(Filters.eq(keyName, keyValue.value), buildUpdate2(summary),
								upsert.upsert(true));
					}
				}
			} else
				collection.insertMany(batchList);

			System.out.println("[" + new Date(System.currentTimeMillis()) + "] Current Thread["
					+ Thread.currentThread().getName() + "][" + collectionName + "] BatchProcess[" + size
					+ "条文档] SpendTime:" + (System.currentTimeMillis() - begin) + " left Queue size:" + queue.size());
			try {
				Thread.sleep(threadSleep);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public ThreadLocal<Integer> getCountThreadLocal() {
			return countThreadLocal;
		}

		public void setCountThreadLocal(ThreadLocal<Integer> countThreadLocal) {
			this.countThreadLocal = countThreadLocal;
		}
	}

	// SUMMMARY 需要汇总
	private static Document UnionSummary(Document doc1, Document doc2) {
		// doc1 新插入的；doc2已经整合的
		if (doc2 == null)
			return doc1;

		// 将doc1 整合到doc2
		for (Object obj : doc1.values()) {
			UpdateField field = (UpdateField) obj;
			if (Operator.set.equals(field.operator)) {
				UpdateField newSet = (UpdateField) doc1.get(field.fieldName);
				doc2.put(field.fieldName, newSet);
			}
		}
		return doc2;
	}

	static class QueueWatcher implements Runnable {

		private final ExecutorService service;

		QueueWatcher(ExecutorService service) {
			this.service = service;
		}

		public void run() {
			while (true) {
				doWork();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		void doWork() {
			// 循环创建处理线程；条件队列长度大于5000且 同类线程数 不超过线程平均数
			for (String key : COLLECTION_MQ_MAP.keySet()) {
				// 队列大于一倍以上批处理量
				if (COLLECTION_MQ_MAP.get(key).size() / batchProcessReqCount >= 1
						// 线程分配数不得大于4分之一总线程数
						&& (COLLECTION_THREAD_COUNT.get(key) < threadCountInSameTime / 4
								// 汇总数据线程线程数 总数-3/4线程数
								|| (MONGODB_SUMMARY.equals(key) && COLLECTION_THREAD_COUNT
										.get(key) < (threadCountInSameTime - (threadCountInSameTime / 4) * 3)))) {
					this.service.execute(new BatchWorker(key, getCountDownLatch(key)));
					System.out.println("Queue[" + key + "] insert is Start!");
				}

				// 队列处理完成
				/*
				 * if (COLLECTION_MQ_MAP.get(key).size() == 0 &&
				 * COLLECTION_THREAD_COUNT.get(key) > 0) {
				 * getCountDownLatch(key).countDown(); // System.out.println(1);
				 * } if (COLLECTION_MQ_MAP.get(key).size() > 0 &&
				 * COLLECTION_THREAD_COUNT.get(key)!=getCountDownLatch(key).
				 * getCount()) {
				 * 
				 * // System.out.println(1); }
				 */
			}
		}

		boolean await() {
			try {
				clickSignal.await();
				exposSignal.await();
				summarySignal.await();
				winSignal.await();
				return false;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return true;
		}

	}

	static CountDownLatch getCountDownLatch(String key) {
		switch (key) {
		case MONGODB_WIN:
			return winSignal;
		case MONGODB_CLICK:
			return clickSignal;
		case MONGODB_EXPOS:
			return exposSignal;
		case MONGODB_SUMMARY:
			return summarySignal;
		default:
			return null;
		}
	}

	public static class UpdateField {
		public static enum Operator {
			set, setOnInsert, addToSet, addEachToSet
		}

		public UpdateField(String fieldName, Object value) {
			this(Operator.set, fieldName, value);
		}

		public UpdateField(Operator operator, String fieldName, Object value) {
			this.operator = operator;
			this.fieldName = fieldName;
			this.value = value;
		}

		public Operator operator;
		public String fieldName;
		public Object value;
	}

}
