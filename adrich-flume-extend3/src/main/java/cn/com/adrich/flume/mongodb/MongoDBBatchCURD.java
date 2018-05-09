package cn.com.adrich.flume.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
	private static BlockingQueue<Document> Summary_Queue = new LinkedBlockingQueue<Document>(
			20 * batchProcessReqCount * blockQueueFactor);
	private static BlockingQueue<Document> Win_Queue = new LinkedBlockingQueue<Document>(
			batchProcessReqCount * blockQueueFactor);
	private static Map<String, BlockingQueue<Document>> COLLECTION_MQ_MAP;

	// 批量插入我们要求线程间排队
	private final static CountDownLatch winSignal = new CountDownLatch(1);
	private final static CountDownLatch clickSignal = new CountDownLatch(1);
	private final static CountDownLatch exposSignal = new CountDownLatch(1);
	private final static CountDownLatch summarySignal = new CountDownLatch(1);

	/**
	 * MongoDB错误代码：主键重复
	 */
	public static final String ERROR_CODE_DUPLICATE_KEY = "E11000";

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

			reqSleep = Integer.parseInt(p.getProperty(MONGODB_THREAD_REQSLEEP));
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
			// 汇总队列，队列做事前汇总，做批量 [单个upsert]
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
				//insertMany(MONGODB_CLICK, dt);
				insertSummary(dt, "clickcount");
				//insertMany(MONGODB_EXPOS, dt);
				insertSummary(dt, "exposcount");
				//insertMany(MONGODB_WIN, dt);
				insertSummary(dt, "wincount");

				if (i % (reqCount / 4) == 0)
					Thread.sleep(reqSleep);
			}
			System.out.println("AllReq spend Time:" + (System.currentTimeMillis() - begin));

			getCountDownLatch(MONGODB_CLICK).await();
			getCountDownLatch(MONGODB_EXPOS).await();
			getCountDownLatch(MONGODB_WIN).await();
			System.out.println("All spend Time:" + (System.currentTimeMillis() - begin));
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
		document.put("log_timehour", timeInt / 100);
		
		Document dt = new Document();
		dt.append("$setOnInsert", document);
		dt.append("$set", new Document(countType, 1));

		Map<String, Object> filterMap = new HashMap<String, Object>();
		filterMap.put("requestTimeID", requestID);
		// MongodbCrud.updateOneByAndEqualsUpsert(LogConstantsForExpos.MONGODB_SUMMARY,
		// filterMap, document);
		insertMany(MONGODB_SUMMARY, dt);
		// try {
		// MongodbCrud.updateOneByAndEqualsUpsert(LogConstantsForExpos.MONGODB_SUMMARY,
		// filterMap, document);
		// } catch (Exception e) {
		// insertSummaryLast(document);
		// }
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

	public static void insertMany(String collectionName, Document document) {
		long begin = System.currentTimeMillis();

		/*
		 * MongoCollection<Document> collection = getCollection(collectionName);
		 * 
		 * collection.insertOne(document);
		 */

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
	 * 从集合缓存中获取指定的集合连接。
	 * 
	 * @param dmpCollection
	 *            要获取的集合。
	 * @return 集合连接。
	 */
	private static MongoCollection<Document> getCollectionMQ(String collectionName) {

		MongoCollection<Document> collection = COLLECTION_MAP.get(collectionName);
		if (collection == null) {
			collection = DATABASE.getCollection(collectionName);
			COLLECTION_MAP.put(collectionName, collection);
		}
		return collection;
	}

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
			else
				batchList = new ArrayList<Document>(batchProcessReqCount);
		}

		/*
		 * BatchWorker(String collectionName) {
		 * COLLECTION_THREAD_COUNT.put(collectionName,
		 * COLLECTION_THREAD_COUNT.get(collectionName)+1); this.collectionName =
		 * collectionName; }
		 */
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

				}
			}
		}

		void doWork(MongoCollection<Document> collection, BlockingQueue<Document> queue) {

			int size = batchProcessReqCount;
			long begin = System.currentTimeMillis();

			for (int i = 0; i < batchProcessReqCount; i++) {
				try {
					if (summaryMap != null) {
						Document doc1 = queue.take();
						String key = doc1.getString("requestTimeID");
						summaryMap.put(key, UnionSummary(doc1, summaryMap.get(key)));
					} else
						batchList.add(queue.take());

					// System.out.println("random
					// "+Unsafe.class.(batchList.get(0)));
					// UNSAFE.equals(batchList)
					// typeSignal.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if (summaryMap != null) {
				size = summaryMap.values().toArray().length;
				UpdateOptions upsert = new UpdateOptions();
				if (size != 0) {
					for (Document summary : summaryMap.values()) {

						String key = summary.getString("requestTimeID");
						collection.updateOne(Filters.eq("requestTimeID", key), buildUpdate(summary),
								upsert.upsert(true));
					}
				}
			} else
				collection.insertMany(batchList);

			System.out.println("Current Thread[" + Thread.currentThread().getName() + "][" + collectionName
					+ "] BatchInsert[" + size + "条文档] SpendTime:" + (System.currentTimeMillis() - begin)
					+ " left Queue size:" + queue.size());
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
		//doc1 新插入的；doc2已经整合的
		if (doc2 == null)
			return doc1;

		// 将doc1 整合到doc2
		for (String key : doc1.keySet()) {
			if (doc2.containsKey(key)&&"$set".equals(key))
			{
				Document oldSet = (Document)doc2.get(key);
				Document newSet = (Document)doc1.get(key);
				for (String set : newSet.keySet())
				{	
					oldSet.append(key, newSet.get(set));
				}
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
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		void doWork() {
			// 循环创建处理线程；条件队列长度大于5000且 同类线程数 不超过线程平均数
			for (String key : COLLECTION_MQ_MAP.keySet()) {
				if (COLLECTION_MQ_MAP.get(key).size() / batchProcessReqCount >= 1
						&& (COLLECTION_THREAD_COUNT.get(key) < threadCountInSameTime / 4
					||(MONGODB_SUMMARY.equals(key)
							&& COLLECTION_THREAD_COUNT.get(key)<(threadCountInSameTime/4+threadCountInSameTime%4)))) {
					this.service.execute(new BatchWorker(key, getCountDownLatch(key)));
					System.out.println("Queue[" + key + "] insert is Start!");
				}

				if (COLLECTION_MQ_MAP.get(key).size() == 0 && COLLECTION_THREAD_COUNT.get(key) > 0) {
					getCountDownLatch(key).countDown();
					// System.out.println(1);
				}
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

}
