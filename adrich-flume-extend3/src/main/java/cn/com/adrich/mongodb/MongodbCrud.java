package cn.com.adrich.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

import cn.com.adrich.mongodb.MongodbCrud.UpdateField.Operator;

public enum MongodbCrud {

	;

	public static final String MONGODB_WIN = "log_win";
	public static final String MONGODB_EXPOS = "log_expos";
	public static final String MONGODB_CLICK = "log_click";
	public static final String MONGODB_SUMMARY = "log_summary";
	public static final String MONGODB_EXCEPTION = "log_exception_logs";
	
	public static final String MONGODB_URI_KEY = "mongodb.uri";
	public static final String MONGODB_DATABASENAME = "mongodb.databasename";

	private static final MongoClient MONGO_CLIENT;
	private static final MongoDatabase DATABASE;
	private static final Map<String, MongoCollection<Document>> COLLECTION_MAP = new ConcurrentHashMap<String, MongoCollection<Document>>();

	// 读取配置文件、初始化客户端、数据库、集合连接
	static {
		InputStream resourceAsStream = null;
		try {
			// 读取配置文件
			resourceAsStream = MongodbCrud.class.getResourceAsStream("/mongodb-config.property");

			Properties p = new Properties();
			p.load(resourceAsStream);

			// MongoDB 连接字符串
			String uri = p.getProperty(MONGODB_URI_KEY);
			MongoClientURI mongoClientURI = new MongoClientURI(uri);
			MONGO_CLIENT = new MongoClient(mongoClientURI);

			// 要连接的数据库
			String databaseName = p.getProperty(MONGODB_DATABASENAME);
			DATABASE = MONGO_CLIENT.getDatabase(databaseName);

			// 将库中已存在的集合放入 Map 中缓存
			MongoIterable<String> collectionNames = DATABASE.listCollectionNames();
			for (String collectionName : collectionNames) {
				MongoCollection<Document> collection = DATABASE.getCollection(collectionName);
				COLLECTION_MAP.put(collectionName, collection);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (resourceAsStream != null) {
				try {
					resourceAsStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 从集合缓存中获取指定的集合连接。
	 * 
	 * @param collectionType
	 *            要获取的集合。
	 * @return 集合连接。
	 */
	private static MongoCollection<Document> getCollection(CollectionType collectionType) {
		String collectionName = collectionType.getName();
		MongoCollection<Document> collection = COLLECTION_MAP.get(collectionName);
		if (collection == null) {
			collection = DATABASE.getCollection(collectionName);
			COLLECTION_MAP.put(collectionName, collection);
		}
		return collection;
	}

	public static FindIterable<Document> findByAndEquals(CollectionType collectionType, Map<String, Object> filterMap) {
		return findByAndEquals(collectionType, filterMap, null);
	}

	/**
	 * 根据查询条件查询指定集合文档多条内容。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param collectionType
	 *            要查询的集合。
	 * @param filterMap
	 *            查询条件。
	 * @return 查询结果游标。
	 */
	public static FindIterable<Document> findByAndEquals(CollectionType collectionType, Map<String, Object> filterMap, List<String> fieldNamesInclude) {
		Bson filter = buildFilterByAndEquals(filterMap);
		MongoCollection<Document> collection = getCollection(collectionType);
		FindIterable<Document> found = null;
		if (filter == null) {
			found = collection.find();
		} else {
			found = collection.find(filter);
		}
		if (fieldNamesInclude != null && !fieldNamesInclude.isEmpty()) {
			Bson projection = Projections.include(fieldNamesInclude);
			found = found.projection(projection);
		}
		return found;
	}

	public static Document findOneByAndEquals(CollectionType collectionType, Map<String, Object> filterMap) {
		return findOneByAndEquals(collectionType, filterMap, null);
	}

	/**
	 * 根据查询条件查询指定集合文档单条内容，如果查出多条内容，将返回第一条。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param collectionType
	 *            要查询的集合。
	 * @param filterMap
	 *            查询条件。
	 * @return 查询结果文档。
	 */
	public static Document findOneByAndEquals(CollectionType collectionType, Map<String, Object> filterMap, List<String> fieldNamesInclude) {
		FindIterable<Document> found = findByAndEquals(collectionType, filterMap, fieldNamesInclude);
		if (found != null) {
			Document first = found.first();
			return first;
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
	private static Bson buildUpdate(List<UpdateField> updateFields) {
		List<Bson> updates = null;
		if (updateFields != null) {
			updates = new ArrayList<Bson>();
			for (UpdateField updateField : updateFields) {
				Operator operator = updateField.operator;
				String fieldName = updateField.fieldName;
				Object value = updateField.value;
				if (Operator.set.equals(operator)) {
					Bson updateTemp = Updates.set(fieldName, value);
					updates.add(updateTemp);
				} else if (Operator.addToSet.equals(operator)) {
					Bson updateTemp = Updates.addToSet(fieldName, value);
					updates.add(updateTemp);
				} else if (Operator.addEachToSet.equals(operator)) {
					List<?> values = (List<?>) value;
					Bson updateTemp = Updates.addEachToSet(fieldName, values);
					updates.add(updateTemp);
				} else {
					throw new RuntimeException("No such operator : " + (operator == null ? "null" : operator));
				}
			}
		}
		Bson update = null;
		if (updates != null) {
			update = Updates.combine(updates);
		}
		return update;
	}

	public static class UpdateField {
		public static enum Operator {
			set, addToSet, addEachToSet
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

	/**
	 * 更新指定集合的符合条件的单条文档。
	 * 
	 * @param collectionType
	 *            要更新的集合。
	 * @param filterMap
	 *            更新条件。
	 * @param updateMap
	 *            更新数据。
	 * @return 更新结果。
	 */
	public static UpdateResult updateOneByAndEquals(CollectionType collectionType, Map<String, Object> filterMap, List<UpdateField> updateFields) {
		Bson filter = buildFilterByAndEquals(filterMap);
		MongoCollection<Document> collection = getCollection(collectionType);
		Bson update = buildUpdate(updateFields);
		UpdateResult updateResult = collection.updateOne(filter, update);
		return updateResult;
	}

	/**
	 * 向指定集合中插入单条文档。
	 * 
	 * @param collectionType
	 *            要插入文档的集合。
	 * @param document
	 *            要插入的文档。
	 */
	public static void insertOne(CollectionType collectionType, Document document) {
		MongoCollection<Document> collection = getCollection(collectionType);
		collection.insertOne(document);
	}

	public static class CollectionType{
		private String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
		
	}

}
