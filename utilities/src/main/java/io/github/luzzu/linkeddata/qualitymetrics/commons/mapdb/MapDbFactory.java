package io.github.luzzu.linkeddata.qualitymetrics.commons.mapdb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;


/**
 * Creates and properly sets up instances of mapdb databases, stored in the filesystem.
 * @author slondono
 *
 */
public class MapDbFactory {
	
private static String mapDbDir = "/tmp/";
	
	public static void setMapDbDirectory(String mapDbDirectory) {
		mapDbDir = mapDbDirectory + ((mapDbDirectory.endsWith("/"))?(""):("/"));
	}
	
	private static DB singletonAsync = null;
	private static DB singleton = null;
	
	private static HashMap<String,Boolean> openDBAccess = new HashMap<String, Boolean>(); // Class Name, True (if open)
	private static HashMap<DB,String> dbLocation = new HashMap<DB, String>(); // DB, File Location

	
	public static DB getMapDBAsyncTempFile(String className){
//		Singleton is useless here as this would be incoroprated in a JAR file		
		if (singletonAsync == null){
			singletonAsync = createAsyncFilesystemDB();
		}
		
//		singletonAsync = createAsyncFilesystemDB();
		
		openDBAccess.putIfAbsent(className, true);
		
		return singletonAsync;
	}
	
	public static DB getMapDBTempFile(){
		if (singleton == null){
			singleton = createFilesystemDB();
		}
		return singleton;
	}
	
	
	
	private static DB createFilesystemDB() {
		return createAsyncFilesystemDB();
	}
	
	private static DB createAsyncFilesystemDB() {
		String timestamp = UUID.randomUUID().toString();
		
//		synchronized(MapDbFactory.class) {
//			timestamp = (new Long((new Date()).getTime())).toString();
//		}
		
		DB mapDB = DBMaker.fileDB(new File(mapDbDir + "mapasyncdb-" + timestamp))
				.fileMmapEnableIfSupported()
				.closeOnJvmShutdown()
				.fileDeleteAfterClose()
				.fileMmapPreclearDisable()
				.fileChannelEnable()
				.cleanerHackEnable()
				.make();
		
		dbLocation.put(mapDB, mapDbDir + "mapasyncdb-" + timestamp);
		return mapDB;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> Set<T> createHashSet(DB mapDBFile, String name){
		Set<T> _hashSet = null;
		synchronized (mapDBFile) {
			_hashSet = (Set<T>) mapDBFile.hashSet(name).create();
					//.createHashSet(name).make();
		}
		return _hashSet;
	}
	
	@SuppressWarnings("unchecked")
	public static <T1,T2> HTreeMap<T1,T2> createHashMap(DB mapDBFile, String name){
		HTreeMap<T1,T2> _htmap = null;
		synchronized (mapDBFile) {
			_htmap = (HTreeMap<T1, T2>) mapDBFile.hashMap(name).create();
		}
		return _htmap;
	}
	
	/**
	 * Creates a new database, stored in memory (more precisely, in heap space)
	 */
	public static DB createHeapDB() {
		DB mapDB = 	DBMaker.heapDB().make();	
		return mapDB;
	}
	
	public static synchronized void invokeClose(String className, DB db) {
		openDBAccess.put(className, false);
		
		boolean keep = openDBAccess.values().stream().anyMatch(b -> b == true);
		if (keep == false) {
			synchronized(db) {
				String filename = dbLocation.get(db);
				db.close();
				db = null;
				singletonAsync = null;
				
				File f = new File(filename);
				try {
					FileUtils.deleteDirectory(f);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static String getLocation(DB db) {
		return dbLocation.get(db);
	}
}
