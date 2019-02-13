package io.github.luzzu.linkeddata.qualitymetrics.commons.mapdb;

import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.mapdb.DB;

public class MapDbFactoryTest extends Assert {

	@Test
	public void initialise() {
//			for (int j = 0; j <= 500; j++) {
//				System.out.print(j + ". ");
//				new Thread(){
//				    public void run() {
//				        System.out.print("Thread");
//				        DB mapDb = MapDbFactory.getMapDBAsyncTempFile("hala");
//				        Set<String> testSet = MapDbFactory.createHashSet(mapDb, UUID.randomUUID().toString());
//				        testSet.add(UUID.randomUUID().toString());
//				    }
//				}.start();
//				System.out.println();
//			}
		DB mapDb = MapDbFactory.getMapDBAsyncTempFile("hala");
        Set<String> testSet = MapDbFactory.createHashSet(mapDb, UUID.randomUUID().toString());
        testSet.add(UUID.randomUUID().toString());
        
        mapDb = MapDbFactory.getMapDBAsyncTempFile("hala");
        testSet = MapDbFactory.createHashSet(mapDb, UUID.randomUUID().toString());
        testSet.add(UUID.randomUUID().toString());
	}
}
