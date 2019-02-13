/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.sparql.core.Quad;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;

/**
 * @author Jeremy Debattista
 * 
 */
public class ValidIFPUsageTest extends Assert {
	protected TestLoader loader = new TestLoader();
	protected ValidIFPUsage metric;


	@Before
	public void setUp() throws Exception {
		loader.loadDataSet("testdumps/SampleInput_ValidIFPUsage_Minimal.ttl");
//		loader.loadDataSet("/Users/jeremy/Downloads/uriburner.com.nt");
		metric = new ValidIFPUsage();
	}

	@After
	public void tearDown() throws Exception {
	}
	
//	@Ignore
	@Test
	public void testValidIFPUsageMinimalExample() {
		List<Quad> streamingQuads = loader.getStreamingQuads();
		
		for(Quad quad : streamingQuads){
			// here we start streaming triples to the quality metric
			metric.compute(quad);
		}
		
		//	Total # IFP Statements : 6; # Violated Predicate-Object Statements : 2
		//  In principle, there are 3 triples with the same value for the IFP, but only 2 of them
		//  are violating the IFP.
		
		//  In our minimal example only the predicate object pair foaf:jabberID "I_AM_NOT_A_VALID_IFP" 
		//  was violated with :bob, :alice and :jack resources having the same jabberID
		
		// 1 - (2 / 6)
//		assertEquals(0.666667,metric.metricValue(), 0.0001);
//		assertEquals(0.75,metric.metricValue(), 0.0001);
		metric.metricValue();
		
//		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Intrinsic/valid-ifp.ttl");
		
	}	
	
	@Ignore
	@Test
	public void test() throws MalformedURLException {
		List<Quad> streamingQuads = loader.getStreamingQuads();
		
		Map<String, Integer> ns = new HashMap<String, Integer>();
		
		for(Quad quad : streamingQuads){
			String _ns = quad.getPredicate().getNameSpace();
			
			if (_ns.startsWith("tag")) continue;
//			URL domURL = new URL(_ns);
//			String domAuth = domURL.getAuthority();
			
			if (_ns.startsWith("http://twitter.com")) _ns = "http://twitter.com";
//			ns.compute(_ns, (k,v) -> (v == null) ? 1 : v + 1);
			ns.merge(_ns, 1, Integer::sum);
		}
		
		ns.forEach( (k,v) -> { System.out.println(k + " - " + v); });
	}
}
