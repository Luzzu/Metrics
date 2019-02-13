/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.accessibility.interlinking;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.mapdb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.HTTPRetriever;
import io.github.luzzu.linkeddata.qualitymetrics.commons.Utils;
import io.github.luzzu.linkeddata.qualitymetrics.commons.mapdb.MapDbFactory;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;

/**
 * @author Jeremy Debattista
 * 
 * In this metric we identify the total number of external linked used in the dataset. An external link
 * is identified if the subject URI is from one data source and an object URI from ￼another data source.
 * The data source should return RDF data to be considered as 'linked'.
 * In this metric rdf:type triples are skipped since these are not normally considered as part of the
 * Data Level Constant (or Data Level Position).
 * The value returned by this metric is the number of valid external links a dataset has (i.e. the number
 * of resource links not the number of links to datasets)
 * 
 * Based on: [1] Hogan Aidan, Umbrich Jürgen. An empirical survey of Linked Data conformance. Section 5.2, 
 * Linking, Issue VI: Use External URIs (page 20).
 */
public class LinkExternalDataProviders extends AbstractQualityMetric<Integer> {
	
	/**
	 * MapDB database, used to persist the Map containing the instances found to be declared in the dataset
	 */
	protected DB mapDB = MapDbFactory.getMapDBAsyncTempFile(LinkExternalDataProviders.class.getName());
	
	
	
	protected Set<String> setResources = MapDbFactory.createHashSet(mapDB, UUID.randomUUID().toString());
	
	/**
	 * A set that holds all unique PLDs that return RDF data
	 */
	protected Set<String> setPLDsRDF = new HashSet<String>();
	
	final static Logger logger = LoggerFactory.getLogger(LinkExternalDataProviders.class);

	protected final Resource METRIC_URI = DQM.LinksToExternalDataProvidersMetric;
	
	protected boolean computed = false;
	protected String localPLD = EnvironmentProperties.getInstance().getDatasetPLD();

	protected Map<String,String> resolver = new HashMap<String,String>();
	protected Set<String> ns404 = new HashSet<String>();

	
	
	@Override
	public void compute(Quad quad) {
		logger.debug("Computing : {} ", quad.asTriple().toString());
		
		Node predicate = quad.getPredicate();
		Node object = quad.getObject();

		if (!(predicate.getURI().equals(RDF.type.getURI()))) {
			if (object.isURI()) {
				String objectURL = object.getURI();
				if (!(objectURL.startsWith(localPLD))) { // then it must be an external link

					if ((objectURL.contains("purl.org")) || (objectURL.contains("w3id.org"))) {
						// we need to resolve the persistance URI
						String datasetNS = Utils.extractDatasetNS(objectURL);

						if (!(resolver.containsKey(datasetNS))) {
							String targetURL = HTTPRetriever.decodePersistantURLS(objectURL);
							resolver.put(datasetNS, Utils.extractDatasetNS(targetURL));
						}
					} 
					setResources.add(objectURL);
				}
			}
		}
	}
	
	@Override
	public Integer metricValue() {
		if (!computed){
			this.checkForRDFLinks();
			computed = true;
		}
		
		
		return setPLDsRDF.size();
	}

	@Override
	public Resource getMetricURI() {
		return METRIC_URI;
	}

	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return DQM.LuzzuProvenanceAgent;
	}
	
	protected void checkForRDFLinks() {
		ExecutorService service = Executors.newCachedThreadPool();
		for (String s : setResources){
			
			String targetDatasetNS = Utils.extractDatasetNS(s);
			if ((s.contains("purl.org")) || (s.contains("w3id.org"))) {
				targetDatasetNS = resolver.get(Utils.extractDatasetNS(s));
			}
			
			if (setPLDsRDF.contains(targetDatasetNS)) continue; // If we resolved data from that domain already, we do not need to recheck

			Future<Boolean> future = service.submit(new ParsableContentChecker(s));
			try {
				boolean isParsable = future.get(3, TimeUnit.SECONDS);
				if (isParsable) {
					setPLDsRDF.add(targetDatasetNS);
					break;
				} 
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				//TODO: Catch better exception
				logger.debug(e.getMessage());
			}
		}
 		service.shutdown();
 		System.gc();
	}
	
	@Override
	public ProblemCollection<?> getProblemCollection() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		
		//TODO: Add profiling information, including what estimation technique used. See Extensional Conciseness metric
		return activity;
	}

	class ParsableContentChecker implements Callable<Boolean> {
		String uri = "";
		
		public ParsableContentChecker(String uri){
			this.uri = uri;
		}

		@Override
		public Boolean call() throws Exception {
			final String targetDatasetNS = Utils.extractDatasetNS(uri);
			if (ns404.contains(targetDatasetNS)) return false;
			
			try{
				return (RDFDataMgr.loadModel(uri).size() > 0);
			} catch (HttpException httpE){
				if (httpE.getResponseCode() == 404) ns404.add(targetDatasetNS);
				return false;
			} catch (Exception e){
				return false;
			}
		}
	}
}
