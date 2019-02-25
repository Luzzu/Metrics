package io.github.luzzu.linkeddata.qualitymetrics.accessibility.interlinking;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.HTTPRetriever;
import io.github.luzzu.linkeddata.qualitymetrics.commons.Utils;
import io.github.luzzu.qualitymetrics.algorithms.ReservoirSampler;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;


/**
 * @author Jeremy Debattista
 * Extends the Link External Data Providers with a reservior sampling technique.
 */
public class EstimatedLinkExternalDataProviders extends LinkExternalDataProviders {
	
	
	final static Logger logger = LoggerFactory.getLogger(EstimatedLinkExternalDataProviders.class);
	
	public int reservoirsize = 25;
	
	// A set that holds all unique PLDs together with a sampled set of resources
	private Map<String, ReservoirSampler<String>> mapPLDs =  new HashMap<String, ReservoirSampler<String>>();
	
	/**
	 * Processes a single quad making part of the dataset. Determines whether the subject and/or object of the quad 
	 * are data-level URIs, if so, extracts their pay-level domain and adds them to the set of TLD URIs.
	 * @param quad Quad to be processed as part of the computation of the metric
	 */
	@Override
	public void compute(Quad quad) throws MetricProcessingException {
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
					this.addUriToSampler(objectURL);
				}
			}
		}
	}
	
	
	private void addUriToSampler(String uri) {
		String pld = Utils.extractDatasetNS(uri);
		
		if(pld != null) {
			if (this.mapPLDs.containsKey(pld)){
				ReservoirSampler<String> res = this.mapPLDs.get(pld);
				if (res.findItem(uri) == null) res.add(uri);
				mapPLDs.put(pld, res);
			} else {
				ReservoirSampler<String> res = new ReservoirSampler<String>(reservoirsize, true);
				res.add(uri);
				mapPLDs.put(pld, res);
			}
		}
	}

	@Override
	protected void checkForRDFLinks() {	
		ExecutorService service = Executors.newCachedThreadPool();
 		for (ReservoirSampler<String> curPldUris : this.mapPLDs.values()) {
			for (String s : curPldUris.getItems()){
				
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
		}
 		service.shutdown();
 		System.gc();
	}
	
	@Override
	public boolean isEstimate() {
		return true;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		
		//TODO: Add profiling information, including what estimation technique used. See Extensional Conciseness metric
		return activity;
	}
	
//	public static void main(String [] args) {
//		PropertyManager.getInstance().addToEnvironmentVars("dataset-pld", "http://example.org");
//
//		EstimatedLinkExternalDataProviders m = new EstimatedLinkExternalDataProviders();
//		Quad q = new Quad(null, ModelFactory.createDefaultModel().createResource("http://www.getty.edu/research/").asNode(),
//				ModelFactory.createDefaultModel().createProperty(FOAF.Person.getURI()).asNode(),
//				ModelFactory.createDefaultModel().createResource("http://vocab.getty.edu/ulan/500283614").asNode());
//		
//		m.compute(q);
//		
//		System.out.println(m.metricValue());
//	}

}