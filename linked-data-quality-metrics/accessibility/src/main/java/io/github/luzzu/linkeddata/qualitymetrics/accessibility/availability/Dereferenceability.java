/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.accessibility.availability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.accessibility.availability.helper.Dereferencer;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.HTTPRetriever;
import io.github.luzzu.linkeddata.qualitymetrics.commons.cache.LinkedDataMetricsCacheManager;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualitymetrics.commons.cache.CachedHTTPResource;
import io.github.luzzu.qualitymetrics.commons.datatypes.HTTPDereference.StatusCode;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionQuad;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.QPRO;

/**
 * @author Jeremy Debatista
 * 
 * This metric calculates the number of valid redirects (303) or hashed links
 * according to LOD Principles
 * 
 * Based on: <a href="http://www.hyperthing.org/">Hyperthing - A linked data Validator</a>
 * 
 * @see <a href="http://dl.dropboxusercontent.com/u/4138729/paper/dereference_iswc2011.pdf">
 * Dereferencing Semantic Web URIs: What is 200 OK on the Semantic Web? - Yang et al.</a>
 * 
 */
public class Dereferenceability extends AbstractQualityMetric<Double> {
	
	private final Resource METRIC_URI = DQM.DereferenceabilityMetric;

	final static Logger logger = LoggerFactory.getLogger(Dereferenceability.class);
	
	private double metricValue = 0.0;
	private double totalURI = 0;
	private double dereferencedURI = 0;
	private Long totalNumberOfTriplesAssessed = 0l;
	
	private HTTPRetriever httpRetreiver = new HTTPRetriever();
	private LinkedDataMetricsCacheManager dcmgr = LinkedDataMetricsCacheManager.getInstance();
	private Queue<String> notFetchedQueue = new ConcurrentLinkedQueue<String>();
	private Set<String> uriSet = Collections.synchronizedSet(new HashSet<String>());
	private boolean metricCalculated = false;
	
	
	private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.DereferenceabilityMetric);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Computing : {} ", quad.asTriple().toString());
		
		if (!(quad.getPredicate().getURI().equals(RDF.type.getURI()))){ // we are currently ignoring triples ?s a ?o
			totalNumberOfTriplesAssessed++;
			String subject = quad.getSubject().toString();
			if (httpRetreiver.isPossibleURL(subject)){
				uriSet.add(subject);
			}
			
			String object = quad.getObject().toString();
			if (httpRetreiver.isPossibleURL(object)){
				uriSet.add(object);
			}
		}
	}

	public Resource getMetricURI() {
		return this.METRIC_URI;
	}

	public Double metricValue() {
		if (!this.metricCalculated){
			ArrayList<String> uriList = new ArrayList<String>();
			uriList.addAll(uriSet);
			httpRetreiver.addListOfResourceToQueue(uriList);
			httpRetreiver.start(true);

			do {
				this.startDereferencingProcess();
				uriSet.clear();
				uriSet.addAll(this.notFetchedQueue);
				this.notFetchedQueue.clear();
			// Continue trying to dereference all URIs in uriSet, that is, those not fetched up to now
			} while(!this.uriSet.isEmpty());
			
			this.metricCalculated = true;
		}
		this.metricValue = this.dereferencedURI / this.totalURI;
		
		return this.metricValue;
	}
	
	private void startDereferencingProcess() {
		for(String uri : uriSet){
			CachedHTTPResource httpResource = (CachedHTTPResource) dcmgr.getFromCache(LinkedDataMetricsCacheManager.HTTP_RESOURCE_CACHE, uri);			
			if (httpResource == null || httpResource.getStatusLines() == null) {
				this.notFetchedQueue.add(uri);
			} else {
				this.totalURI++;
				
				if (Dereferencer.hasValidDereferencability(httpResource)) {
					dereferencedURI++;
				}
				
				if (requireProblemReport) createProblemReport(httpResource);
				
				logger.trace("{} - {} - {}", uri, httpResource.getStatusLines(), httpResource.getDereferencabilityStatusCode());
			}
		}
	}
	
	private void createProblemReport(CachedHTTPResource httpResource){
		StatusCode sc = httpResource.getDereferencabilityStatusCode();
		
		switch (sc){
			case SC200 : this.createProblemQuad(httpResource.getUri(), DQMPROB.SC200OK); break;
			case SC301 : this.createProblemQuad(httpResource.getUri(), DQMPROB.SC301MovedPermanently); break;
			case SC302 : this.createProblemQuad(httpResource.getUri(), DQMPROB.SC302Found); break;
			case SC307 : this.createProblemQuad(httpResource.getUri(), DQMPROB.SC307TemporaryRedirectory); break;
			case SC3XX : this.createProblemQuad(httpResource.getUri(), DQMPROB.SC3XXRedirection); break;
			case SC4XX : this.createProblemQuad(httpResource.getUri(), DQMPROB.SC4XXClientError); break;
			case SC5XX : this.createProblemQuad(httpResource.getUri(), DQMPROB.SC5XXServerError); break;
			case SC303 : if (!httpResource.isContentParsable())  this.createProblemQuad(httpResource.getUri(), DQMPROB.SC303WithoutParsableContent); break;
			default	   : break;
		}
	}
	
	private void createProblemQuad(String resource, Resource problem){
		Quad q = new Quad(null, ModelFactory.createDefaultModel().createResource(resource).asNode(), QPRO.exceptionDescription.asNode(), problem.asNode());
		this.problemCollection.addProblem(q);
	}

	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return 	DQM.LuzzuProvenanceAgent;
	}


	@Override
	public ProblemCollection<Quad> getProblemCollection() {
		return this.problemCollection;
	}

	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		
		activity.add(mp, DAQ.totalDatasetTriplesAssessed, ResourceCommons.generateTypeLiteral((long)this.totalNumberOfTriplesAssessed));
		activity.add(mp, DQM.totalNumberOfResources, ResourceCommons.generateTypeLiteral((long)this.totalURI));
		activity.add(mp, DQM.totalValidDereferenceableURIs, ResourceCommons.generateTypeLiteral((int)this.dereferencedURI));

		return activity;
	}
}
