package io.github.luzzu.linkeddata.qualitymetrics.accessibility.availability;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionQuad;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.QPRO;

/**
 * @author Jeremy Debattista
 * 
 * Check if a SPARQL endpoint (matching void:sparqlEndpoint) is available and returns a result. 
 * 
 */
public class SPARQLAccessibility extends AbstractQualityMetric<Boolean> {

	private final Resource METRIC_URI = DQM.EndPointAvailabilityMetric;
	
	static Logger logger = LoggerFactory.getLogger(SPARQLAccessibility.class);
	
	
	boolean hasAccessibleEndpoint = false;
	
	private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.EndPointAvailabilityMetric);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	
	final static List<String> endpointProperty = new ArrayList<String>();
	static{
		endpointProperty.add(VOID.sparqlEndpoint.getURI());
		endpointProperty.add("http://rdfs.org/sioc/services#service_endpoint");
		endpointProperty.add("http://www.w3.org/ns/sparql-service-description#endpoint");
	}
	
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Computing : {} ", quad.asTriple().toString());
		
		if (!(quad.getSubject().isBlank())) {
			if ((quad.getSubject().getURI().equals(super.getDatasetURI()))
					&& (endpointProperty.contains(quad.getPredicate().getURI()))) {
	
				String sparqlQuerystring = "ASK {?s ?p ?o}";
				Query query = QueryFactory.create(sparqlQuerystring);
				
	
				QueryExecution qexec = QueryExecutionFactory.sparqlService(quad.getObject().toString(), query);
	
				try{
					this.hasAccessibleEndpoint = qexec.execAsk();
					qexec.close();
				} catch (QueryException e){
					logger.error("Endpoint " + quad.getObject().toString() + " responded with : " + e.getMessage());
					// TODO: problem reporting
					// Quad q = new Quad(null, quad.getSubject() , QPRO.exceptionDescription.asNode(), DQMPROB.InvalidSPARQLEndPoint.asNode());
				}
			}
		}
	}
	
	@Override
	public Boolean metricValue() {
		return this.hasAccessibleEndpoint;
	}

	public Resource getMetricURI() {
		return this.METRIC_URI;
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
	public ProblemCollection<?> getProblemCollection() {
		if (requireProblemReport) {
			if (!this.hasAccessibleEndpoint) {
				Quad q = new Quad(null, ResourceCommons.toResource(super.getDatasetURI()).asNode() , QPRO.exceptionDescription.asNode(), DQMPROB.NoEndPointAccessibility.asNode());
				problemCollection.addProblem(q);
			}
		}
		
		return problemCollection;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		return activity;
	}
}
