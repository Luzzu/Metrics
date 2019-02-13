/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.contextual.provenance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.PROV;

/**
 * @author Jeremy Debattista
 * 
 * According the the Data on the Web BP W3C WG, "consumers
 * need to know the origin or history of the published data... 
 * data published should include or link to provenance information".
 * 
 * This metric checks if a dataset has the required provenance information
 * that would enable the consumer to know the origin (where), the owner (who)
 * and the activity that create the triple (how). In this metric
 * we consider the following requirements:
 * 1) Identification of an Agent;
 * 2) Identification of Activities in an Entity;
 * 3) Identification of DataSource in an Activity;
 * 4) Identification of an Agent for an Activity .
 * 
 * The metric value is calculated as follows:
 * SUM(value of Entities) / number of entities.
 * 
 * The value of entities calculation is done as follows:
 * An agent and activities are both given a weighting of 0.5.
 * The 0.5 weighting for activities are split amongst all activities.
 * 
 * Agent and Datasource are equally given a weighting of 0.5 per activity.
 * 
 * In this metric, we measure how much datasets conform to the W3C standard
 * PROV-O ontology.
 * 
 */
public class ExtendedProvenanceMetric extends AbstractQualityMetric<Double> {
	
	private ConcurrentMap<String, Entity> entityDirectory = new ConcurrentHashMap<String, Entity>();
	private ConcurrentMap<String, Activity> activityDirectory =  new ConcurrentHashMap<String, Activity>();

	private static Logger logger = LoggerFactory.getLogger(ExtendedProvenanceMetric.class);

	private long totalTriples = 0;

	@Override
	public void compute(Quad quad) {
		logger.debug("Assessing {}",quad.asTriple().toString());
		Node predicate = quad.getPredicate();
		Node object = quad.getObject();
		
		//is it an identification of an Agent?
		if (predicate.hasURI(PROV.wasAttributedTo.getURI())){
			Entity e = this.getOrPutEntity(quad.getSubject().toString());
			e.agent = quad.getObject().toString();
		}
		
		//is it an activity in an entity?
		if (predicate.hasURI(PROV.wasGeneratedBy.getURI())){
			Activity a = this.getOrPutActivity(quad.getObject().toString());
			activityDirectory.put(quad.getSubject().toString(), a);
			Entity e = this.getOrPutEntity(quad.getSubject().toString());
			e.activities.add(a);
		}
		
		//is it an identification of a datasource in an activity?
		if (predicate.hasURI(PROV.used.getURI())){
			Activity a = this.getOrPutActivity(quad.getSubject().toString());
			a.datasource = quad.getObject().toString();
		}
		
		//is it an identification of an agent in an activity?
		if (predicate.hasURI(PROV.wasAssociatedWith.getURI()) || predicate.hasURI(PROV.actedOnBehalfOf.getURI())){
			Activity a = null;
				a = this.getOrPutActivity(quad.getSubject().toString());
			a.agent = quad.getObject().getURI();
		}
		
		//is it an entity?
		if (object.hasURI(PROV.Entity.getURI())){
				this.getOrPutEntity(quad.getSubject().toString());
		}
		
		//is it an activity?
		if (object.hasURI(PROV.Activity.getURI())){
				this.getOrPutActivity(quad.getSubject().toString());
		}
	}

	private Entity getOrPutEntity(String uri){
		Entity e = new Entity();
		if (entityDirectory.containsKey(uri))
			e = entityDirectory.get(uri);
		else entityDirectory.put(uri, e);
		e.uri = (e.uri == null) ? uri : e.uri;
		return e;
	}
	
	private Activity getOrPutActivity(String uri){
		Activity a = new Activity();
		if (activityDirectory.containsKey(uri))
			a = activityDirectory.get(uri);
		else activityDirectory.put(uri, a);
		a.uri = (a.uri == null) ? uri : a.uri;
		return a;
	}
	
	
	@Override
	public Double metricValue() {
		// The metric value is calculated by taking the summation
		// of all entities and divide them by the number of entities
		// available in a dataset.
		double val = 0.0;
		for (Entity e : entityDirectory.values()) val += e.getBasicValue();
		
		return (entityDirectory.size() == 0) ? 0.0 : (val / (double)entityDirectory.size());
	}

	@Override
	public Resource getMetricURI() {
		return DQM.ExtendedProvenanceMetric;
	}


	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return DQM.LuzzuProvenanceAgent;
	}
	
	
	class Entity {
		protected String uri = null;
		protected String agent = null;
		protected boolean publisherOrCreator = false;
		protected List<Activity> activities = new ArrayList<Activity>();
		
		protected double getBasicValue(){
			// An agent and activities are both given a weighting of 0.5.
			// The 0.5 weighting for activities are split amongst all activities
			double val = 0.0;
			val = (agent == null) ? val + 0.0 : val + 0.5;
			
			double valAct = 0.0;
			for(Activity a : activities) valAct += a.getBasicValue();
			
			if (valAct > 0.0){
				valAct = (activities.size() == 0)? 0.0 : 0.5 * Math.abs((valAct / (double)activities.size())); //normalising value between 0 and 0.5 
				val += valAct;
			}	
			return val;
		}
	}
	
	
	class Activity {
		protected String uri = null;
		protected String agent = null;
		protected String datasource = null;
		
		protected double getBasicValue(){
			double val = 0.0;
			val = (datasource == null) ? val + 0.0 : val + 0.5;
			val = (agent == null) ? val + 0.0 : val + 0.5;
			return val;
		}
	}


	@Override
	public ProblemCollection<?> getProblemCollection() {
		return null;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral((int)this.totalTriples));
		
		return activity;	
	}

}
