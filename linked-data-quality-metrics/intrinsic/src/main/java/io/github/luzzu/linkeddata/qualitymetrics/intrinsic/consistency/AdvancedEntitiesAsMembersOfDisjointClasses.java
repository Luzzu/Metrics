package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.VocabularyLoader;
import io.github.luzzu.linkeddata.qualitymetrics.commons.mapdb.MapDbFactory;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionModel;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.QPRO;

/**
 * 
 * This metric checks both explicit resource type and their implicit (inferred)
 * subclasses for disjointness
 * 
 * @author Jeremy Debattista
 * 
 * TODO: fix when having a lot of classes
 */
public class AdvancedEntitiesAsMembersOfDisjointClasses extends AbstractQualityMetric<Double> {
	/**
	 * Metric URI
	 */
	private final Resource METRIC_URI = DQM.EntitiesAsMembersOfDisjointClassesMetric;
	/**
	 * logger static object
	 */
	private static Logger logger = LoggerFactory.getLogger(AdvancedEntitiesAsMembersOfDisjointClasses.class);
	
	/**
	 * number of entities that are instances of disjoint classes
	 */
	protected long entitiesAsMembersOfDisjointClasses = 0;
	
	
	/**
	 * the data structure that for each resource collects the classes it's an
	 * instance of
	 */
	protected HTreeMap<String, Set<String>> typesOfResource = MapDbFactory.createHashMap(mapDb, UUID.randomUUID().toString());
	protected Set<String> multipleTypeResource = MapDbFactory.createHashSet(mapDb, UUID.randomUUID().toString());
	
	private long totalResources = 0l;
	
	/**
	 * list of problematic nodes
	 */
	private static DB mapDb = MapDbFactory.getMapDBAsyncTempFile(AdvancedEntitiesAsMembersOfDisjointClasses.class.getName());
	
	private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(METRIC_URI);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	
	private boolean metricCalculated = false;

	/**
	 */
	public void compute(Quad quad) {
		logger.debug("Assessing {}", quad.asTriple().toString());
		try {
			String subject = quad.getSubject().toString();
			Node predicate = quad.getPredicate();
			String object = quad.getObject().toString();

			if (predicate.getURI().equals(RDF.type.getURI())){
				// If we have a triple ?s rdf:type ?o, we add ?o to the list of
				// types of ?s
				Set<String> tmpTypes = typesOfResource.get(subject);
				
				if (tmpTypes == null) {
					tmpTypes = new HashSet<String>();
					tmpTypes.add(object);
					typesOfResource.put(subject, tmpTypes);
					totalResources++;
				} else {
					tmpTypes.add(object);
					typesOfResource.put(subject, tmpTypes);
					multipleTypeResource.add(subject);
				}
			}
		} catch (Exception exception) {
			logger.error(exception.getMessage());
		}
	}

	/**
	 * counts number of entities that are members of disjoint classes
	 * 
	 * @return the number of entities that are members of disjoint classes
	 */
	protected long countEntitiesAsMembersOfDisjointClasses() {
		long count = 0;
		
//		for (Map.Entry<String, Set<String>> entry : typesOfResource.entrySet()) {
		for (String entity : multipleTypeResource){
			// one entity in the dataset …
//			String entity = entry.getKey();
			// … and the classes it's an instance of
			Set<String> classes = typesOfResource.get(entity);
			
			if (classes.size() >= 2) {
				// we only need to check disjointness when there are at least 2 classes
				boolean isDisjoint = false;
				for (String s : classes){
					if (VocabularyLoader.getInstance().checkTerm(ModelFactory.createDefaultModel().createResource(s).asNode())){
						Set<String> _set = this.rdfNodeSetToString(VocabularyLoader.getInstance().getDisjointWith(ModelFactory.createDefaultModel().createResource(s).asNode()));
						
						SetView<String> setView = Sets.intersection(classes, _set);
						if (setView.size() > 0){
							isDisjoint = true;
							createProblemModel(ModelFactory.createDefaultModel().createResource(entity).asNode(), setView);
						}
					}
				}
				if (isDisjoint) count++;
//				SELECT DISTINCT * {
//					<http://social.mercedes-benz.com/mars/schema/InternalModelName> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?s0 . 
//					<http://www.w3.org/2008/05/skos-xl#Label> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?s1 . 
//					?s0 <http://www.w3.org/2002/07/owl#disjointWith> ?s1 . 
//					?s1 <http://www.w3.org/2002/07/owl#disjointWith> ?s0 . }
			}
		}
		metricCalculated = true;
		return count;
	}
	
	
	
	private void createProblemModel(Node resource, SetView<String> setView){
		if (requireProblemReport) {
			Model m = ModelFactory.createDefaultModel();
			
			Resource subject = m.createResource(resource.toString());
			m.add(new StatementImpl(subject, QPRO.exceptionDescription, DQMPROB.MultiTypedResourceWithDisjointedClasses));
			
			for(String s : setView){
				m.add(new StatementImpl(subject, DQMPROB.violatingDisjoinedClass, m.createResource(s)));
			}	
			
			((ProblemCollectionModel)problemCollection).addProblem(m, subject);
		}
	}
	
	/**
	 * Returns metric value for the object of this class
	 * 
	 * @return (number of heterogeneous properties ) / (total number of
	 *         properties)
	 */
	
	public Double metricValue() {
		if (!metricCalculated){
			this.entitiesAsMembersOfDisjointClasses = countEntitiesAsMembersOfDisjointClasses();
		}

		if (totalResources <= 0) {
			logger.warn("Total number of entities in given dataset is found to be zero.");
			return 0.0;
		}

		double metricValue = 1 - ((double) entitiesAsMembersOfDisjointClasses / this.totalResources);

		return metricValue;
	}

	/**
	 * Returns Metric URI
	 * 
	 * @return metric URI
	 */
	
	public Resource getMetricURI() {
		return this.METRIC_URI;
	}


	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return DQM.LuzzuProvenanceAgent;
	}
	
	private Set<String> rdfNodeSetToString(Set<RDFNode> set){
		Set<String> hSet = new HashSet<String>();
		for(RDFNode n : set) hSet.add(n.asResource().getURI());
		return hSet;
	}


	@Override
	public ProblemCollection<?> getProblemCollection() {
		return this.problemCollection;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		activity.add(mp, DQM.totalNumberOfResources, ResourceCommons.generateTypeLiteral(this.totalResources));

		return activity;
	}
}
