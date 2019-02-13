package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.VocabularyLoader;
import io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency.helper.MDC;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualitymetrics.algorithms.ReservoirSampler;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionModel;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.QPRO;

/**
 * @author Jeremy Debattista
 * 
 * An Estimate version for the metric Entities as members of
 * disjoint classes using reservoir sampling
 * 
 * Note: This was not used in the LOD Survey
 */
public class EstimateSimpleEntitiesAsMembersOfDisjointClasses extends AbstractQualityMetric<Double> {

	private final Resource METRIC_URI = DQM.EntitiesAsMembersOfDisjointClassesMetric;
	private static Logger logger = LoggerFactory.getLogger(EstimateSimpleEntitiesAsMembersOfDisjointClasses.class);
	protected long entitiesAsMembersOfDisjointClasses = 0;
	
	private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(METRIC_URI);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	
	private boolean metricCalculated = false;
	
	//Reservoir Settings
	private int MAX_SIZE = 1000000;
	private ReservoirSampler<MDC> reservoir = new ReservoirSampler<MDC>(MAX_SIZE, true);

	public void setMaxSize(int size){
		MAX_SIZE = size;
		reservoir = new ReservoirSampler<MDC>(MAX_SIZE, true);
	}
	
	private long totalTriples = 0;
	private long totalTriplesAssessed = 0;

	
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Assessing {}", quad.asTriple().toString());
		totalTriples++;
		
		try {
			RDFNode subject = ResourceCommons.asRDFNode(quad.getSubject());
			Node predicate = quad.getPredicate();
			RDFNode object = ResourceCommons.asRDFNode(quad.getObject());

			if (RDF.type.asNode().equals(predicate)) {
				totalTriplesAssessed++;
				MDC mdc = new MDC(subject.asResource());
				MDC foundMDC = this.reservoir.findItem(mdc);
				if (foundMDC == null){
					logger.trace("Subject not in reservoir: {} Trying to add it", mdc.subject);

					mdc.objects.add(object);
					this.reservoir.add(mdc);
				} else {
					foundMDC.objects.add(object);
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
		for (MDC mdc : this.reservoir.getItems()){
			if (mdc.objects.size() >= 2){
				Iterator<RDFNode> iter = new ArrayList<RDFNode>(mdc.objects).iterator();
				Set<RDFNode> checked = new HashSet<RDFNode>();
				while (iter.hasNext()){
					RDFNode _class = iter.next();
					checked.add(_class);
					Model model = VocabularyLoader.getInstance().getModelForVocabulary(_class.asResource().getNameSpace());
					Set<RDFNode> disjoinedClasses = model.listObjectsOfProperty(_class.asResource(), OWL.disjointWith).toSet();
					disjoinedClasses.retainAll(mdc.objects);
					disjoinedClasses.removeAll(checked);
					if (disjoinedClasses.size() > 0){
						count++;
						this.createProblemModel(mdc.subject, _class.asNode(), disjoinedClasses);
					} 
				}
			}
		}
		
		metricCalculated = true;
		return count;
	}
	
	
	private void createProblemModel(Resource subject, Node _class, Set<RDFNode> _otherClasses){
		if (requireProblemReport) {
			Model m = ModelFactory.createDefaultModel();
			
			m.add(new StatementImpl(subject, QPRO.exceptionDescription, DQMPROB.MultiTypedResourceWithDisjointedClasses));
			
			
			m.add(new StatementImpl(subject, DQMPROB.violatingDisjoinedClass, m.asRDFNode(_class)));	
			for(RDFNode s : _otherClasses){
				m.add(new StatementImpl(subject, DQMPROB.violatingDisjoinedClass, s));
			}
			
			((ProblemCollectionModel)problemCollection).addProblem(m, subject);
		}
	}
		

	public Double metricValue() {
		if (!metricCalculated){
			this.entitiesAsMembersOfDisjointClasses = countEntitiesAsMembersOfDisjointClasses();
		}
		
		if (this.reservoir.getItems().size() <= 0) {
			logger.warn("Total number of entities in given dataset is found to be zero.");
			return 0.0;
		}

		double metricValue = 1 - ((double) entitiesAsMembersOfDisjointClasses / this.reservoir.size());


		return metricValue;
	}

	public Resource getMetricURI() {
		return this.METRIC_URI;
	}


	public boolean isEstimate() {
		return true;
	}

	public Resource getAgentURI() {
		return DQM.LuzzuProvenanceAgent;
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
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.totalTriples));
		activity.add(mp, DAQ.totalDatasetTriplesAssessed, ResourceCommons.generateTypeLiteral(this.totalTriplesAssessed));
		activity.add(mp, DAQ.estimationTechniqueUsed, ModelFactory.createDefaultModel().createResource("http://dbpedia.org/resource/Simple_random_sample"));

		
		Resource ep = ResourceCommons.generateURI();
		activity.add(mp, DAQ.estimationParameter, ep);
		activity.add(ep, RDF.type, DAQ.EstimationParameter);
		activity.add(ep, DAQ.estimationParameterValue, ResourceCommons.generateTypeLiteral(this.MAX_SIZE));
		activity.add(ep, DAQ.estimationParameterKey, ResourceCommons.generateTypeLiteral("k"));
		
		return activity;
	}	
}
