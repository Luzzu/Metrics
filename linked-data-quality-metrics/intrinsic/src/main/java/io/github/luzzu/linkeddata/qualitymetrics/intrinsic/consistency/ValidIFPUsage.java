/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import java.util.HashMap;
import java.util.Map;

import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.VocabularyLoader;
import io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency.helper.IFPTriple;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionModel;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;

/**
 * @author Jeremy Debattista
 * 
 * This metric checks if the Inverse Functional Properties (IFP)
 * is used correctly, i.e. if we have S P O and P is set to be
 * an owl:InverseFunctionalProperty, then S is the one and only
 * resource connected to O. If there is a triple S1 P O, then
 * the IFP is not used correctly and thus since S1 will be "reasoned"
 * to be the same as S.
 * 
 * More information can be found in Hogan et. al Weaving the Pedantic Web.
 * 
 */
public class ValidIFPUsage extends AbstractQualityMetric<Double> {
	
	private final Resource METRIC_URI = DQM.ValidIFPUsageMetric;

	private static Logger logger = LoggerFactory.getLogger(ValidIFPUsage.class);
	
	private int totalIFPs = 0;
	private int totalViolatedIFPs = 0;
//	private DB mapDB = MapDbFactory.getMapDBAsyncTempFile(ValidIFPUsage.class.getName());
//	private Map<IFPTriple,IFPTriple> seenIFPs = MapDbFactory.createHashMap(mapDB, "seen-ifp-statements");
	private Map<IFPTriple,IFPTriple> seenIFPs = new HashMap<IFPTriple,IFPTriple>();
	
	private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(DQM.ValidIFPUsageMetric);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();
	int counter = 0;
	
	@Override
	public void compute(Quad quad) {
		counter++;
		logger.debug("Computing : {} ", quad.asTriple().toString());
		
		if (VocabularyLoader.getInstance().checkTerm(quad.getPredicate())){ // if we do not know the predicate, then we assume that it is not an IFP
			if (VocabularyLoader.getInstance().isInverseFunctionalProperty(quad.getPredicate())){
				logger.debug("{} is an IFP", quad.asTriple().toString());
				totalIFPs++;
				
				IFPTriple t = new IFPTriple(quad.asTriple());
				if (seenIFPs.containsKey(t)){
					totalViolatedIFPs++;
					t = seenIFPs.get(t);
					if (requireProblemReport) this.addProblem(t, quad);
				} else {
					seenIFPs.put(t,t);
				}
			}
		}
	}
	
	Model sharedProblemModel = ModelFactory.createDefaultModel();

	private void addProblem(IFPTriple t, Quad q){
		Bag bag = sharedProblemModel.createBag();
		
		Resource problemURI = t.getProblemURI();
		if (!(sharedProblemModel.contains(problemURI, RDF.type, DQMPROB.InverseFunctionalPropertyViolation))){
			sharedProblemModel.add(problemURI, RDF.type, DQMPROB.InverseFunctionalPropertyViolation);
			sharedProblemModel.add(problemURI, DQMPROB.violatedPredicate, ResourceCommons.asRDFNode(q.getPredicate()));
			sharedProblemModel.add(problemURI, DQMPROB.violatedObject, ResourceCommons.asRDFNode(q.getObject()));
			
//			bag = sharedProblemModel.createBag();
			bag.add(t.getSubject());
			sharedProblemModel.add(problemURI, DQMPROB.violatingSubjects, bag);
		}
		
		Resource bagURI = sharedProblemModel.listObjectsOfProperty(problemURI, DQMPROB.violatingSubjects).next().asResource();
		bag = sharedProblemModel.getBag(bagURI);
		sharedProblemModel.remove(problemURI, DQMPROB.violatingSubjects, bag);
			
		bag.add(ResourceCommons.asRDFNode(q.getSubject()));
		sharedProblemModel.add(problemURI, DQMPROB.violatingSubjects, bag);
		
		((ProblemCollectionModel)problemCollection).addProblem(sharedProblemModel, problemURI);
	}
	
	@Override
	public Double metricValue() {
		double metricValue = 1.0;
		
		if (totalIFPs == 0) metricValue = 1.0;
		else metricValue = 1.0 - ((double)totalViolatedIFPs/(double)totalIFPs);
		

		return metricValue;
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

	@Override
	public ProblemCollection<?> getProblemCollection() {
		return this.problemCollection;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.counter));
		
		return activity;
	}
}
