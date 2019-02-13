/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.syntacticvalidity;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionQuad;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;

/**
 * This metric checks the compatability of the literal datatype
 * against the lexical form of the said literal.
 * This metric only catches literals with a datatype
 * whilst untyped literals are not checked in this metric
 * as their lexical form cannot be validated.
 * 
 * Therefore, in order to check for untyped literals,
 * the metric UntypedLiterals in the same dimension
 * checks for such quality problems.
 * 
 * @author Jeremy Debattista
 * 
 */
public class CompatibleDatatype extends AbstractQualityMetric<Double> {

	private static Logger logger = LoggerFactory.getLogger(CompatibleDatatype.class);
	
	private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.CompatibleDatatype);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	private int numberCorrectLiterals = 0;
	private int numberIncorrectLiterals = 0;
	
	private int totalNumberLiterals = 0;

	@Override
	public void compute(Quad quad) throws MetricProcessingException {
		Node obj = quad.getObject();
		
		if (obj.isLiteral()){
			logger.debug("Found Literal object: {}", obj.toString());
			totalNumberLiterals++;
			if (obj.getLiteralDatatype() != null){
				// unknown datatypes cannot be checked for their correctness,
				// but in the UsageOfIncorrectDomainOrRangeDatatypes metric
				// we check if these literals are used correctly against their
				// defined property. We also check for untyped literals in another metric
				if (this.compatibleDatatype(obj)) 
					numberCorrectLiterals++; 
				else {
					this.addToProblem(quad);
					numberIncorrectLiterals++;
				}
			} 
		}
	}
	
	private void addToProblem(Quad q){
		if (requireProblemReport) {
			problemCollection.addProblem(q);
		}
	}
    

	@Override
	public Double metricValue() {
		double metricValue = (double) numberCorrectLiterals / ((double)numberIncorrectLiterals + (double)numberCorrectLiterals);
		statsLogger.info("CompatibleDatatype. Dataset: {} - Total # Correct Literals : {}; # Incorrect Literals : {}; # Metric Value: {}", 
				this.getDatasetURI(), numberCorrectLiterals, numberIncorrectLiterals, metricValue);
		
		if (((Double)metricValue).isNaN())
			metricValue = 1.0d;

		return metricValue;
	}

	@Override
	public Resource getMetricURI() {
		return DQM.CompatibleDatatype;
	}

	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return DQM.LuzzuProvenanceAgent;
	}
	
	private boolean compatibleDatatype(Node lit_obj){
		RDFNode n = ResourceCommons.asRDFNode(lit_obj);
		Literal lt = (Literal) n;
		RDFDatatype dt = lt.getDatatype();
		String stringValue = lt.getLexicalForm();
		
		return dt.isValid(stringValue);
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
		activity.add(mp, DQM.totalNumberOfLiterals, ResourceCommons.generateTypeLiteral(this.totalNumberLiterals));

		return activity;	
	}
}
