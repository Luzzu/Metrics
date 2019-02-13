/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.versatility;

import java.util.HashSet;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.DCAT;
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
 * Datasets can be represented in different formats, such as RDF/XML, N3,
 * N-Triples etc... The voID vocabulary allows data publishers to define
 * the possible formats in the dataset's metadata using the void:feature 
 * predicate.
 * 
 * In this metric we check if in a dataset has triples descibing
 * the different serialisation formats that a dataset is available in, 
 * using the void:feature. A list of possible serialisation formats 
 * can be found: http://www.w3.org/ns/formats/ 
 * 
 * The metric returns the number of different serialisation formats
 * in a dataset.
 */
public class DifferentSerialisationFormatsCount extends AbstractQualityMetric<Integer>{
	
	private static Logger logger = LoggerFactory.getLogger(DifferentSerialisationFormatsCount.class);
	
	private int featureCount = 0;
	
	private int totalTriples = 0;
	
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	
	private static Set<String> formats = new HashSet<String>();
	static{
		formats.add("http://www.w3.org/ns/formats/JSON-LD");
		formats.add("http://www.w3.org/ns/formats/N3");
		formats.add("http://www.w3.org/ns/formats/N-Triples");
		formats.add("http://www.w3.org/ns/formats/N-Quads");
		formats.add("http://www.w3.org/ns/formats/LD_Patch");
		formats.add("http://www.w3.org/ns/formats/microdata");
		formats.add("http://www.w3.org/ns/formats/OWL_XML");
		formats.add("http://www.w3.org/ns/formats/OWL_Functional");
		formats.add("http://www.w3.org/ns/formats/OWL_Manchester");
		formats.add("http://www.w3.org/ns/formats/POWDER");
		formats.add("http://www.w3.org/ns/formats/POWDER-S");
		formats.add("http://www.w3.org/ns/formats/PROV-N");
		formats.add("http://www.w3.org/ns/formats/PROV-XML");
		formats.add("http://www.w3.org/ns/formats/RDFa");
		formats.add("http://www.w3.org/ns/formats/RDF_JSON");
		formats.add("http://www.w3.org/ns/formats/RDF_XML");
		formats.add("http://www.w3.org/ns/formats/RIF_XML");
		formats.add("http://www.w3.org/ns/formats/SPARQL_Results_XML");
		formats.add("http://www.w3.org/ns/formats/SPARQL_Results_JSON");
		formats.add("http://www.w3.org/ns/formats/SPARQL_Results_CSV");
		formats.add("http://www.w3.org/ns/formats/SPARQL_Results_TSV");
		formats.add("http://www.w3.org/ns/formats/Turtle");
		formats.add("http://www.w3.org/ns/formats/TriG");
	}

	
	private static Set<String> featurePredicates = new HashSet<String>();
	static {
		featurePredicates.add(VOID.feature.getURI());
		featurePredicates.add(DCAT.mediaType.getURI());
	}
	
	private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.DifferentSerialisationsMetric);

	
	@Override
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Assessing {}",quad.asTriple().toString());
		
		Node predicate = quad.getPredicate();
		Node object = quad.getObject();
		totalTriples++;
		
		if (featurePredicates.contains(predicate.getURI())) {
			if ((object.isURI()) && (formats.contains(object.getURI()))) {
				featureCount++;
			}
			else {
				if (requireProblemReport) {
					Quad q = new Quad(null, object, QPRO.exceptionDescription.asNode(), DQMPROB.IncorrectFormatDefined.asNode());
					problemCollection.addProblem(q);
				}
			}
		}
	}

	@Override
	public Integer metricValue() {
		return featureCount;
	}
	

	@Override
	public Resource getMetricURI() {
		return DQM.DifferentSerialisationsMetric;
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
	public ProblemCollection<Quad> getProblemCollection() {
		return this.problemCollection;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.totalTriples));
		
		return activity;
	}

}
