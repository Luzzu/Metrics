/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.VocabularyLoader;
import io.github.luzzu.linkeddata.qualitymetrics.commons.mapdb.MapDbFactory;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualitymetrics.algorithms.ReservoirSampler;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionModel;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;

/**
 * @author Jeremy Debattista
 * 
 * This metric tests if a property's domain and range
 * are of the same type as declared in the corresponding
 * schema.
 * 
 */
public class EstimatedUsageOfIncorrectDomainOrRangeDatatypes extends AbstractQualityMetric<Double> {

	private static final int MAX_SIZE = 1000000;

	private final Resource METRIC_URI = DQM.UsageOfIncorrectDomainOrRangeDatatypesMetric;

	private static Logger logger = LoggerFactory.getLogger(EstimatedUsageOfIncorrectDomainOrRangeDatatypes.class);
	
	private static DB mapDb = MapDbFactory.getMapDBAsyncTempFile(EstimatedUsageOfIncorrectDomainOrRangeDatatypes.class.getName());
	
	private HTreeMap<String, List<String>> mapResourceType =  MapDbFactory.createHashMap(mapDb, UUID.randomUUID().toString());

	private ReservoirSampler<Triple> unknownTriples = new ReservoirSampler<Triple>(MAX_SIZE, false);

	private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(METRIC_URI);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();


	private long incorrectDomain = 0;
	private long incorrectRange = 0;
	private long undereferenceablePredicates = 0;
	private long unknownDomainAndRange = 0;

	private ModelCom mc = new ModelCom(Graph.emptyGraph);

	private long totalTriples = 0;
	
	@Override
	public void compute(Quad quad) throws MetricProcessingException {
		
		logger.debug("Computing : {} ", quad.asTriple().toString());
		totalTriples++;
		
		if (quad.getPredicate().getURI().equals(RDF.type.getURI())) {
			String s = "";
			if (quad.getSubject().isBlank()) s = quad.getSubject().getBlankNodeLabel();
			else s = quad.getSubject().getURI();
			
			String o = "";
			if (quad.getObject().isURI()) {
				o = quad.getObject().getURI();
				List<String> types = new ArrayList<String>();

				if (mapResourceType.containsKey(s)){
					types = mapResourceType.get(s);
				} 
				types.add(o);
				mapResourceType.put(s, types);
			}
			else
				logger.error("Objects for types have to be uris. {}", quad);

						
		}
		else {
			this.unknownTriples.add(quad.asTriple());
		}
	}
	
	private void addToProblem(Quad q, char type){
		if (requireProblemReport) {
		    	Model m = ModelFactory.createDefaultModel();
		    	
		    	Resource gen = ResourceCommons.generateURI();
		    	if (type == 'd')
		    		m.add(gen, RDF.type, DQMPROB.IncorrectDomain);
		    	if (type == 'r')
		    		m.add(gen, RDF.type, DQMPROB.IncorrectRange);
		    	if (type == 'x')
		    		m.add(gen, RDF.type, DQMPROB.UnknownType);
		    	if (type == 'u')
		    		m.add(gen, RDF.type, DQMPROB.UnknownType);
	
		    	
		    	Resource anon = m.createResource();
		    	m.add(gen, DQMPROB.problematicTriple, anon);
		    	m.add(anon, RDF.subject, ResourceCommons.asRDFNode(q.getSubject()));
		    	m.add(anon, RDF.predicate, ResourceCommons.asRDFNode(q.getPredicate()));
		    	m.add(anon, RDF.object, ResourceCommons.asRDFNode(q.getObject()));
		    	
		    ((ProblemCollectionModel)this.problemCollection).addProblem(m,gen);
		}
	}
    
	
	@Override
	public Double metricValue() {
		
		List<Triple> triples = this.unknownTriples.getItems();
		
		triples.forEach(t -> {
			if (VocabularyLoader.getInstance().checkTerm(t.getPredicate())){
				checkDomain(t);
				checkRange(t);
			} else {
				undereferenceablePredicates++;
			}
		});
		
		double value = 1 - ((double) incorrectDomain + (double) incorrectRange + (double) undereferenceablePredicates + (double) unknownDomainAndRange) / ((double) this.unknownTriples.size() * 2);

		return value;
	}

	
	private void checkDomain(Triple t){
		String subURI = (t.getSubject().isBlank()) ? t.getSubject().toString() : t.getSubject().getURI();

		if (mapResourceType.containsKey(subURI)){
			List<String> _typeList = mapResourceType.get(subURI);
			
			Set<RDFNode> types = new LinkedHashSet<RDFNode>();
			//TODO: what about underef properties?
//			types.add(RDFS.Resource); // this is required as everything is a resource, useful for those undereferenceable properties
			for (String type : _typeList){
				types.add(mc.createResource(type));
				types.addAll(VocabularyLoader.getInstance().inferParentClass(mc.createResource(type).asNode()));
			}
			
			Set<RDFNode> _dom = VocabularyLoader.getInstance().getPropertyDomain(t.getPredicate());
			// Remove owl:Thing and rdfs:Resource as all resources are implicity defined as members of THING and RESOURCE
			_dom.remove(OWL.Thing);
			_dom.remove(RDFS.Resource);
			if (_dom.size() > 0){ // do not consider those properties which have an open domain
				if(Sets.intersection(_dom, types).size() == 0){
					addToProblem(new Quad(null, t),'d');
					incorrectDomain++;
				}
			}
		} else {
			addToProblem(new Quad(null, t),'x');
			unknownDomainAndRange++;
		}
	}
	
	private void checkRange(Triple t){
		Set<RDFNode> _ran = VocabularyLoader.getInstance().getPropertyRange(t.getPredicate());
		// Remove owl:Thing and rdfs:Resource as all resources are implicity defined as members of THING and RESOURCE
		_ran.remove(OWL.Thing);
		_ran.remove(RDFS.Resource);
		if (_ran.size() > 0){ // do not consider those properties which have an open range
			if (t.getObject().isLiteral()){
				Resource litRes = this.getLiteralType(t.getObject());
				if (!_ran.contains(litRes)){
					addToProblem(new Quad(null, t),'r');
					incorrectRange++;
				}
			} else {
				String objURI = (t.getObject().isBlank()) ? t.getObject().toString() : t.getObject().getURI();
				
				if (mapResourceType.containsKey(objURI)){
					List<String> _typeList = mapResourceType.get(objURI);
					
					Set<RDFNode> types = new LinkedHashSet<RDFNode>();
					
					for (String type : _typeList){
						types.add(mc.createResource(type));
						types.addAll(VocabularyLoader.getInstance().inferParentClass(mc.createResource(type).asNode()));
					}
					
					if(Sets.intersection(_ran, types).size() == 0){
						addToProblem(new Quad(null, t),'r');
						incorrectRange++;
					}
				} else {
					addToProblem(new Quad(null, t),'u');
					unknownDomainAndRange++;
				}
			}
		}
	}
	
	
	
	@Override
	public Resource getMetricURI() {
		return METRIC_URI;
	}


	@Override
	public boolean isEstimate() {
		return true;
	}

	@Override
	public Resource getAgentURI() {
		return DQM.LuzzuProvenanceAgent;
	}
	
	
	private Resource getLiteralType(Node lit_obj){
		RDFNode n = ResourceCommons.asRDFNode(lit_obj);
		Literal lt = (Literal) n;

		if (((Literal)n).getDatatype() != null){
			if (lt.getDatatype().getURI().equals(RDF.dtLangString.getURI())) return XSD.xstring;
			if (lt.getDatatype().getURI().equals(RDF.langString.getURI())) return XSD.xstring;
			return  ModelFactory.createDefaultModel().createResource(lt.getDatatype().getURI());
		} else {
			if (lt.getValue() instanceof Byte) return XSD.xbyte;
			else if (lt.getValue() instanceof Boolean) return XSD.xboolean;
			else if (lt.getValue() instanceof Short) return XSD.xshort;
			else if (lt.getValue() instanceof Integer) return XSD.xint;
			else if (lt.getValue() instanceof Long) return XSD.xlong;
			else if (lt.getValue() instanceof Float) return XSD.xfloat;
			else if (lt.getValue() instanceof Double) return XSD.xdouble;
			else if (lt.getValue() instanceof String) return XSD.xstring;
			else return RDFS.Literal;
		}
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
		activity.add(mp, DAQ.estimationTechniqueUsed, ModelFactory.createDefaultModel().createResource("http://dbpedia.org/resource/Simple_random_sample"));

		
		Resource ep = ResourceCommons.generateURI();
		activity.add(mp, DAQ.estimationParameter, ep);
		activity.add(ep, RDF.type, DAQ.EstimationParameter);
		activity.add(ep, DAQ.estimationParameterValue, ResourceCommons.generateTypeLiteral(MAX_SIZE));
		activity.add(ep, DAQ.estimationParameterKey, ResourceCommons.generateTypeLiteral("k"));
		
		return activity;
	}
}
