package es.udc.fi.ri.pract1;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import org.apache.lucene.document.Document;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class SimilarDocs {

	/**
	 * SimilarDocs con argumentos -index, -rep, -docID y -field. 
	 * Para el documento con docID de Lucene que se le pasa como valor a -docID y 
	 * campo que se le pasacomo valor a -field, y usando las representación pasada 
	 * como valor a -rep, se obtienen y devuelve lalista de 10 documentos mas 
	 * similares, ordenados de mayor a menor similaridad, con respecto al quese 
	 * le pasa como argumento.
	 */

	static String indexPath = null;
	static String representationMode = null;
	static String field = null;
	static String partialIndex = null;
	static int docId;
	static String representation = null;
	static Similarity similarity = null;
	private static Set<String> terms = new HashSet<>();

	static Map<String, Integer> getTermFrequencies(IndexReader reader, int id) throws IOException {
		System.out.println("[*] Retrieving Term Vector for the document with id: "+id);
		//Obtenemos el TermVector para el field 'field' para el documento de id 'id'
		Terms vector = reader.getTermVector(id, field);
		System.out.println("Vector: "+vector);

		TermsEnum termsEnum = null;
		termsEnum = vector.iterator();

		System.out.println("TermsEnum: "+termsEnum);

		Map<String, Integer> frequencies = new HashMap<>();
		BytesRef text = null;

		//Iteramos por el termsEnum
		while ((text = termsEnum.next()) != null) {
			String term = text.utf8ToString();
			int freq = (int) termsEnum.totalTermFreq();
			System.out.println("Storeando "+term+" con frecuencia "+freq);
			frequencies.put(term, freq);
			terms.add(term);
		}
		System.out.println("Frequencies: "+frequencies);
		return frequencies;
	}

	static void parseArguments(String[] args){
		String usage = 	"USAGE: java -jar SimilarDocs -docID DOCUMENT_ID" +
				"-rep REPRESENTATION_MODE -index INDEX_PATH -field FIELD";

		if (args.length<8) {
			System.out.println(usage);
			System.exit(0);
		}

		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-rep".equals(args[i])) {
				String rm = args[i+1];
				//bin
				if (rm == "bin"){
					similarity = new BooleanSimilarity();
					representation = rm;
				}
				if (rm == "tf-idf" || rm =="tf"){
					//classic similarity es una implementacion de tfidf
					similarity = new ClassicSimilarity();
					representation = rm;
				}

				i++;
			} else if ("-field".equals(args[i])) {
				field = args[i+1];
				i++;
			} else if("-docID".equals(args[i])) {
				docId =  Integer.parseInt(args[i + 1]);
				i++;
			}
		}
	}
	public static void getBestDocuments(){
		/*
		 * l = new List<float, indexlemenet>
		 * foreach element in the index
		 * compute similarity.tf() or similarity.idf() or similarity.bin()
		 * l.add(similarity,element)
		 * sort l by similarity
		 * return 10 highest values of l
		 */
	}
	static double getCosineSimilarity(RealVector v1, RealVector v2) {
		System.out.println(v1.getNorm());
		System.out.println(v2.getNorm());
		return (v1.dotProduct(v2)) / (v1.getNorm() * v2.getNorm());
	}
	static RealVector toRealVector(Map<String, Integer> map) {
		System.out.println("Terms size: "+terms);
		RealVector vector = new ArrayRealVector(terms.size());
		int i = 0;
		for (String term : terms) {
			int value = map.containsKey(term) ? map.get(term) : 0;
			vector.setEntry(i++, value);
		}
		return (RealVector) vector.mapDivide(vector.getL1Norm());
	}



	public static void main(final String[] args) throws IOException {

		parseArguments(args);
		Directory dir = null;
		DirectoryReader indexReader = null;

		try {
			dir = FSDirectory.open(Paths.get(indexPath));
			indexReader = DirectoryReader.open(dir);
		} catch (CorruptIndexException e1) {
			System.out.println("Corrupted index " + e1);
			e1.printStackTrace();
			System.exit(-1);
		} catch (IOException e1) {
			System.out.println("Error reading index " + e1);
			e1.printStackTrace();
			System.exit(-1);
		}

		Document doc = null;
		List<IndexableField> fields = null;

		Map<String, Integer> tr = getTermFrequencies(indexReader,docId);
		RealVector vr = toRealVector(tr);

		for (int i = 0; i < indexReader.numDocs(); i++) {

			try {
				doc = indexReader.document(i);
			} catch (CorruptIndexException e1) {
				System.out.println("Graceful message: exception " + e1);
				e1.printStackTrace();
			} catch (IOException e1) {
				System.out.println("Graceful message: exception " + e1);
				e1.printStackTrace();
			}

			System.out.println("Documento " + i);

			fields = doc.getFields();
			// Note doc.getFields() gets the stored fields



			RealVector vi = toRealVector(getTermFrequencies(indexReader,i));
			getCosineSimilarity(vr,vi);



			for (IndexableField field : fields) {
				String fieldName = field.name();
				System.out.println(fieldName + ": " + doc.get(fieldName));
			}

		}
		try {
			indexReader.close();
			dir.close();
		} catch (IOException e) {
			System.out.println("Graceful message: exception " + e);
			e.printStackTrace();
		}
	}

}