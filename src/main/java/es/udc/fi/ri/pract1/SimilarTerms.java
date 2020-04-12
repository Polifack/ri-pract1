package es.udc.fi.ri.pract1;

import org.apache.commons.math3.linear.RealVector;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.util.BytesRef;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class SimilarTerms {

	/**
	 * La práctica tendrá una clase principal SimilarTerms con  argumentos -index donde se le indique la
	 * ruta de la carpeta de un índice construido con IndexFiles, -field campo -term term donde se le
	 * indicaun par <término, campo> y -rep donde se le indica una representación que puede ser bin,
	 * tf o tf-idf. Debe tratarse con esa representación de los términos según su ocurrencia en los
	 * documentos, y para cada término obtener los diez mas similares según la similaridad de coseno.
	 * Se visualizará la lista de 10 términos, ordenados de mayor a menor similaridad
	 *
	 * SimilarTerms debe devolver los 10 terminos mas similares al termino pasado como argumento
	 * segun la similaridad pasada por argumento.
	 */

	private static String indexPath;
	private static String representation;
	private static String field;
	private static String term;

	private static class TermValues{
		public String term;
		public double tf;
		public double idf;
		public double tfidf;
		public double tfidflog10;

		public TermValues(String term, double tf, double idf) {
			this.term = term;
			//tf = importancia en el documento
			this.tf = tf;
			// idf = importancia en la colección
			this.idf = idf;
			this.tfidf = tf * idf;
			this.tfidflog10 = tf * Math.log(idf) / Math.log(10);
		}
	}

	private static class TermValuesSorting implements Comparator<TermValues>{
		public int compare(TermValues a, TermValues b)
		{
			if (a.tfidf > b.tfidf) return -1;
			if (b.tfidf > a.tfidf) return 1;
			return 0;
		}
	}

	public static double CosineSimilarity(RealVector v1, RealVector v2){
		return v1.dotProduct(v2)/(v1.getNorm()*v2.getNorm());
	}

	public static ArrayList<TermValues> getTfidf(IndexReader reader, String field, int docID)
			throws IOException {
		//Obtenemos el term vector en el campo 'field' del documento con id 'docID'
		Terms terms = reader.getTermVector(docID, field);

		//El termino no tiene termVectors
		if (terms == null) {
			System.out.println("ERROR: DOCUMENT HAS NO TERM VECTOR");
			return null;
		}

		TermsEnum iterator = terms.iterator();
		BytesRef tempTerm = null;
		TFIDFSimilarity tfidfSim = new ClassicSimilarity();

		int docCount = reader.numDocs();
		PostingsEnum docs = null;
		ArrayList<TermValues> result = new ArrayList<TermValues>();

		//Iteramos sobre los todos los terminos del index
		while ((tempTerm = iterator.next()) != null) {
			Term termInstance = new Term(field, tempTerm);
			long indexDf = reader.docFreq(termInstance);
			double idf = tfidfSim.idf(docCount, indexDf);
			docs = iterator.postings(docs, PostingsEnum.NONE);
			while(docs.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
				double tf = tfidfSim.tf(docs.freq());
				result.add(new TermValues(termInstance.text(),tf,idf));
			}
		}
		//Devolvemos el array de los TermValues para cada termino del documento
		return result;
	}

	static void parseArguments(String[] args){
		String usage = 	"USAGE: java -jar SimilarTerms -term TERM" +
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
					representation = rm;
				}
				if (rm == "tf-idf" || rm =="tf"){
					representation = rm;
				}
				i++;
			} else if ("-field".equals(args[i])) {
				field = args[i+1];
				i++;
			} else if("-term".equals(args[i])) {
				term = args[i+1];
				i++;
			}
		}
	}

	public static void main(final String[] args) throws IOException {
		parseArguments(args);

		Directory dir = null;
		DirectoryReader indexReader = null;

		try {
			dir = FSDirectory.open(Paths.get(indexPath));
			indexReader = DirectoryReader.open(dir);
		} catch (CorruptIndexException e1) {
			System.out.println("Corrupt index error " + e1);
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println("Error opening index " + e1);
			e1.printStackTrace();
		}
		//Obtenemos la lista invertida del termino de referencia
		PostingsEnum referenceTermPosting = MultiTerms.getTermPostingsEnum(indexReader,
				field, new BytesRef(term));
		//Comprobamos que existe
		if (referenceTermPosting!=null){
			int docid;
			//Parseamos la lista entera de documentos en los que aparece el termino
			while ((docid = referenceTermPosting.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
				//DocId contiene el id de un documento en el que aparece el termino solicitado
				//Calculamos los termValues para cada termino del documento
				ArrayList<TermValues> tv = getTfidf(indexReader,field, docid);
				//Lo ordenamos por tfidf
				Collections.sort(tv, new TermValuesSorting());
				//Mostramos los 10 documentos con el tf idf mas alto
				for (int i = 0; i<10;i++){
					System.out.println(tv.get(i).term);
				}

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
