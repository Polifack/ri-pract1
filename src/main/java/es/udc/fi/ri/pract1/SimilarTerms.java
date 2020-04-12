package es.udc.fi.ri.pract1;

import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

public class SimilarTerms {

	/**
	 * La práctica tendrá una clase principal SimilarTerms con  argumentos -index donde se le indique la ruta de la
	 * carpeta de un índice construido con IndexFiles, -field campo -term term donde se le indicaun par
	 * <término, campo> y -rep donde se le indica una representación que puede ser bin, tf o tf-idf.
	 * Debe tratarse con esa representación de los términos según su ocurrencia en los documentos, y para cada
	 * término obtener los diez mas similares según la similaridad de coseno. Se visualizará la lista de 10 términos,
	 * ordenados de mayor a menor similaridad
	 */

	private static class TermValues {
		public String term;
		public double tf;
		public double idf;
		public double tfidf;
		public double tfidflog10;

		public TermValues(String term, double tf, double idf) {
			this.term = term;
			this.tf = tf;
			this.idf = idf;
			this.tfidf = tf * idf;
			this.tfidflog10 = tf * Math.log(idf) / Math.log(10);
		}
	}

	public static ArrayList<TermValues> getTfidf(IndexReader reader, String field, int docID) throws IOException {
		Terms terms = reader.getTermVector(docID, field);
		System.out.println(terms);
		if (terms == null) { //El termino no tiene termVectors
			return null;
		}
		TermsEnum iterator = terms.iterator();

		BytesRef term = null;
		TFIDFSimilarity tfidfSim = new ClassicSimilarity();
		int docCount = reader.numDocs();
		PostingsEnum docs = null;
		ArrayList<TermValues> result = new ArrayList<TermValues>();
		while ((term = iterator.next()) != null) {
			Term termInstance = new Term(field, term);
			long indexDf = reader.docFreq(termInstance);
			double idf = tfidfSim.idf(docCount, indexDf);
			docs = iterator.postings(docs, PostingsEnum.NONE);
			while(docs.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
				double tf = tfidfSim.tf(docs.freq());
				result.add(new TermValues(termInstance.text(),tf,idf));
			}
		}
		return result;
	}


	private static String indexPath;
	private static String representation;
	private static String field;
	private static String term;

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
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}
		System.out.printf("%-20s%-10s%-25s%-10s%-80s\n", "TERM", "DOCID", "FIELD", "FREQ",
				"POSITIONS (-1 means No Positions indexed for this field)");
		//Obtenemos la lista invertida del termino de referencia
		PostingsEnum referenceTermPosting = MultiTerms.getTermPostingsEnum(indexReader, field,new BytesRef(term));

		//Comprobamos que existe
		if (referenceTermPosting!=null){
			int docid;
			while ((docid = referenceTermPosting.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
				int freq = referenceTermPosting.freq(); // get the frequency of the term in the current document
				System.out.printf("%-20s%-10d%-25s%-10d", term, docid, field, freq);
				for (int i = 0; i < freq; i++) {
					System.out.print((i > 0 ? "," : "") + referenceTermPosting.nextPosition());
				}
				System.out.println();
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
