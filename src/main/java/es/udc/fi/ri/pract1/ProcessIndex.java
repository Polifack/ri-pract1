package es.udc.fi.ri.pract1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.packed.PackedInts.Writer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.util.Bits;
import org.apache.lucene.index.PostingsEnum;

public class ProcessIndex {
	
	static String inputIndexPath = null;
	static String outputIndexPath = null;
	
	static void parseArguments(String[] args){
		String usage = 	"USAGE: java -jar ProcessIndex [-inputindex PATH] [-outputindex PATH]";
		
		
		if (args.length <= 2) {
			System.out.println(usage);
			System.exit(0);	
		}
		if (args[0]=="--help") {
			System.out.println(usage);
			System.exit(0);
		}

		for (int i = 0; i < args.length; i++) {
			if ("-inputindex".equals(args[i])) {
				inputIndexPath = args[i + 1];
				i++;
			}
			else if ("-outputindex".equals(args[i])) {
				outputIndexPath = args[i + 1];
				i++;
			}
		}
	}
	
	
	private static class TermValues {
		public String term;
		public double tf;
		public double df;
		public double tfidf;
		public double tfidflog10;
		
		public TermValues(String term, double tf, double idf) {
			this.term = term;
			this.tf = tf;
			this.df = idf;
			this.tfidf = tf * idf;
			this.tfidflog10 = tf * Math.log(idf) / Math.log(10);
		}
	}

	public static ArrayList<TermValues> getTfidf(IndexReader reader, String field, int docID) throws IOException {
		
		Terms terms = reader.getTermVector(docID, field);
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
	            
	        	//double tfidf = tfidfSim.tf(docs.freq()) * tfidfSim.idf(docCount, indexDf);

	            double tf = tfidfSim.tf(docs.freq());
	            
	            //System.out.println(termInstance+" "+tf*idf);
	            result.add(new TermValues(termInstance.text(),tf,idf));
	        }
	    }
	    return result;

	    /*while ((term = iterator.next()) != null) {
	        String termText = term.utf8ToString();
	        Term termInstance = new Term(field, term);
	        // term and doc frequency in all documents

	        long indexTf = reader.totalTermFreq(termInstance); 
	        long indexDf = reader.docFreq(termInstance);       
	        double tfidf = tfidfSim.tf(indexTf) * tfidfSim.idf(docCount, indexDf);
	        // store it, but that's not the problem
	        System.out.println(termInstance+" "+tfidf);
	    }*/
	}
	
	public static ArrayList<TermValues> get5Highest(ArrayList<TermValues> list){
		ArrayList<TermValues> result = new ArrayList<TermValues>();
		for (int j=0;j<=5;j++) {
			double max = Double.MIN_VALUE;
		    int max_index = 0;
		    for(int i=0; i<list.size(); i++){
		        if(list.get(i).tfidf > max){
		            max = list.get(i).tfidf;
		            max_index = i;
		        }
		    }
    		result.add(list.get(max_index));
    		list.remove(max_index);
    	}
		return result;
	}
	
	public static void main(String[] args) {
        
		parseArguments(args);
        System.out.println("Processing index in "+inputIndexPath);
        System.out.println("The modified index will be saved in "+outputIndexPath);
        System.out.println("\n");
        
        Directory dir;
        Directory odir;
		try {
			dir = FSDirectory.open(Paths.get(inputIndexPath));
			odir = FSDirectory.open(Paths.get(outputIndexPath));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(OpenMode.CREATE);
			IndexWriter outputWriter = new IndexWriter(odir, iwc);
			IndexReader inputReader = DirectoryReader.open(dir);
			
			for (int i=0; i<inputReader.maxDoc(); i++) {
			   
			    Document doc = inputReader.document(i);
			    String docPath = doc.get("path");
			    System.out.println("\nProcessing document "+docPath);
			    
			    ArrayList<TermValues> termsContents = getTfidf(inputReader, "contents",i);
			    if (termsContents != null) {
			    	System.out.println("Added new field bestTermsContents to document");
			    	ArrayList<TermValues> bestTermsContents = get5Highest(termsContents);
			    	for (TermValues term : bestTermsContents) {
			    		doc.add(new TextField("bestTermsContents",term.term+
			    												" "+Double.toString(term.tf)+
			    												" "+Double.toString(term.df)+
			    												" "+Double.toString(term.tfidflog10)
			    												,Field.Store.YES));
			    		System.out.println("--Added new term to field--: "+term.term+" "+Double.toString(term.tf)+" "+Double.toString(term.df)+" "+Double.toString(term.tfidflog10));
			    	}
			    	
			    }
			    ArrayList<TermValues> termsTop5Lines = getTfidf(inputReader, "top5Lines",i);
			    if (termsTop5Lines != null) {
			    	System.out.println("Added new field bestTermsTop5Lines to document");
			    	ArrayList<TermValues> bestTermsTop5Lines = get5Highest(termsTop5Lines);
			    	for (TermValues term : bestTermsTop5Lines) {
			    		doc.add(new TextField("bestTermsTop5Lines",term.term+
			    												" "+Double.toString(term.tf)+
			    												" "+Double.toString(term.df)+
			    												" "+Double.toString(term.tfidflog10)
			    												,Field.Store.YES));
			    		System.out.println("--Added new term to field--: "+term.term+" "+Double.toString(term.tf)+" "+Double.toString(term.df)+" "+Double.toString(term.tfidflog10));
			    	}
			    	
			    }
			    ArrayList<TermValues> termsBottom5Lines = getTfidf(inputReader, "bottom5Lines",i);
			    if (termsBottom5Lines != null) {
			    	System.out.println("Added new field bestTermsBottom5Lines to document");
			    	ArrayList<TermValues> bestTermsBottom5Lines = get5Highest(termsBottom5Lines);
			    	for (TermValues term : bestTermsBottom5Lines) {
			    		doc.add(new TextField("bestTermsBottom5Lines",term.term+
			    												" "+Double.toString(term.tf)+
			    												" "+Double.toString(term.df)+
			    												" "+Double.toString(term.tfidflog10)
			    												,Field.Store.YES));
			    		System.out.println("--Added new term to field--: "+term.term+" "+Double.toString(term.tf)+" "+Double.toString(term.df)+" "+Double.toString(term.tfidflog10));
			    	}
			    	
			    }
			    
			    System.out.println("Adding document "+docPath+" to new index");
			    outputWriter.addDocument(doc);
			}
			outputWriter.commit();
			outputWriter.close();
		} catch (IOException e) {
			System.out.println("An error occurred: "+e);
			e.printStackTrace();
		}
		
		


	}

}
