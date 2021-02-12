package be.vanoosten.esa;

import static be.vanoosten.esa.WikiIndexer.TITLE_FIELD;

import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import be.vanoosten.esa.tools.SemanticSimilarityTool;
import be.vanoosten.esa.tools.Vectorizer;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.json.JSONException;
import org.json.JSONObject;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

import javax.xml.parsers.ParserConfigurationException;

import static org.apache.lucene.util.Version.LUCENE_48;

/**
 *
 * @author Philip van Oosten
 * @author Dimitrios Tsesmelis
 * @author Andrea Armani
 */

public class Main {

    public static void main(String[] args) throws IOException, ParseException, SAXNotSupportedException, SAXNotRecognizedException, ParserConfigurationException, CsvException {

        int method = 0;
        boolean createIndex = false;
        String indexPath = "";
        String candidateSetPath = "";
        String rankingPath = "";
        String dumpFilePath = "";

        //Read input arguments
        if(args.length != 11) {
            System.out.println("Please provide 8 arguments in the following format: --indexPath ... --candidateSetPath ... --rankingPath ... --method [1 for titles, 2 for abstracts]) --createIndex [0 or 1] and [dumpPath]");
            return;
        }
        for (int count = 0; count < args.length; count++) {
            switch (args[count]) {
                case "--indexPath":
                    indexPath = args[count + 1];
                    count++;
                    System.out.println(indexPath);
                    break;
                case "--candidateSetPath":
                    candidateSetPath = args[count+1];
                    count++;
                    break;
                case "--rankingPath":
                    rankingPath = args[count+1];
                    count++;
                    break;
                case "--method":
                    method = Integer.parseInt(args[count+1]);
                    count++;
                    break;
                case "--createIndex":
                    if(Integer.parseInt(args[count+1]) == 1)
                        createIndex = true;
                    else
                        createIndex = false;
                    count++;
                    dumpFilePath = args[count+1];
                    count++;
                    break;
            }
        }

        //String indexPath = String.join(File.separator, "C:\\Users\\zas11\\Desktop\\Jim Profile\\Studies\\BDMA\\Courses\\Big Data Research Project\\project\\data\\wikipedia\\index2");
        //File wikipediaDumpFile = new File(String.join(File.separator, "C:", "Downloads", "metawiki-latest-pages-articles-multistream.xml.bz2"));
        File wikipediaDumpFile = new File(dumpFilePath);

        String startTokens = "geheim anoniem auteur verhalen lezen schrijven wetenschappelijk artikel peer review";

        WikiFactory factory = new EnwikiFactory();
        CharArraySet stopWords = factory.getStopWords();

//        File termDocIndexDirectory = new File(String.join(File.separator, "C:\\Users\\zas11\\Desktop\\Jim Profile\\Studies\\BDMA\\Courses\\Big Data Research Project\\project\\data\\wikipedia\\index2"));
//        File conceptTermIndexDirectory = new File(String.join(File.separator, "C:\\Users\\zas11\\Desktop\\Jim Profile\\Studies\\BDMA\\Courses\\Big Data Research Project\\project\\data\\wikipedia"));
        File termDocIndexDirectory = new File(String.join(File.separator, indexPath+"/termdoc"));
        File conceptTermIndexDirectory = new File(String.join(File.separator, indexPath+"/conceptterm"));

        // If this flag is true, the Inverted Index is created. This process takes a long time!!!
        if(createIndex) {
            System.out.println("creating index");
            indexing(termDocIndexDirectory, wikipediaDumpFile, stopWords);
            createConceptTermIndex(termDocIndexDirectory, conceptTermIndexDirectory);
        }
        File indexDir = new File(String.join(File.separator, indexPath));

        // Initialize the semantic interpreter
        Vectorizer vectorizer = new Vectorizer(indexDir, new WikiAnalyzer(LUCENE_48,stopWords));
        SemanticSimilarityTool sst = new SemanticSimilarityTool(vectorizer);

        List<List<String>> result_rows = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(candidateSetPath))) {
            List<String[]> lines = reader.readAll();

            //for each line, replace ',' with ''
            for(int line_num = 0; line_num < lines.size(); line_num++)
                lines.set(line_num, new String[] {lines.get(line_num)[0].replace(",","")});

            String titleSearch = lines.get(0)[0].split("\t")[1];

            // Use titles
            if(method == 1) {
                for (int i = 0; i < lines.size(); i++) {
                    String[] tokens = lines.get(i)[0].split("\t");
                    for (int j = 0; j < tokens.length; j++)
//                        tokens[j] = tokens[j].replace("_", " ").replace("/", " ");
                        tokens[j] = tokens[j].replace("/", " ");

                    // parse only english records
                    if (!tokens[4].equals("en"))
                        break;
                    String title_src = tokens[1];
                    String title_dst = tokens[3];

                    //compute similarity score
                    float score;
                    if (title_dst.equals("Andes orthohantavirus"))
                        score = sst.findSemanticSimilarity(title_src, title_dst, Boolean.TRUE);
                    else
                        score = sst.findSemanticSimilarity(title_src, title_dst, Boolean.FALSE);
                    System.out.println(title_src + " " + title_dst);
                    System.out.println(score);
                    result_rows.add(Arrays.asList(title_src, title_dst, String.valueOf(score)));
                }
            }
            else {
                HashSet<String> articlesToSearch = new HashSet<String>();
                articlesToSearch.add(titleSearch);
                for (int i = 0; i < lines.size(); i++) {
                    String[] tokens = lines.get(i)[0].split("\t");
//                    for (int j = 0; j < tokens.length; j++)
//                        tokens[j] = tokens[j].replace("_", " ").replace("/", " ");

                    // parse only english records
                    if (!tokens[4].equals("en"))
                        break;
                    String title_dst = tokens[3];
                    articlesToSearch.add(title_dst);
                }
                HashMap<String, String> map = extractAbstractAPI(articlesToSearch);
                for ( String title : map.keySet() ){
                    System.out.println(title);
                    System.out.println(map.get(title));
                }

                // For each candidate article, compute the similarity score
                for ( String title_dst : map.keySet() ) {
                    float score = 0;
                    if(!title_dst.equals(titleSearch)) {
                        if(!map.get(title_dst).equals("")) {
//System.out.println(titleSearch);
                            score = sst.findSemanticSimilarity(map.get(titleSearch)
                                            .replace("_", " ")
                                            .replace("/", " ")
                                            .replace("{", "")
                                            .replace("}", "")
                                            .replace("[", "")
                                            .replace("]", "")
                                            .replace(":", " ")
                                            .replace("(", "")
                                            .replace(")", "")
                                            .replace(")", "")
                                    , map.get(title_dst)
                                            .replace("_", " ")
                                            .replace("/", " ")
                                            .replace("{", "")
                                            .replace("}", "")
                                            .replace("[", "")
                                            .replace("]", "")
                                            .replace(":", " ")
                                            .replace("(", "")
                                            .replace(")", "")
                                            .replace("'", "")
                                    , Boolean.FALSE);
                        }
                        System.out.println(titleSearch + " " + title_dst);
                        System.out.println(score);
                        result_rows.add(Arrays.asList(titleSearch, title_dst, String.valueOf(score)));
                    }
                }
            }
        }

        // Output the ranking to a csv file
        FileWriter csvWriter = new FileWriter(rankingPath);
        csvWriter.append("Source_title");
        csvWriter.append(",");
        csvWriter.append("Destination_title");
        csvWriter.append(",");
        csvWriter.append("Similarity_score");
        csvWriter.append("\n");

        for (List<String> rowData : result_rows) {
            csvWriter.append(String.join(",", rowData));
            csvWriter.append("\n");
        }

        csvWriter.flush();
        csvWriter.close();

    }

    /**
     * Creates a concept-term index from a term-to-concept index (a full text index of a Wikipedia dump).
     * @param termDocIndexDirectory The directory that contains the term-to-concept index, which is created by {@code indexing()} or in a similar fashion.
     * @param conceptTermIndexDirectory The directory that shall contain the concept-term index.
     * @throws IOException
     */
    static void createConceptTermIndex(File termDocIndexDirectory, File conceptTermIndexDirectory) throws IOException {
        ExecutorService es = Executors.newFixedThreadPool(2);

        final Directory termDocDirectory = FSDirectory.open(termDocIndexDirectory);
        final IndexReader termDocReader = IndexReader.open(termDocDirectory);
        final IndexSearcher docSearcher = new IndexSearcher(termDocReader);

        Fields fields = MultiFields.getFields(termDocReader);
        if (fields != null) {
            Terms terms = fields.terms(WikiIndexer.TEXT_FIELD);
            TermsEnum termsEnum = terms.iterator(null);

            final IndexWriterConfig conceptIndexWriterConfig = new IndexWriterConfig(LUCENE_48, null);
            try (IndexWriter conceptIndexWriter = new IndexWriter(FSDirectory.open(conceptTermIndexDirectory), conceptIndexWriterConfig)) {
                int t = 0;
                BytesRef bytesRef;
                while ((bytesRef = termsEnum.next()) != null) {
                    String termString = bytesRef.utf8ToString();
                    if (termString.matches("^[a-zA-Z]+:/.*$") || termString.matches("^\\d+$")) {
                        continue;
                    }
                    if (termString.charAt(0) >= '0' && termString.charAt(0) <= '9') {
                        continue;
                    }
                    if (termString.contains(".") || termString.contains("_")) {
                        continue;
                    }
                    if (t++ == 1000) {
                        t = 0;
                        System.out.println(termString);
                    }
                    TopDocs td = SearchTerm(bytesRef, docSearcher);

                    // add the concepts to the token stream
                    byte[] payloadBytes = new byte[5];
                    ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(payloadBytes);
                    CachingTokenStream pcTokenStream = new CachingTokenStream();
                    double norm = ConceptSimilarity.SIMILARITY_FACTOR;
                    int last = 0;
                    for(ScoreDoc scoreDoc : td.scoreDocs){
                        if(scoreDoc.score/norm < ConceptSimilarity.SIMILARITY_FACTOR ||
                                last>= 1.0f / ConceptSimilarity.SIMILARITY_FACTOR) break;
                        norm += scoreDoc.score * scoreDoc.score;
                        last++;
                    }
                    for (int i=0; i<last; i++) {
                        ScoreDoc scoreDoc = td.scoreDocs[i];
                        Document termDocDocument = termDocReader.document(scoreDoc.doc);
                        String concept = termDocDocument.get(WikiIndexer.TITLE_FIELD);
                        Token conceptToken = new Token(concept, i * 10, (i + 1) * 10, "CONCEPT");
                        // set similarity score as payload
                        int integerScore = (int) ((scoreDoc.score/norm)/ConceptSimilarity.SIMILARITY_FACTOR);
                        dataOutput.reset(payloadBytes);
                        dataOutput.writeVInt(integerScore);
                        BytesRef payloadBytesRef = new BytesRef(payloadBytes, 0, dataOutput.getPosition());
                        conceptToken.setPayload(payloadBytesRef);
                        pcTokenStream.produceToken(conceptToken);
                    }

                    Document conceptTermDocument = new Document();
                    AttributeSource attributeSource = termsEnum.attributes();
                    conceptTermDocument.add(new StringField(WikiIndexer.TEXT_FIELD, termString, Field.Store.YES));
                    conceptTermDocument.add(new TextField("concept", pcTokenStream));
                    conceptIndexWriter.addDocument(conceptTermDocument);
                }
            }
        }
    }

    private static TopDocs SearchTerm(BytesRef bytesRef, IndexSearcher docSearcher) throws IOException {
        Term term = new Term(WikiIndexer.TEXT_FIELD, bytesRef);
        Query query = new TermQuery(term);
        int n = 1000;
        TopDocs td = docSearcher.search(query, n);
        if (n < td.totalHits) {
            n = td.totalHits;
            td = docSearcher.search(query, n);
        }
        return td;
    }

    private static void searchForQuery(final QueryParser parser, final IndexSearcher searcher, final String queryString, final IndexReader indexReader) throws ParseException, IOException {
        Query query = parser.parse(queryString);
        TopDocs topDocs = searcher.search(query, 12);
        System.out.println(String.format("%d hits voor \"%s\"", topDocs.totalHits, queryString));
        for (ScoreDoc sd : topDocs.scoreDocs) {
            System.out.println(String.format("doc %d score %.2f shardIndex %d title \"%s\"", sd.doc, sd.score, sd.shardIndex, indexReader.document(sd.doc).get(TITLE_FIELD)));
        }
    }

    /**
     * Creates a term to concept index from a Wikipedia article dump.
     * @param termDocIndexDirectory The directory where the term to concept index must be created
     * @param wikipediaDumpFile The Wikipedia dump file that must be read to create the index
     * @param stopWords The words that are not used in the semantic analysis
     * @throws IOException
     */
    public static void indexing(File termDocIndexDirectory, File wikipediaDumpFile, CharArraySet stopWords) throws IOException, SAXNotSupportedException, SAXNotRecognizedException, ParserConfigurationException {
        try (Directory directory = FSDirectory.open(termDocIndexDirectory)) {
            Analyzer analyzer = new WikiAnalyzer(LUCENE_48, stopWords);
            try(WikiIndexer indexer = new WikiIndexer(analyzer, directory)){
                indexer.parseXmlDump(wikipediaDumpFile);
            }
        }
    }

    public static HashMap<String, String> extractAbstractAPI(HashSet<String> articlesToSearch) throws IOException {

        System.out.println("Starting querying Wikipedia's API to get articles' abstract");
        String articleTitle = "";
        HashMap<String, String> map = new HashMap<String, String>();
        int counter = 0;

        //For each candidate article, extract its abstract using Wikipedia's API
        Iterator<String> it = articlesToSearch.iterator();
        while(it.hasNext()){
            counter++;
            if(counter % 50 == 0)
                System.out.println("Querying "+String.valueOf(counter)+"/"+String.valueOf(articlesToSearch.size()));

            articleTitle = it.next();
            URL url = new URL("https://en.wikipedia.org/w/api.php?action=query&format=json&prop=extracts&titles="+ URLEncoder.encode(articleTitle, StandardCharsets.UTF_8.toString())+"&redirects=1&exintro=1&explaintext=1");

            try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openConnection().getInputStream()))) {
                String line = null;
                String jasonString = br.lines().collect(Collectors.joining());

                try {
                    JSONObject jsonObject = new JSONObject(jasonString);
                    String query = jsonObject.getString("query");
                    jsonObject = new JSONObject(query);
                    String pages = jsonObject.getString("pages");
                    jsonObject = new JSONObject(pages);

                    Iterator itr = jsonObject.keys();
                    if(itr.hasNext()) {
                        Object page_number = itr.next();
                        String page = jsonObject.getString(page_number.toString());
                        jsonObject = new JSONObject(page);
                        String articleSummary = jsonObject.getString("extract");

//                        //Keep the first WORDS_TO_KEEP words of the abstract
//                        String[] words = articleSummary.split(" ");
//                        articleSummary = "";
//			int WORDS_TO_KEEP = 200;
//                        int word_count = WORDS_TO_KEEP;
//                        if(words.length < WORDS_TO_KEEP)
//                            word_count = words.length;
//                        for (int i = 0; i < word_count; i++) { articleSummary += " " + words[i];}
                        map.put(articleTitle, articleSummary);
                    }


                }catch (JSONException err){
                    System.out.println("String to JSONObject failed for:"+jasonString);
                }


            }
        }
        return map;
    }

    public static void extractAbstractXML(File wikipediaDumpFile, String targetTitle, String dirToWrite, CharArraySet stopWords, HashSet<String> articlesToSearch) throws IOException, SAXNotSupportedException, SAXNotRecognizedException, ParserConfigurationException {
        //try (Directory directory = FSDirectory.open(termDocIndexDirectory)) {
        try(SimilarityUsingAbstract parser = new SimilarityUsingAbstract(targetTitle, dirToWrite, articlesToSearch)){
            parser.parseXmlDump(wikipediaDumpFile);
        }
        //}
    }

    public static void storeAbstract(String outputPath, HashMap<String, String> abstractMap) throws IOException {
        List<List<String>> rows = new ArrayList<>();

        for ( String title : abstractMap.keySet() )
            rows.add(Arrays.asList(title, abstractMap.get(title)));

        FileWriter csvWriter = new FileWriter(outputPath);
        csvWriter.append("Article_title");
        csvWriter.append("|");
        csvWriter.append("Abstract");
        csvWriter.append("\n;");

        for (List<String> rowData : rows) {
            csvWriter.append(String.join("|", rowData));
            csvWriter.append("\n;");
        }

        csvWriter.flush();
        csvWriter.close();
    }
}
