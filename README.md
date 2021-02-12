# Wikipedia-Internal-Missing-Links-Detection

### Execution Instructions:
After compiling the program, the following command can be used to run it: **java -cp target/esa-1.0-SNAPSHOT.jar be.vanoosten.esa.Main --method [1/2] --createIndex [0/1] [path to pages-articles-multistream.xml.bz2] --candidateSetPath [path to candidate set csv] --rankingPath [output path] --indexPath [path to the index]** <br/>
where the list of parameters are the following:
<ul>
<li>--method: 1 to use titles and 2 to use abstracts</li>
<li>--createIndex: if the index has not been created 1 followed by the path to the xml file, else 0</li>
<li>--indexPath: the path to the index</li>
<li>--candidateSetPath: the path to the input csv containing the candidate set</li>
<li>--rankingPath: the path to output the ranking</li>
</ul>
