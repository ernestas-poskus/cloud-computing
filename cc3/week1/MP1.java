import java.io.File;
import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

public class MP1 {
	private class CountWordTuple implements Comparable<CountWordTuple> {
		public CountWordTuple(Integer count, String word) {
	    this.count = count;
	    this.word = new String(word);
	}

	public Integer getCount() { return count; }
	public String getWord() { return word; }

	public int compareTo(CountWordTuple other) {
	    if (this.getCount().equals(other.getCount())) {
		return this.getWord().compareTo(other.getWord());
	    }
	    else {
		// Descending on count
		return other.getCount().compareTo(this.getCount());
	    }
	}

	Integer count;
	String word;
    };

    Random generator;
    String userName;
    String inputFileName;
    String delimiters = " \t,;.?!-:@[](){}_*/";
    String[] stopWordsArray = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
            "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};

    void initialRandomGenerator(String seed) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA");
        messageDigest.update(seed.toLowerCase().trim().getBytes());
        byte[] seedMD5 = messageDigest.digest();

        long longSeed = 0;
        for (int i = 0; i < seedMD5.length; i++) {
            longSeed += ((long) seedMD5[i] & 0xffL) << (8 * i);
        }

        this.generator = new Random(longSeed);
    }

    Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = 50000;
        Integer[] ret = new Integer[n];
        this.initialRandomGenerator(this.userName);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    public MP1(String userName, String inputFileName) {
        this.userName = userName;
        this.inputFileName = inputFileName;
    }

    public String[] process() throws Exception {
        String[] ret = new String[20];

        //TODO
        List<String> stopWordsList = Arrays.asList(stopWordsArray);
        File file = new File(this.inputFileName);
        BufferedReader reader = null;

        int lines = 0, words = 0, unique = 0, skipped = 0;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text = null;
            Map<String, Integer> wordCount = new HashMap<String, Integer>();
            List<String> fileLines = new ArrayList<String>();
            while((text = reader.readLine()) != null) {
                ++lines;
                fileLines.add(text);
            }

            Integer[] indexes = getIndexes();
            for (int ii=0; ii<indexes.length; ++ii) {
                text = fileLines.get(indexes[ii]);
                StringTokenizer st = new StringTokenizer(text, delimiters);
                while(st.hasMoreTokens()) {
                    String word = st.nextToken().trim().toLowerCase();
                    ++words;

                    if (!stopWordsList.contains(word)) {
                        Integer count = wordCount.get(word);
                        if (count == null) {
                            count = new Integer(0);
                            ++unique;
                        }
                        ++count;
                        wordCount.put(word, count);
                    }
                    else {
                        ++skipped;
                    }
                }
            }
            List<CountWordTuple> countWordList = new ArrayList<CountWordTuple>();
            for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
                String word = entry.getKey();
                Integer count = entry.getValue();
                CountWordTuple cwt = new CountWordTuple(count, word);
                countWordList.add(cwt);
            }

            Collections.sort(countWordList);
            Iterator<CountWordTuple> it = countWordList.iterator();
            int retIdx = 0;
            while(it.hasNext()) {
                CountWordTuple cwt = (CountWordTuple) it.next();
                ret[retIdx] = cwt.getWord();
                if (++retIdx == 20) {
                    break;
                }
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1){
            System.out.println("MP1 <User ID>");
        }
        else {
            String userName = args[0];
            String inputFileName = "./input.txt";
            MP1 mp = new MP1(userName, inputFileName);
            String[] topItems = mp.process();
            for (String item: topItems){
                System.out.println(item);
            }
        }
    }
}
