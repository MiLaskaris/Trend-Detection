import java.util.*;
import java.util.regex.*;
import java.io.*;

public class MapReduce implements Runnable {
	public static final int NUM_REDUCE_WORKERS = Runtime.getRuntime().availableProcessors();
	public static final int NUM_MAP_WORKERS = Runtime.getRuntime().availableProcessors();

	/*
	 * Every position in the array contains the information for every mapper as a string
	 */
	public static String[] contents;

	/*
	 * A hash table from MapResult objects and the event id
	 */
	public static Map<String, MapResult> map_results;

	public static final int FUNCTION_MAP = 0;

	public static final int FUNCTION_REDUCE = 1;

	protected int function;

	protected String workerjob;

	/* Contains the results of every mapper */
	protected Map<String, MapResult> results;

	/* Contains the results of a specific mapper */
	public MapResult current_result;

	public MapReduce() {
	}

	/*
	 * Create a worker thread to process each input file and run the map
	 * function over the data
	 */
	public void MapPhase() {

		/*
		 * A synchronised hashtable for the map and reduce results
		 */

		map_results = Collections.synchronizedMap(new HashMap<String, MapResult>());

		Thread[] worker_threads = new Thread[NUM_MAP_WORKERS];

		for (int i = 0; i < NUM_MAP_WORKERS; i++) {
			MapReduce worker = new MapReduce(FUNCTION_MAP, contents[i]);
			worker_threads[i] = new Thread(worker);
			worker_threads[i].start();
		}

		/* Waiting for the all the threads to close */
		try {
			for (int i = 0; i < NUM_MAP_WORKERS; i++) {
				worker_threads[i].join();
			}
		} catch (InterruptedException e) {
		}

	}

	public MapReduce(int _function, String myjob) {
		function = _function;
		workerjob = myjob;
	}

	public void run() {

		switch (function) {
		case FUNCTION_MAP:

			map(workerjob);

			break;

		case FUNCTION_REDUCE:

			Set<Map.Entry<String, MapResult>> results_set = results.entrySet();
			Iterator<Map.Entry<String, MapResult>> results_iterator = results_set.iterator();

			while (results_iterator.hasNext()) {
				Map.Entry<String, MapResult> result_entry = results_iterator.next();
				current_result = result_entry.getValue();
				reduce(current_result.getEventid(), current_result.iterator());
			}

			break;
		}
	}

	
	public void map(String value) {
		// Split value into words
		String[] words = value.split(",");

		// We collect the words of interest
		for (int i = 0; i < words.length; i += 7) {
			// We collect only the 'yes' responses
			if (words[i + 2].equals("yes"))
				EmitIntermediate(words[i + 1], words[i + 3], words[i + 5], words[i + 6], "1");
		}
	}

	/*
	 * Method that creates an object MapResult (if it doesn't already exists) with eventid as key value 
	 * and it is added in map_results. If the object already exists then it adds the values of that object
	 */
	public void EmitIntermediate(String eventid, String catid, String lat, String lon, String value) {
		MapResult bucket;

		synchronized (map_results) {
			bucket = map_results.get(eventid);
			if (bucket == null) {
				bucket = new MapResult(eventid, catid, lat, lon);
				map_results.put(eventid, bucket);
			}
		}

		bucket.addValue(value);
	}

	/*
	 * Method that creates a set of worker threads for the reduce function of the results from every mapper
	 */
	public void ReducePhase() {
		int i;

		/*
		 * We divide the map_results hashtable to a number equal with the number of workers that will make the reduce
		 */
		int num_reduce_workers = NUM_REDUCE_WORKERS;
		// Catch the too many workers case
		if (num_reduce_workers > map_results.size())
			num_reduce_workers = map_results.size();

		int chunk_size = map_results.size() / num_reduce_workers;
		int records_remaining = map_results.size();

		Iterator<MapResult> iterator = map_results.values().iterator();

		List<Map<String, MapResult>> reduce_chunks = new ArrayList<Map<String, MapResult>>(num_reduce_workers);

		while (records_remaining > 0) {

			if (records_remaining < chunk_size) {
				chunk_size = records_remaining;
			}

			Map<String, MapResult> worker_map = new HashMap<String, MapResult>(chunk_size);
			reduce_chunks.add(worker_map);

			for (int record = 0; record < chunk_size; record++) {
				MapResult m = iterator.next();
				worker_map.put(m.getEventid(), m);
			}
			records_remaining -= chunk_size;
		}

		Thread[] worker_threads = new Thread[num_reduce_workers];

		for (i = 0; i < num_reduce_workers; i++) {
			MapReduce worker = new MapReduce(FUNCTION_REDUCE, reduce_chunks.get(i));
			worker_threads[i] = new Thread(worker);
			worker_threads[i].start();
		}

		try {
			for (i = 0; i < num_reduce_workers; i++) {
				worker_threads[i].join();
			}
		} catch (InterruptedException e) {
		}

	}

	public MapReduce(int _function, Map<String, MapResult> _results) {
		function = _function;
		results = _results;
	}

	public void Emit(String result) {
		current_result.setResult(result);
	}

	public void reduce(String key, Iterator<String> values) {
		int result = 0;
		while (values.hasNext()) {
			result += 1;
		}

		Emit(result + "");
	}

	public void FillContentsFromFile(String file) throws IOException {
		int linesnum = countLines(file), i, j;
		// The lines in the file that goes to the mappers
		int count = linesnum / NUM_MAP_WORKERS;

		StringBuffer conts = new StringBuffer(2000);
		BufferedReader reader = null;
		contents = new String[NUM_MAP_WORKERS];
		try {
			String line = "";
			reader = new BufferedReader(new FileReader(file));
			// fill everything except last one
			for (i = 0; i < NUM_MAP_WORKERS - 1; i++) {
				for (j = 0; j < count; j++) {
					line = reader.readLine();
					conts.append(line);
					conts.append(",");
				}
				
				contents[i] = conts.toString();
				conts = new StringBuffer(2000);
			}

			line = reader.readLine();
			while (line != null) {
				conts.append(line);
				conts.append(",");

				// Get the next line
				line = reader.readLine();
			}
			contents[i] = conts.toString();
		} catch (IOException ex) {
			ex.printStackTrace();
			conts = null;
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException ignore) {
				}
			}
		}
	}

	public static int countLines(String filename) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(filename));
		try {
			byte[] c = new byte[1024];
			int count = 0;
			int readChars = 0;
			boolean empty = true;
			while ((readChars = is.read(c)) != -1) {
				empty = false;
				for (int i = 0; i < readChars; ++i) {
					if (c[i] == '\n') {
						++count;
					}
				}
			}
			return (count == 0 && !empty) ? 1 : count;
		} finally {
			is.close();
		}
	}

	public ArrayList<MapResult> go(String args) throws IOException {

		FillContentsFromFile(args);

		MapPhase();

		ReducePhase();

		ArrayList<MapResult> items = new ArrayList<MapResult>(map_results.values());
		return items;

	}

	public void PrintResults() {
		System.out.println("Results");
		System.out.println("-------");

		ArrayList<MapResult> items = new ArrayList<MapResult>(map_results.values());

		for (MapResult result : items) {
			System.out.println(result.getEventid() + ": " + result.getResult());
		}
	}
}
