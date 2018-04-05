import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class Demo {
	public static final int CATEGORIES = 22;
	public static final int DAYS = 105;
	//contains the information from the files that we read(files represent days)
	public static DayInfo[] daysinfos = new DayInfo[DAYS];

	public static void FillDaysInfowithMapReduce() throws IOException {
		System.out.println("MapReduce Starting...");
		System.out.println("---------------------");

		long start_time, total_time;
		start_time = System.currentTimeMillis();
		for (int i = 0; i < DAYS; i++) {
			// Category frequency for the events
			int[] categoryfreq = new int[CATEGORIES];
			ArrayList<MapResult> items;
			MapReduce me = new MapReduce();
			String file = ".\\newfiles\\file" + i;
			// String file = ".\\newfiles\\totalfusingfile";
			items = me.go(file);
			/*
			 * The values in the array categoryfreq are the frequencies that 
			 * we calculate.Also for every category id we match that id with the 
			 * position in the array.
			 */
			for (MapResult result : items) {
				categoryfreq[Integer.parseInt(result.getCatid())] += Integer.parseInt(result.getResult());
			}
	
			daysinfos[i] = new DayInfo(items, categoryfreq);
		}

		total_time = System.currentTimeMillis() - start_time;
		System.out.println("...MapReduce just finished");
		System.out.println("--------------------------");
		System.out.println("MapReduce processing time = " + (total_time / 1000.0f) + "s");
		System.out.println();
	}

	public static void FillDaysInfowithSingleThread() throws IOException {
		System.out.println("SingleThread Starting...");
		System.out.println("---------------------");

		long start_time, total_time;
		start_time = System.currentTimeMillis();
		for (int i = 0; i < DAYS; i++) {
			// The array of category frequencies 
			int[] categoryfreq = new int[CATEGORIES];
			ArrayList<MapResult> items;
			SingleThread st = new SingleThread();
			String file = ".\\newfiles\\file" + i;
			// String file = ".\\newfiles\\totalfusingfile";
			items = st.go(file);
			for (MapResult result : items) {
				categoryfreq[Integer.parseInt(result.getCatid())] += result.getIntresult();
			}
			daysinfos[i] = new DayInfo(items, categoryfreq);
		}

		total_time = System.currentTimeMillis() - start_time;
		System.out.println("...SingleThread just finished");
		System.out.println("--------------------------");
		System.out.println("SingleThread processing time = " + (total_time / 1000.0f) + "s");
		System.out.println();
	}

	public static void initAverage(double[] average) {
		for (int i = 0; i < average.length; i++)
			average[i] = 0.;
	}

	public static void writeToFile(FileWriter fw, double[] average, int day) throws IOException {
		int i;
		fw.write("DAY " + day + ":" + System.lineSeparator());
		for (i = 1; i < average.length; i++) {
			fw.write("Average of Category " + i + ": " + String.format("%.2f", average[i]) + "\tDAY " + (day + 1) + ": "
					+ daysinfos[day].Catfreqs[i] + System.lineSeparator());
		}
	}

	public static void CalculateAverages(int from, int to, int shamt, double[] average) {
		int i, j;
		initAverage(average);
		for (i = from; i <= to; i++) {
			for (j = 1; j < CATEGORIES; j++) {
				average[j] += daysinfos[i].Catfreqs[j];
			}
		}
		for (j = 1; j < CATEGORIES; j++) {
			average[j] /= shamt;
		}
		// Saving the moving average of every day
		daysinfos[to].setMovAver(average);
	}

	public static void MovingAverage(int fromday, int today, int shamt) {

		int i;
		double[] average = new double[CATEGORIES];
		// double [] sum = new double[CATEGORIES];
		String filename = ".\\MovingAveragefile.txt";
		FileWriter fw;
		try {
			fw = new FileWriter(filename);
			for (i = 0; i < today; i++) {
				fw.write(daysinfos[i].toStrinCategoriesfreqs(i + 1));
			}
			fw.write(System.lineSeparator() + "------Moving Averages--------" + System.lineSeparator());
			CalculateAverages(fromday, fromday + shamt - 1, shamt, average);
			writeToFile(fw, average, fromday + shamt);
			for (i = fromday + 1; i <= today - shamt + 1; i++) {
				CalculateAverages(i, i + shamt - 1, shamt, average);
				writeToFile(fw, average, i + shamt);
			}

			fw.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void FileMovingAverPrediction() throws IOException {
		String filename = ".\\MovingAveragePredictions.txt";
		FileWriter fw;
		try {
			fw = new FileWriter(filename);
			fw.write("------ Moving Averages Predictions --------" + System.lineSeparator());
			fw.write(
					"Previous Day Freq / Current Day Freq------------ Moving Average Previous Day / Moving Average Current Day"
							+ System.lineSeparator());
			for (int i = 3; i < 100; i++) {
				fw.write("-------- DAY " + (i + 1) + " -----------" + System.lineSeparator());
				for (int j = 0; j < 25; j++) {
					fw.write(String.format("%.2f", ((double) daysinfos[i - 1].Catfreqs[j]) / daysinfos[i].Catfreqs[j])
							+ "\t" + String.format("%.2f", daysinfos[i - 1].movaver[j] / daysinfos[i].movaver[j])
							+ System.lineSeparator());
				}
			}
			fw.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	

	/* This method calculates and saves into a file the predictions from the day
	* uptotrainingsetday+1 up to 100.
	*/
	public static void LinearRegressionPrediction(FileWriter fw, int uptotrainingsetday) {
		int i, j;
		SimpleRegression[] regressions = new SimpleRegression[CATEGORIES];
		for (i = 1; i < CATEGORIES; i++)
			regressions[i] = new SimpleRegression();

		/* Adding the data of the training set for every category in every
		* SimpleRegression Object
		*/
		for (i = 1; i < CATEGORIES; i++) {
			for (j = 0; j < uptotrainingsetday; j++) {
				regressions[i].addData(j + 1, daysinfos[j].Catfreqs[i]);
			}
		}
		try {
			
			for (i = 1; i < CATEGORIES; i++) {
				fw.write(System.lineSeparator() + "CATEGORY : " + i + "\t");
				
				fw.write("\tReal Frequency : " + daysinfos[uptotrainingsetday].Catfreqs[i] + "\tPrediction : "
						+ String.format("%.0f ", regressions[i].predict(uptotrainingsetday)) + System.lineSeparator());

			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void CalculateLinearRegressionPredictions() {
		int i;
		String filename = ".\\LinearRegressionPredictions.txt";
		FileWriter fw;
		try {
			fw = new FileWriter(filename);
			fw.write(" ");
			for (i = 9; i < 100; i++) {
				fw.write(System.lineSeparator() + "-----------------------DAY " + (i + 1) + "-----------------------"
						+ System.lineSeparator());
				LinearRegressionPrediction(fw, i);
			}
			fw.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	public static void ShowTopXEventsForEveryDay(int x) {
		int i;
		String filename = ".\\Top" + x + "Eventsforeveryday" + ".txt";
		FileWriter fw;
		try {
			fw = new FileWriter(filename);
			for (i = 0; i < daysinfos.length; i++) {
				fw.write("------TOP " + x + " of " + "DAY " + (i + 1) + " EVENTS--------" + System.lineSeparator());
				fw.write(daysinfos[i].getTopXEvents(x));

			}
			fw.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	// Method for the Top X Events from every category
	public static void ShowTopXEventsForAllDays(int x) {
		int i;
		Map<String, MapResult> AllEvents = new HashMap<String, MapResult>();
		ArrayList<MapResult> items;
		for (i = 0; i < daysinfos.length; i++) {
			for (MapResult t : daysinfos[i].items) {
				MapResult temp = (MapResult) AllEvents.get(t.getEventid());
				if (temp == null) {
					AllEvents.put(t.getEventid(), t);
				} else {
					temp.increaseintresult(t.getIntresult());
				}
			}
		}
		items = new ArrayList<MapResult>(AllEvents.values());
		// Sorting
		Collections.sort(items, new Comparator<MapResult>() {
			@Override
			public int compare(MapResult event1, MapResult event2) {
				Integer r1 = event1.getIntresult();
				Integer r2 = event2.getIntresult();
				return r1.compareTo(r2);
			}
		});

		System.out.println("------TOP " + x + " of " + items.size() + " EVENTS--------");
		for (i = items.size() - 1; i >= items.size() - x; i--)
			System.out.println(items.get(i).getEventid() + " " + items.get(i).getIntresult());
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		// FillDaysInfowithSingleThread();
		 FillDaysInfowithMapReduce();

		MovingAverage(0, 100, 3);
		FileMovingAverPrediction();
		CalculateLinearRegressionPredictions();
		ShowTopXEventsForEveryDay(5);

	}

}
