import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/*This class represents a time unit and it contains the information that we get from every file*/
public class DayInfo {

	public int [] Catfreqs;
	public double [] movaver;
	ArrayList<MapResult> items;
	
	DayInfo(ArrayList<MapResult> items, int [] Catfreqs){
		this.items = items;
		this.Catfreqs = Catfreqs;
		movaver = new double[23];
	}
	
	
	public void printCategoriesfreqs(){
	     System.out.println("---Frequencies of Categories---");
	     int sum = 0;
	     for(int i=1;i<=22;i++){
	    	 System.out.println("Category " + i + " : " + Catfreqs[i]);
	    	 sum += Catfreqs[i];
	     }
	     System.out.println("frequencies sum = " + sum);
	     System.out.println("items size = " + items.size());
	}
	
	/*Method which converts the frequencies to string from a specific file.*/
	public String toStrinCategoriesfreqs(int day){
		String str = new String();
		str += "---Frequencies of Categories Day " + day + "---" + System.lineSeparator();
		
	     int sum = 0;
	     for(int i=1;i<=22;i++){
	    	 str += "Category " + i + " : " + Catfreqs[i] + System.lineSeparator();
	    	 sum += Catfreqs[i];
	     }
	     str += "frequencies sum = " + sum + System.lineSeparator();
	     return str;
	}
	/*Method which properly fills the moving average */
	public void setMovAver(double [] aver){
		for(int i=0;i<aver.length;i++)
			movaver[i] = aver[i];
	}
	/*Method for the top X Events */
	public String getTopXEvents(int x){
		int i;
		String str = "";
		
		Collections.sort(items, new Comparator<MapResult>() {
	        @Override
	        public int compare(MapResult event1, MapResult event2)
	        {
	        	Integer r1 = event1.getIntresult();
	        	Integer r2 = event2.getIntresult();
	        	return r1.compareTo(r2);
	        }
	    });			
		
		for(i=items.size()-1;i >= items.size()-x;i--)
			str += items.get(i).getEventid() + " " + items.get(i).getIntresult() + System.lineSeparator();
			
		
		return str;
	}
}
