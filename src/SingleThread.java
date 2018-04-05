import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SingleThread {
	
	public String contents;
	public Map <String, MapResult> results;
	
	public SingleThread(){
		results = new HashMap<String, MapResult> ();
	}
	
	// Filling the Contents properly from the file
	public void FillContentsFromFile (String file) throws IOException {
		StringBuffer conts = new StringBuffer(2000);
		BufferedReader reader = null;
		
		try{
			String line = "";
			reader = new BufferedReader (new FileReader (file));
			line = reader.readLine ();
			while (line != null){
				conts.append (line);
				conts.append (",");
				line = reader.readLine ();
			}
			contents = conts.toString();
		}
		catch(IOException ex)
		{
			ex.printStackTrace ();
			conts = null;
		}
		finally
		{
	         if (reader != null)
	         {
	            try
	            {
	               reader.close ();
	            }
	            catch (IOException ignore)
	            {
	            }
	         }
		}
	}
	
	public void FillResults(){
		MapResult bucket;
		String [] words = contents.split (",");
		
		for(int i=0;i<words.length;i+=7){
			// We collect only the 'yes' responses
			if(words[i+2].equals("yes")){
				bucket = results.get (words[i+1]);
				if (bucket == null){
					bucket = new MapResult (words[i+1], words[i+3], words[i+5], words[i+6]);
					bucket.setIntresult(1);
					results.put (words[i+1], bucket);
				}
				else {
					bucket.increaseintresult();
				}
	      	}		
		
		
		}
	}
	
	public ArrayList<MapResult> go (String args) throws IOException {
		
		FillContentsFromFile (args);
		FillResults();
		
		ArrayList<MapResult> items = new ArrayList<MapResult>(results.values());
		return items;
	}
	

}
