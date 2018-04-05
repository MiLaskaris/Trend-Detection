import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class MapResult {
	/* The eventid is associated with the result set. */
	private String eventid;

	/*
	 * The list is the result of the map phase. Every time we add the values of the 
	 * EmitIntermediate (eventid, value), in this list.
	 */
	private List<String> values;

	private String result;
	private int intresult;
	private String catid;
	private String lat;
	private String lon;

	public MapResult(String _eventid, String _catid, String _lat, String _lon) {
		eventid = new String(_eventid);
		catid = new String(_catid);
		lat = new String(_lat);
		lon = new String(_lon);
		values = Collections.synchronizedList(new LinkedList<String>());
		result = "1";
		setIntresult(0);
	}

	public void addValue(String _value) {
		values.add(_value);
	}

	public void setResult(String _result) {
		result = _result;
	}

	public String getEventid() {
		return eventid;
	}

	public String getResult() {
		return result;
	}

	public Iterator<String> iterator() {
		return values.iterator();
	}

	public String toString() {
		return eventid + ":" + result;
	}

	public String getCatid() {
		return catid;
	}

	public void setCatid(String catid) {
		this.catid = catid;
	}

	public List<String> getValues() {
		return values;
	}

	public String getLat() {
		return lat;
	}

	public void setLat(String lat) {
		this.lat = lat;
	}

	public String getLon() {
		return lon;
	}

	public void setLon(String lon) {
		this.lon = lon;
	}

	public int getIntresult() {
		return intresult;
	}

	public void setIntresult(int intresult) {
		this.intresult = intresult;
	}

	public void increaseintresult() {
		intresult++;
	}

	public void increaseintresult(int incr) {
		intresult += incr;
	}
}
