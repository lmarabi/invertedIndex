package server;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TwitterPost{

	private JSONObject post;

    private double lat;
    private double lng;
    private String created_at ="";


	public TwitterPost(){
        post = null;
    }
	public TwitterPost(String jsonStr) {
		
		post = new JSONObject(jsonStr);
        //System.out.println(post.getString("text"));
		

		
		//fill the reduced post
		
		//extract tweet location
		double [] coords = new double[2];
		
		
		try
		{

            if(post.get("created_at") != null) created_at =post.getString("created_at");
            JSONObject geo = post.getJSONObject("geo");
			JSONArray coordsJSON = (JSONArray) geo.get("coordinates");
			lat = coordsJSON.getDouble(0);
			lng = coordsJSON.getDouble(1);
		}
		catch(org.json.JSONException e)
		{
			//
		}


		
	}

    public static String getTwitterDate(String date) throws ParseException {

        final String TWITTER="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
        SimpleDateFormat sf = new SimpleDateFormat(TWITTER);
        sf.setLenient(true);
        System.out.println(sf.parse(date).toString() + " you");
        return sf.parse(date).toString();
    }


    public double getLng(){
        return lng;

    }

    public double getLat(){
        return lat;
    }

    public String getCreatedAt(){
        return created_at;
    }
    public String getText() {
		try{
			return post.getString("text");
		}
		catch(JSONException e)
		{
			System.err.println("No Text");
			return "dummy";
		}
	}
	public QEntry createQEntry() {
		return new QEntry(this, getText());
	}



}
