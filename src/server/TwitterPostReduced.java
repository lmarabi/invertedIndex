package server;

import org.json.JSONObject;

public class TwitterPostReduced {

	public JSONObject post;
	public TwitterPostReduced(){post = null;}
	public TwitterPostReduced(String jsonStr) {
		post = new JSONObject(jsonStr);
	}
}
