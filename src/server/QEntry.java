package server;

public class QEntry {
	private TwitterPost post;
	private String text;
	public QEntry(TwitterPost p, String t)
	{

		post = p;
		text = t;
	}
	public TwitterPost getPost() {
		return post;
	}
	public void setPost(TwitterPost post) {
		this.post = post;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
}
