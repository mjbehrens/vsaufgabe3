package de.tu_berlin.cit.vs.jms.common;

public class QuitMessage extends BrokerMessage {

	private String content;
	
	public QuitMessage(String content) {
		super(Type.SYSTEM_QUIT);
		
	}
	
	public String getContent() {
		return content;
	}

}
