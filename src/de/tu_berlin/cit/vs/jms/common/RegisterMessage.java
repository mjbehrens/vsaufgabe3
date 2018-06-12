package de.tu_berlin.cit.vs.jms.common;


public class RegisterMessage extends BrokerMessage {

	private static final long serialVersionUID = -5491978849786236925L;
	private String clientName;
    
    public RegisterMessage(String clientName) {
        super(Type.SYSTEM_REGISTER);
        
        this.clientName = clientName;
    }
    
    public String getClientName() {
        return clientName;
    }
}
