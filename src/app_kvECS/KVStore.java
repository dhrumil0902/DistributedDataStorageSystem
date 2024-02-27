package app_kvECS;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();

	private final String address;
	private final int port;
	
	public CommManager commManager;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		commManager = new CommManager();
	}

	@Override
	public void connect() throws Exception {
		if (commManager.isConnected()) {
			throw new Exception("Already connected to a server");
		}
		commManager.connect(address, port);
	}

	@Override
	public void disconnect() {
		commManager.disconnect();
	}
	
	@Override
	public KVMessage put(String key, String value) throws Exception {
		String msg = "put " + key + " " + value;
		String response = commManager.sendMessage(msg);
		return KVMessageImpl.fromString(response);
		
	}

	@Override
	public KVMessage get(String key) throws Exception {
		String msg = "get " + key;
		String response = commManager.sendMessage(msg);
		return KVMessageImpl.fromString(response);
	}

	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	public boolean isConnected() {
		return commManager.isConnected();
	}
}