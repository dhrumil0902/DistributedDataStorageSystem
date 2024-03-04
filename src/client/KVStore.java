package client;

import org.apache.log4j.Logger;

import ecs.IECSNode;
import shared.BST;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.utils.HashUtils;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();

	private String address;
	private int port;
	private String nodeName;
	private BST metadata;
	
	private final CommManager commManager;
	
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
		updateMetadata();
		nodeName = address + ":" + port;
	}

	@Override
	public void disconnect() {
		commManager.disconnect();
	}
	
	@Override
	public KVMessage put(String key, String value) throws Exception {
		String request = "put " + key + " " + value;
		setServerForKey(key);
		KVMessage responseMessage = sendRequest(request);
		if (responseMessage.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			updateMetadata();
			setServerForKey(key);
			responseMessage = sendRequest(request);
		}
		return responseMessage;
		
	}

	@Override
	public KVMessage get(String key) throws Exception {
		String request = "get " + key;
		setServerForKey(key);
		KVMessage responseMessage = sendRequest(request);
		if (responseMessage.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			updateMetadata();
			setServerForKey(key);
			responseMessage = sendRequest(request);
		}
		return responseMessage;
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

	public String getServerName() {
		return nodeName;
	}

	private KVMessage sendRequest(String request) throws Exception {
		String response = commManager.sendMessage(request);
		KVMessage responseMessage =  KVMessageImpl.fromString(response);
		return responseMessage;
	}

	private void updateMetadata() throws Exception {
		KVMessage metadataMessage = sendRequest("keyrange");
		if (metadataMessage.getStatus() != KVMessage.StatusType.KEYRANGE_SUCCESS)
			throw new Exception("Keyrange query failed");
		metadata = metadataMessage.getMetadata();
	}

	private void setServerForKey(String key) throws Exception {
		String hashedKey = HashUtils.getHash(key);
		if (metadata.isEmpty()) {
            return;
        }
		
		IECSNode node = metadata.floorEntry(hashedKey);

		if (!node.getNodeName().equals(nodeName)) {
			disconnect();
			this.address = node.getNodeHost();
			this.port = node.getNodePort();
			this.nodeName = node.getNodeName();
			connect();
		}
	}
}