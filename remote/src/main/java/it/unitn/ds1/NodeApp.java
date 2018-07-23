package it.unitn.ds1;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.HashMap;
import java.util.Collections;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.util.Timeout;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;

public class NodeApp {
  static private String remotePath = null; // Akka path of the bootstrapping peer

  /*public static class Join implements Serializable {
    int id;
    public Join(int id) {
      this.id = id;
    }
  }*/
  public static class RequestNodelist implements Serializable {}
  public static class Nodelist implements Serializable {
    Map<Integer, ActorRef> nodes;
    public Nodelist(Map<Integer, ActorRef> nodes) {
      this.nodes = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(nodes)); 
      }
    }
  
  public static class RequestJoin implements Serializable{
	  ActorRef sender;
	  public RequestJoin(ActorRef sender) {
		  this.sender = sender;
	  }
  }
  
  public static class ViewChange implements Serializable{
	  int viewID;
	  Map<Integer, ActorRef> nodes;
	  ActorRef lastNode;
	  int lastNodeID;
	  
	  public ViewChange(int viewID, Map<Integer, ActorRef> nodes, ActorRef lastNode, int lastNodeID) {
		  this.viewID = viewID;
		  this.nodes = nodes;
		  this.lastNode = lastNode;
		  this.lastNodeID = lastNodeID;
	  }
	public int getViewID() {
		return viewID;
	}
	public void setViewID(int viewID) {
		this.viewID = viewID;
	}
	public Map<Integer, ActorRef> getNodes() {
		return nodes;
	}
	public void setNodes(Map<Integer, ActorRef> nodes) {
		this.nodes = nodes;
	}
	public ActorRef getLastNode() {
		return lastNode;
	}
	public void setLastNode(ActorRef lastNode) {
		this.lastNode = lastNode;
	}
	public int getLastNodeID() {
		return lastNodeID;
	}
	public void setLastNodeID(int lastNodeID) {
		this.lastNodeID = lastNodeID;
	}
  }
  
  public static class AcceptedJoin implements Serializable{
	  int nodeID;
	  int viewID;
	  Map<Integer, ActorRef> nodes;
	  public AcceptedJoin(int nodeID, int viewID, Map<Integer, ActorRef> nodes) {
		  this.nodeID = nodeID;
		  this.viewID = viewID;
		  this.nodes = nodes;
	  }
  }
  
  public static class FlushMessage implements Serializable{
	  int senderID;
	  int flushID;
	  Map<Integer, ActorRef> view;
	  
	  public FlushMessage(int senderID, int flushID, Map<Integer, ActorRef> view) {
		  this.senderID = senderID;
		  this.flushID = flushID;
		  this.view = view;
	  }
  }
  
  public static class DataMessage implements Serializable{
	  int id;
	  int senderID;
	  int viewID;
	  boolean stable;
	  
	  public DataMessage(int id, int senderID, int viewID) {
		  this.id = id;
		  this.senderID = senderID;
		  this.stable = false;
		  this.viewID = viewID;
	  }  
  }
  
  public static class HeartbeatMessage implements Serializable{
	  int senderID;
	  public HeartbeatMessage(int senderID) {
		  this.senderID = senderID;
	  }
  }
  
  public static class TimeoutMessage implements Serializable{
	  int nodeID;
	  ActorRef node;
	  public TimeoutMessage(int nodeID, ActorRef node){
		  this.nodeID = nodeID;
		  this.node = node;
	  }
  }
  
  public static class TimerNode implements Serializable{
	  
	  ActorRef node;
	  int nodeID;
	  Cancellable cancellable;
	  public TimerNode(ActorRef node, int nodeID, Cancellable cancellable) {
		  this.node = node;
		  this.nodeID = nodeID;
		  this.cancellable = cancellable;
	  }
  }
  
  public static class Node extends AbstractActor {
  
    private Map<Integer, Map<Integer, ActorRef>> views = new HashMap<>();
    private String remotePath = null;
    private int id;	//process id
    private boolean manager = false; //flag to know if it is the manager
    private int countID = 0;	//used only by manager to handle unique process id
    private boolean crashed = false;	//flag to simulate crash
    private int viewID = 0;	//current view id
    private double timeout = 4000; //max time before timeout 
    private int maxDelay = 10; //max time delay before sending new message
    private boolean active = false; //flag if process can interact
    private int inhibit = 0; //inhibit_sends counter
    private double hearttimer = 50; //heartbeat timer
    private Map<Integer, TimerNode> timerNodes = new HashMap<>();
    private Map<Integer, DataMessage> receivedMessages = new HashMap<>();
    private Map<Integer, DataMessage> deleteMessages = new HashMap<>();
    private int messageCounter = 0;
    private int messageTimer = 700;

    /* -- Actor constructor --------------------------------------------------- */
    public Node(int id, String remotePath) {
      this.id = id;
      this.remotePath = remotePath;
    }

    public static Props props(int id, String remotePath) {
      return Props.create(Node.class, () -> new Node(id, remotePath));
    }

    public void preStart() {
    	System.out.println("ID: "+this.id);
    	System.out.println("Remote path: "+this.remotePath);
      if (this.remotePath != null) {
    	  if(this.id==0) {
    		  setManager(true);
    		  this.active = true;
    		  Map<Integer, ActorRef> initNode = new HashMap<>();
    		  initNode.put(this.id, getSelf());
    		  views.put(viewID, initNode);
    		  System.out.println("VIEW_ID "+this.viewID+" COUNT VIEWS "+views.size());
    		  setTimerData();
    	  }
    	  else {
    		  if(!active) {
	    		  System.out.println("NEW JOIN REQUEST FROM "+this.id);
	    		  ActorRef managerRef = getContext().actorSelection(remotePath).anchor();
	    		  getContext().actorSelection(remotePath).tell(new RequestJoin(getSelf()), getSelf());
    		  }
    	  }
	  }
      //views.get(viewID).put(this.id, getSelf());
    }

    private void onRequestJoin(RequestJoin message) {
    	if(manager) {
    		System.out.println("RECIEVED REQUEST JOIN FROM "+getSender());
	    	increaseCountID();
	    	int newNodeID = getCountID();
	    	
	    	Map<Integer, ActorRef> newView = views.get(getViewID());
	    	newView.put(newNodeID, getSender());

	    	for(ActorRef n: views.get(getViewID()).values()) {
	    		n.tell(new ViewChange(getViewID()+1, newView, message.sender, newNodeID), getSelf() );
		    }
    	}
    }
    
    private void onViewChange(ViewChange message) {

//    	if(!manager)
//    	{
//    		System.exit(0);
//    	}
    	
    		
    	if(active) {
    		System.out.println("VIEW CHANGE "+message.viewID);
	    	//FLUSH
	    	inhibit++;
	    	for(DataMessage dm : receivedMessages.values()) {
	    		//System.out.println("MULTICAST FOR "+message.nodes.size());
	    		multicast(dm, message.getNodes());
	    		
	    		//deliver and delete
	    		//PRINT deliver
	    		System.out.println(id+" deliver multicast "+ dm.id+ " from "+dm.senderID+" within "+ dm.viewID);
	    		deleteMessages.put(dm.id, dm);
	    		receivedMessages.remove(dm.id);
	    	}
	    	multicast(new FlushMessage(this.id,message.getViewID(),message.nodes), message.getNodes());
	    	//intersect
	    	//flush
    	}
    	//FINITO FLUSH
    	//mando active al nuovo arrivato
    	if(manager) {
    		if(message.getLastNode()!=null) {
    			message.getLastNode().tell(new AcceptedJoin(message.getLastNodeID(), message.getViewID(), message.getNodes()), getSender());
    			//initTimerNodes(timeout, message.getLastNodeID(),message.getLastNode());
    		}
			//start timer to know if heartbeat crash
    		//initTimerNodes(timeout, message.getNodes());
    		
    	}
    }
    boolean created = false;
    private void multicast(Serializable message, Map<Integer, ActorRef> nodes){
    	
    	
    	for(ActorRef process :nodes.values()) {
    		try {
    			process.tell(message, getSelf());
//    			if(!manager)
//    		    	{
//    		    		System.exit(0);
//    		    	}
    		}
    		catch(Exception ex) {}
    	}
    }
    
    private void onJoin(AcceptedJoin message) {
      this.id = message.nodeID;
      //this.viewID = message.viewID;
      //this.views.put(this.viewID, message.nodes);
      //this.active = true;
      System.out.println("Node " + id + " joined");
      //start heartbeat from node to manager
      setTimerHeartbeat(hearttimer);
      setTimerData();
    }
    
    //manager timeout heartbeat
    private void addTimerNode(int nodeID, ActorRef node, double time) {
    	if(manager) {	    	
	    	Cancellable cancellable = getContext().system().scheduler().scheduleOnce(
	    			 (FiniteDuration) Duration.create(time, TimeUnit.MILLISECONDS), getSelf(),
	    			 new TimeoutMessage(nodeID, node),
	    			 getContext().system().dispatcher(), getSelf());
	    	TimerNode tn = new TimerNode(node, nodeID, cancellable);
	    	timerNodes.put(nodeID, tn);
    	}
    }
    
    private void restartTimerNodes(int nodeID, ActorRef node, double time) {
    	timerNodes.get(nodeID).cancellable.cancel();
    	timerNodes.remove(nodeID);
    	addTimerNode(nodeID, node, time);
    }
    
    //private void initTimerNodes(double time, Map<Integer, ActorRef> nodes) {
	private void initTimerNodes(double time, Map<Integer,ActorRef> nodes) {
		for(Entry<Integer, ActorRef> entry : nodes.entrySet()) {
			if(entry.getKey()!=0)
				addTimerNode(entry.getKey(), entry.getValue(), time);
		}
    }
    
	Cancellable heartCancellable = null;
	
    //node hearthbeat timer
    void setTimerHeartbeat(double time) {
//    	if(messageCounter>10 && id==2) {
//    		active=false;heartCancellable.cancel();getContext().getSystem().terminate();
//    		}else
//    	{
    	heartCancellable = getContext().system().scheduler().schedule(Duration.create(0, TimeUnit.MILLISECONDS),Duration.create(messageTimer, TimeUnit.MILLISECONDS),
  			  new Runnable() {
  			    @Override
  			    public void run() {
  			    	System.out.println(id+" SENDING HEARTBEAT TO "+getContext().actorSelection(remotePath).anchor());
  			    	getContext().actorSelection(remotePath).tell(new HeartbeatMessage(id), getSelf());
  			    }
  			}, getContext().system().dispatcher());
    	//}
	}
    
    void delay(int d) {
        try {Thread.sleep(d);} catch (Exception e) {}
    }
    
    //manager receive heartbeat
    private void onHeartbeat(HeartbeatMessage message){
    	if(manager) {
	    	try {
	    		ActorRef a = views.get(viewID).get(message.senderID);
	    		restartTimerNodes(message.senderID, a, timeout);
	    	}catch(Exception ex) {}
    	}
    }
    
    private void onReceiveMessage(DataMessage message) {
    	delay(maxDelay);
//    	if(!manager)
//        	{
//        		System.exit(0);
//        	}
    	
    	if(active) {
	    	if(message.viewID==this.viewID) {
	    		if(deleteMessages.containsKey(message.senderID)) {
		    		if(!(deleteMessages.get(message.senderID).id==message.id)) {
			    		if(receivedMessages.containsKey(message.senderID)) {
			        		//stable older message
			    			DataMessage older = receivedMessages.get(message.senderID);
			    			if(message.id > older.id ){
			    				receivedMessages.remove(message.senderID);
			    				//PRINT delivery
	
			    	    		System.out.println(id+" deliver multicast "+ older.id+ " from "+older.senderID+" within "+ older.viewID);
			        		}
			        	}
			        	receivedMessages.put(message.senderID, message);
		        	}
	    		}else {
	    			if(receivedMessages.containsKey(message.senderID)) {
		        		//stable older message
		    			DataMessage older = receivedMessages.get(message.senderID);
		    			if(message.id > older.id ){
		    				receivedMessages.remove(message.senderID);
		    				//PRINT delivery
	
		    	    		System.out.println(id+" deliver multicast "+ older.id+ " from "+older.senderID+" within "+ older.viewID);
		        		}
		        	}
		        	receivedMessages.put(message.senderID, message);
	    		}
	    	}
	    	if(message.viewID>this.viewID) {
	    		if(receivedMessages.containsKey(message.senderID)) {
	        		//stable older message
	    			DataMessage older = receivedMessages.get(message.senderID);
	    			if(message.id > older.id ){
	    				receivedMessages.remove(message.senderID);
	    				//PRINT delivery
	    				System.out.println(id+" deliver multicast "+ older.id+ " from "+older.senderID+" within "+ older.viewID);
	        		}
	        	}
	        	receivedMessages.put(message.senderID, message);
	    	}
    	}
    }
    
    private Map<Integer, Map<Integer, ActorRef>> flushes = new HashMap<>();
    
    private void cleanTimerNodes() {
    	try {
	    	for(Entry<Integer, TimerNode> entry : timerNodes.entrySet()) {	
		    		entry.getValue().cancellable.cancel();
		    		//timerNodes.remove(entry.getKey());
	    	}
	    	timerNodes.clear();
    	}catch(Exception ex) {}
    }
    
    private void onFlushMessage(FlushMessage message) {
    	delay(maxDelay);
    	//if(active) {
	    	Map<Integer, ActorRef> singleIntersect = flushes.get(message.flushID);
	    	if(singleIntersect==null)
	    		singleIntersect = new HashMap<>();
	    	singleIntersect.put(message.senderID, getSender());
	    	List<Integer> intersectedID = intersectionNodes(message.flushID);

	    	boolean checked = true;
	    	for(Integer i : intersectedID) {
	    		if(!singleIntersect.containsKey(i))
	    			checked = false;
	    	}
	    	System.out.println("FLUSH CHECKED "+checked);
	    	if(checked) {
	    		views.put(message.flushID, message.view);
				
				cleanTimerNodes();
				initTimerNodes(timeout, message.view);
				
				if(inhibit==0) {// solo il nuovo processo ha inhibit = 0 gli altri no
					this.viewID = message.flushID;
					System.out.println(id+" install view "+ this.viewID + " listprocesses");
					this.active = true;
				}
				else {
		    		while(this.viewID<message.flushID) {
		    			this.inhibit--;
		    			this.viewID++;
		    			//check install view?
			    		System.out.println(id+" install view "+ this.viewID + " listprocesses");
		    		}
		    		for(Entry<Integer, DataMessage> entry : receivedMessages.entrySet()) {
		    			if(entry.getValue().viewID<this.viewID) {
		    				//PRINT deliver
		    				//receivedMessages.remove(entry.getKey());
		    			}
		    		}
		    		//remove deletedMessages
		    		for(Entry<Integer, DataMessage> entry : deleteMessages.entrySet()) {
		    			if(entry.getValue().viewID<this.viewID) {
		    				//deleteMessages.remove(entry.getKey());
		    			}
		    		}
				}
	    		
	    	}
    	//}
    }

  //node DataMessage timer
    void setTimerData() {
    	System.out.println("TIMER DATA "+this.id);
    	getContext().system().scheduler().schedule(Duration.create(0, TimeUnit.MILLISECONDS),Duration.create(messageTimer, TimeUnit.MILLISECONDS),
    			  new Runnable() {
    			    @Override
    			    public void run() {
    			    	System.out.println(id+" VIEW: "+viewID);
    			    	//if(messageCounter>10 && id==2) {
//    			    		active=false;heartCancellable.cancel();
//    			    		delay(30000);
//    			    		getContext().system().terminate();
    			    	//	}
    			    	if(inhibit==0 && active) {
    			    		//System.out.println(id+" VIEW: "+viewID);
	    			    	messageCounter++;
	    			    	multicast(new DataMessage(messageCounter, id, viewID),views.get(viewID));
	    			    	System.out.println(id+" send multicast "+ messageCounter +" within "+ viewID);
    			    	}
    			    }
    			}, getContext().system().dispatcher());
    }
    
    private void onTimeout(TimeoutMessage message) {
    	if(manager) {
    		System.out.println("TIMEOUT PROCESS_ID:"+message.nodeID);
    		try {
	    		timerNodes.get(message.nodeID).cancellable.cancel();
		    	timerNodes.remove(message.nodeID);
    		}
    		catch(Exception ex) {}
    		
	    	Map<Integer, ActorRef> newView = views.get(viewID);
	    	newView.remove(message.nodeID);

	    	multicast(new ViewChange(viewID+1,newView,null,-1),newView);
	    	
    	}
    }
    
    @Override
    public Receive createReceive() {
    	return receiveBuilder()
    		.match(RequestJoin.class, this::onRequestJoin)
  	        .match(TimeoutMessage.class, this::onTimeout)
	        .match(ViewChange.class, this::onViewChange)
	        .match(HeartbeatMessage.class, this::onHeartbeat)
	        .match(AcceptedJoin.class, this::onJoin)
	        //on timeout-> view change con last = null
	        .match(FlushMessage.class, this::onFlushMessage)
	        .match(DataMessage.class, this::onReceiveMessage)
	        .build();
    }

	public boolean isManager() {
		return manager;
	}

	public void setManager(boolean manager) {
		this.manager = manager;
	}

	public int getCountID() {
		return countID;
	}

	public void setCountID(int countID) {
		this.countID = countID;
	}
	
	public void increaseCountID(){
		this.countID++;
	}

	public boolean isCrashed() {
		return crashed;
	}

	public void setCrashed(boolean crashed) {
		this.crashed = crashed;
	}

	public int getViewID() {
		return viewID;
	}

	public void setViewID(int viewID) {
		this.viewID = viewID;
	}
	public void increaseViewID() {
		this.viewID++;
	}

	public Map<Integer, Map<Integer, ActorRef>> getViews() {
		return views;
	}

	public void setViews(Map<Integer, Map<Integer, ActorRef>> views) {
		this.views = views;
	}
	
	public void addView(Map<Integer, ActorRef> view, int viewID) {
		this.views.put(viewID, view);
	}
	
	public void removeView(int viewID) {
		this.views.remove(viewID);
	}
	
	public List<Integer> intersectionNodes(int flushID){
		List<Integer> ids = new ArrayList<>();
		for(Entry<Integer, Map<Integer, ActorRef>> view : views.entrySet()) {
			if(view.getKey() > viewID && view.getKey() <= flushID) {
				for(Entry<Integer, ActorRef> process : view.getValue().entrySet()) {
					ids.add(process.getKey());
				}
			}
		}
		int max = flushID - viewID;
		List<Integer> uniqueIDs = new ArrayList<>();
		for(Integer id : ids) {
			int count = Collections.frequency(ids, id);
			if(count == max){
				if(!uniqueIDs.contains(id))
				{
					uniqueIDs.add(id);
				}
			}
		}
		
		return uniqueIDs;
	}
	
  }
  
  public static void main(String[] args) {

  // Load the configuration file
  Config config = ConfigFactory.load();
  int myId = config.getInt("nodeapp.id");
  String remotePath = null;

  if (config.hasPath("nodeapp.remote_ip")) {
    String remote_ip = config.getString("nodeapp.remote_ip");
    int remote_port = config.getInt("nodeapp.remote_port");
    // Starting with a bootstrapping node
    // The Akka path to the bootstrapping peer
    remotePath = "akka.tcp://mysystem@"+remote_ip+":"+remote_port+"/user/node";
    System.out.println("Starting node " + myId + "; bootstrapping node: " + remote_ip + ":"+ remote_port);
  }
  else {
    System.out.println("Starting disconnected node " + myId);
  }
  // Create the actor system
  final ActorSystem system = ActorSystem.create("mysystem", config);
  
  // Create a single node actor locally
  final ActorRef receiver = system.actorOf(
      Node.props(myId, remotePath),
      "node"      // actor name
      );
  
  }
}
