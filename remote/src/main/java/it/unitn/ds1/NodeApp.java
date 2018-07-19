package it.unitn.ds1;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Collections;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;

public class NodeApp {
  static private String remotePath = null; // Akka path of the bootstrapping peer

  public static class Join implements Serializable {
    int id;
    public Join(int id) {
      this.id = id;
    }
  }
  public static class RequestNodelist implements Serializable {}
  public static class Nodelist implements Serializable {
    Map<Integer, ActorRef> nodes;
    public Nodelist(Map<Integer, ActorRef> nodes) {
      this.nodes = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(nodes)); 
      }
    }
  
  public static class FlushMessage implements Serializable{
	  int senderID;
	  int flushID;
	  
	  public FlushMessage(int senderID, int flushID) {
		  this.senderID = senderID;
		  this.flushID = flushID;
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
	  
	  public boolean isStable() {
		  return stable;
	  }
	  
	  public void setStable(boolean stable) {
		  this.stable = stable;
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
    private double timeout = 2; //max time before timeout 
    private double maxDelay = 1.8; //max time delay before sending new message
    private boolean active = false; //flag if process can interact
    private int inhibit = 0; //inhibit_sends counter

    /* -- Actor constructor --------------------------------------------------- */
    public Node(int id, String remotePath) {
      this.id = id;
      this.remotePath = remotePath;
    }

    static public Props props(int id, String remotePath) {
      return Props.create(Node.class, () -> new Node(id, remotePath));
    }

    public void preStart() {
      if (this.remotePath != null) {
        getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
      }
      views.get(viewID).put(this.id, getSelf());
    }

    private void onRequestNodelist(RequestNodelist message) {
        getSender().tell(new Nodelist(views.get(viewID)), getSelf());
    }
    private void onNodelist(Nodelist message) {
    	views.get(viewID).putAll(message.nodes);
      for (ActorRef n: views.get(viewID).values()) {
        n.tell(new Join(this.id), getSelf());
      }
    }
    private void onJoin(Join message) {
      int id = message.id;
      System.out.println("Node " + id + " joined");
      views.get(viewID).put(id, getSender());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
        .match(RequestNodelist.class, this::onRequestNodelist)
        .match(Nodelist.class, this::onNodelist)
        .match(Join.class, this::onJoin)
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
