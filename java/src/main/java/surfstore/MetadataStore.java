package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.*;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;

public final class MetadataStore {
	private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

	private static List<surfstore.SurfStoreBasic.LogEntry> log;
	private static ManagedChannel blockChannel;
	private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;

	protected Server server;
	protected ConfigReader config;

	protected static int serverNum;
	protected static int leaderNum;
	protected static boolean isDistributed;
  protected static boolean hasCrashedServer;

	private static ManagedChannel metadataChannel1;
	private static ManagedChannel metadataChannel2;
	private static MetadataStoreGrpc.MetadataStoreBlockingStub followerStub1;
	private static MetadataStoreGrpc.MetadataStoreBlockingStub followerStub2;


	public MetadataStore(ConfigReader config) {

 		this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
		.usePlaintext(true).build();
		this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

		this.config = config;
		this.serverNum = 0; //default value
		this.leaderNum = 0;
		this.isDistributed = false;
    this.hasCrashedServer = false;
		this.log = new ArrayList<surfstore.SurfStoreBasic.LogEntry>();
	}

	public void shutdown() throws InterruptedException {
		blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
	}

	public void connectFollowers(int n) {
		// Find port number for 2 followers
		int[] followerPort = new int[n-1];
		int j = 0;
		for (int i=1 ; i <= n; i++) {
			if (i != leaderNum) {
				followerPort[j] = i;
				j++;
			}
		}

		// For leader, make 2 metadata stubs to communicate follower
		this.metadataChannel1 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(followerPort[0]))
		.usePlaintext(true).build();
   	 	this.followerStub1 = MetadataStoreGrpc.newBlockingStub(metadataChannel1);
		this.metadataChannel2 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(followerPort[1]))
		.usePlaintext(true).build();
		this.followerStub2 = MetadataStoreGrpc.newBlockingStub(metadataChannel2);
	}

	enum RPCResult {
		OK, OLD_VERSION, MISSING_BLOCKS, NOT_LEADER
	}

	private void start(int port, int numThreads) throws IOException {
    server = ServerBuilder.forPort(port)
      .addService(new MetadataStoreImpl())
      .executor(Executors.newFixedThreadPool(numThreads))
      .build()
      .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
    	@Override
      public void run() {
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        MetadataStore.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() {
		if (server != null) {
			server.shutdown();
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
		if (server != null) {
			server.awaitTermination();
    }
  }

  private static Namespace parseArgs(String[] args) {
    ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
      .description("MetadataStore server for SurfStore");
    parser.addArgument("config_file").type(String.class)
      .help("Path to configuration file");
    parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
      .help("Set which number this server is");
    parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
      .help("Maximum number of concurrent threads");

    Namespace res = null;
    try {
      res = parser.parseArgs(args);
    } catch (ArgumentParserException e){
      parser.handleError(e);
    }
      return res;
  }

  public static void main(String[] args) throws Exception {
    Namespace c_args = parseArgs(args);
    if (c_args == null){
      throw new RuntimeException("Argument parsing failed");
    }
 
    File configf = new File(c_args.getString("config_file"));
    ConfigReader config = new ConfigReader(configf);

    if (c_args.getInt("number") > config.getNumMetadataServers()) {
      throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
    }

		final MetadataStore server = new MetadataStore(config);
		server.serverNum = c_args.getInt("number");
		server.leaderNum = config.getLeaderNum();
		if (config.getNumMetadataServers() > 1) server.isDistributed = true;

		/* Get follower port number */
		if (server.serverNum == server.leaderNum) {
			System.out.println("Num of server is " + config.getNumMetadataServers());
			if (isDistributed) server.connectFollowers(config.getNumMetadataServers());
		}

		System.out.println("starting:");
		server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));

		// For distributed system, call update for followers every 500ms.
		if (isDistributed) {
			if (server.serverNum == server.leaderNum){
			  Timer t = new Timer();
				t.schedule(new TimerTask(){
					@Override
					public void run(){
						// Get leader's log
						if (hasCrashedServer) {
							LogList.Builder loglistBuilder = LogList.newBuilder();
							loglistBuilder.addAllLog(MetadataStore.log);
							LogList loglist = loglistBuilder.build();
							// Call updateAll for 2 followers
							SimpleAnswer f1 = followerStub1.updateAll(loglist);
							SimpleAnswer f2 = followerStub2.updateAll(loglist);
            	if (f1.getAnswer() && f2.getAnswer()) server.hasCrashedServer = false;
            }
					}
				},0, 500);
			}
		}
		server.blockUntilShutdown();
	}

		static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {

			class MetaData {
				ArrayList<String> hashlist;
				int version;
				public MetaData(ArrayList<String> hashlist, int version){
					this.hashlist = hashlist;
					this.version = version;
				}
			}

			HashMap<String, MetaData> fileMap;
			boolean isOnline = false;

			public MetadataStoreImpl() {
				super();
				this.fileMap = new HashMap<String, MetaData>();
				this.isOnline = true;
			}

			private void invokeCommand(int command, FileInfo info){
				//call the method
				if(command == 0) localModify(info);
				else localDelete(info);
			}

			// Construct LogEntry
			private surfstore.SurfStoreBasic.LogEntry logBuilder(int term, int command, FileInfo info) {

				// Append new entry to log
				surfstore.SurfStoreBasic.LogEntry.Builder newLogBuilder = surfstore.SurfStoreBasic.LogEntry.newBuilder();
				newLogBuilder.setTerm(term);
				newLogBuilder.setCommand(command);
				newLogBuilder.setFile(info);
				return newLogBuilder.build();			

			}
		
			// Local function of localModify. Only called to update follower's metadata
			private void localModify(FileInfo request){

				String filename = request.getFilename();
				int requestVersion = request.getVersion();
				int currentVersion = 0;

				if (this.fileMap.containsKey(filename)) {
					currentVersion = this.fileMap.get(filename).version;
					if (requestVersion != currentVersion + 1) return;
				}
				if (requestVersion < 0) return;

				List<String> hashlistToFind = new ArrayList<String>();
				hashlistToFind = request.getBlocklistList();
				ArrayList<String> missingBlocklist = new ArrayList<String>();
				boolean hasMissing = false;	// indicate whether there is missing block
			
				// Create the list of missing blocks
				for (int i=0; i < hashlistToFind.size(); i++) {
					String thisBlock = hashlistToFind.get(i);
					Block blockstoreRequest = Block.newBuilder().setHash(thisBlock).build();
					boolean isMissing = blockStub.hasBlock(blockstoreRequest).getAnswer();
					if (!isMissing) {
						hasMissing = true;	// If there is at least one missing block, set to true
					}
				}
				if(!hasMissing){
					ArrayList<String> hashlistToAdd = new ArrayList<String>(hashlistToFind);
					MetaData dataToAdd = new MetaData(hashlistToAdd, requestVersion);
					fileMap.put(filename, dataToAdd);
				}
			}

			// Local function of deleteFile. Only called to update follower's metadata
			private void localDelete(FileInfo request){
				String filename = request.getFilename();
				if (fileMap.containsKey(filename)) {	
					int requestVersion = request.getVersion();
					int currentVersion = fileMap.get(filename).version;
					if (requestVersion != currentVersion + 1) return;
					fileMap.remove(filename);
					// add hashlist with a single value of "0"
					ArrayList<String> nullHashlist = new ArrayList<>();
					nullHashlist.add("0");
					MetaData nullData = new MetaData(nullHashlist, requestVersion);
					fileMap.put(filename, nullData);
				}

			}

    	@Override
    	public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
 	  		Empty response = Empty.newBuilder().build();
      	responseObserver.onNext(response);
      	responseObserver.onCompleted();
   		}

      @Override
    	public void readFile(surfstore.SurfStoreBasic.FileInfo request,
        io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {

				FileInfo.Builder builder = FileInfo.newBuilder();
				String filename = request.getFilename();

				if (fileMap.containsKey(filename) == false) {
					// File Not Found
					builder.setFilename(filename);
					builder.setVersion(0);
				} else if (fileMap.get(filename).hashlist.size() == 1 && fileMap.get(filename).hashlist.get(0) == "0") {
					// Deleted File Found
					builder.setFilename(filename);
					builder.setVersion(fileMap.get(filename).version);
					builder.addBlocklist("0");
				}
				else {
					// File Found
					MetaData fileToRead = fileMap.get(filename);
					builder.setFilename(filename);
					builder.setVersion(fileToRead.version);
					for (int i=0; i < fileToRead.hashlist.size(); i++) {
						builder.addBlocklist(fileToRead.hashlist.get(i));
					}
				}
				FileInfo response = builder.build();
				responseObserver.onNext(response);
				responseObserver.onCompleted();
			}

      @Override
    	public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
      	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {

				WriteResult.Builder writeresultBuilder = WriteResult.newBuilder();
				WriteResult.Result result;

				// Initialize variable fields
				String filename = request.getFilename();
				int requestVersion = request.getVersion();
				int currentVersion = 0;
				boolean isOK = false;
				boolean isOld = false;

				// For negative version, set isOld to true
				if (requestVersion < 0) isOld = true;

				// Is request version is old, set isOld to true
				if (this.fileMap.containsKey(filename)){
					currentVersion = this.fileMap.get(filename).version;
					if (requestVersion < currentVersion + 1) isOld = true;
				}

				// Initiate fields for finding missing blocks.
				List<String> hashlistToFind = new ArrayList<String>();
				hashlistToFind = request.getBlocklistList();
				ArrayList<String> missingBlocklist = new ArrayList<String>();
				boolean hasMissing = false;	// indicate whether there is missing block

				// Create the list of missing blocks
				for (int i=0; i < hashlistToFind.size(); i++) {
					String thisBlock = hashlistToFind.get(i);
					Block blockstoreRequest = Block.newBuilder().setHash(thisBlock).build();
					boolean isMissing = blockStub.hasBlock(blockstoreRequest).getAnswer();
					if (!isMissing) {
						hasMissing = true;	// If there is at least one missing block, set to true
						writeresultBuilder.addMissingBlocks(thisBlock);
					}
				}

				// Write the result
				if (isOld) {
					// Old version
					result = WriteResult.Result.OLD_VERSION;
				} else if (hasMissing) {
					// Missing block
					result = WriteResult.Result.MISSING_BLOCKS;
				} else {
					// OK - update metadata
					isOK = true;
					result = WriteResult.Result.OK;
					ArrayList<String> hashlistToAdd = new ArrayList<String>(hashlistToFind);
					MetaData dataToAdd = new MetaData(hashlistToAdd, requestVersion);
					fileMap.put(filename, dataToAdd);
				}

				// Set version
				writeresultBuilder.setResult(result);
				writeresultBuilder.setCurrentVersion(requestVersion);

				// Build the response and return
				WriteResult response = writeresultBuilder.build();
				responseObserver.onNext(response);
				responseObserver.onCompleted();

				if (isOK && isDistributed){
					// Issue 2-phase commit with followers
					if (serverNum == leaderNum) {
						surfstore.SurfStoreBasic.LogEntry newLog = logBuilder(1,0,request);			
						MetadataStore.log.add(newLog);

						// First phase: append log of followers
						SimpleAnswer response1 = followerStub1.appendLog(newLog);
						SimpleAnswer response2 = followerStub2.appendLog(newLog);

            if(!response1.getAnswer() || !response2.getAnswer()) hasCrashedServer = true;
						// Second phase: commit
						if (response1.getAnswer())	followerStub1.modifyFile(request);
						if (response2.getAnswer())	followerStub2.modifyFile(request);

					}
				}

			}

      @Override
    	public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
     		io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {

				WriteResult.Builder responseBuilder = WriteResult.newBuilder();

				String filename = request.getFilename();
				boolean isOK = false;

				if (fileMap.containsKey(filename)) {	
					int requestVersion = request.getVersion();
					int currentVersion = fileMap.get(filename).version;
					// For request of old version, just indicate that it's an old version
					if (requestVersion != currentVersion + 1) {
						responseBuilder.setResult(WriteResult.Result.OLD_VERSION);
					}
					else {
						// Remove from metadata
						fileMap.remove(filename);

						// add hashlist with a single value of "0"
						ArrayList<String> nullHashlist = new ArrayList<>();
						nullHashlist.add("0");
						MetaData nullData = new MetaData(nullHashlist, requestVersion);
						fileMap.put(filename, nullData);
						responseBuilder.setCurrentVersion(requestVersion);
						responseBuilder.setResult(WriteResult.Result.OK);
						isOK = true;
					}
				}
				else {
					responseBuilder.setResult(WriteResult.Result.MISSING_BLOCKS);
				}

				// Build a response
				WriteResult response = responseBuilder.build();
				responseObserver.onNext(response);
				responseObserver.onCompleted();


				// Issue 2-phase commit with followers
				if (isOK && isDistributed) {
					if (serverNum == leaderNum) {
						surfstore.SurfStoreBasic.LogEntry newLog = logBuilder(1,1,request);			
						MetadataStore.log.add(newLog);
	
						SimpleAnswer response1 = followerStub1.appendLog(newLog);
						SimpleAnswer response2 = followerStub2.appendLog(newLog);

            if(!response1.getAnswer() || !response2.getAnswer()) hasCrashedServer = true;

						// Second phase: commit
						if (response1.getAnswer())	followerStub1.deleteFile(request);
						if (response2.getAnswer())	followerStub2.deleteFile(request);
					}
				}
    	}

      @Override
    	public void isLeader(surfstore.SurfStoreBasic.Empty request,
        io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

				boolean answer = (leaderNum == serverNum);

        SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(answer).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    	}

      @Override
    	public void crash(surfstore.SurfStoreBasic.Empty request,
				io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {

				isOnline = false;	// set isOnline to false to indicate server has crashed
        Empty response = Empty.newBuilder().build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    	}

			@Override
			public void restore(surfstore.SurfStoreBasic.Empty request,
				io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {

				isOnline = true; // set isOnline to true to indicate server is back
				Empty response = Empty.newBuilder().build();
				responseObserver.onNext(response);
				responseObserver.onCompleted();
		}

			@Override
			public void isCrashed(surfstore.SurfStoreBasic.Empty request,
				io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

				SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(!isOnline).build();
				responseObserver.onNext(response);
				responseObserver.onCompleted();
			}

			@Override
			public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
				io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {

				FileInfo.Builder builder = FileInfo.newBuilder();
				String filename = request.getFilename();

				if (fileMap.containsKey(filename) == false) {
					// File Not Found
					builder.setFilename(filename);
					builder.setVersion(0);
				} else if (fileMap.get(filename).hashlist.size() == 1 && fileMap.get(filename).hashlist.get(0) == "0") {
					// Deleted File Found 
					builder.setFilename(filename);
					builder.setVersion(fileMap.get(filename).version);
				}
				else {
					// File Found
					MetaData fileToRead = fileMap.get(filename);
					builder.setFilename(filename);
					builder.setVersion(fileToRead.version);
				}

				FileInfo response = builder.build();
				responseObserver.onNext(response);
				responseObserver.onCompleted();
			}

			// Append log of follower
			@Override
			public void appendLog(surfstore.SurfStoreBasic.LogEntry request,
				io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

				SimpleAnswer.Builder responseBuilder = SimpleAnswer.newBuilder();
				// Append log if server is online.
				if (isOnline) {
					MetadataStore.log.add(request);
					responseBuilder.setAnswer(true);
				}
				else {
          responseBuilder.setAnswer(false);
        }

				SimpleAnswer response = responseBuilder.build();
				responseObserver.onNext(response);
				responseObserver.onCompleted();
			}

			//leader calls this function every 500 ms
			@Override
			public void updateAll(surfstore.SurfStoreBasic.LogList request,
				io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

				SimpleAnswer.Builder responseBuilder = SimpleAnswer.newBuilder();
        responseBuilder.setAnswer(false);
				if (isOnline) {
					List<surfstore.SurfStoreBasic.LogEntry> leaderList = request.getLogList();
					//check if not up to date	
					if(leaderList.size() > MetadataStore.log.size()){
						//update the log, then call the command with param
						for(int i = MetadataStore.log.size(); i < leaderList.size(); i++){
							//update this server's log
							surfstore.SurfStoreBasic.LogEntry entry = leaderList.get(i);
							int command = entry.getCommand();
							int term = entry.getTerm();
							FileInfo info = entry.getFile();
							MetadataStore.log.add(logBuilder(term,command,info));
							//invoke the command
							invokeCommand(command, info);
						}
					}
          else if (leaderList.size() == MetadataStore.log.size()) {
            responseBuilder.setAnswer(true);
          }
				}
        SimpleAnswer response = responseBuilder.build();
				responseObserver.onNext(response);
				responseObserver.onCompleted();
			}


		}	// End of MetadataStoreImpl

} // End of MetadataStore
