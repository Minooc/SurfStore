package surfstore;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.nio.file.*;
//import java.nio.file.Files;
//import java.nio.file.Paths;

public final class Client {
  private static final Logger logger = Logger.getLogger(Client.class.getName());

  private final ManagedChannel metadataChannel;
  private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

  private final ManagedChannel blockChannel;
  private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

  private final ConfigReader config;

  public Client(ConfigReader config) {
    this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
      .usePlaintext(true).build();
    this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

    this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
      .usePlaintext(true).build();
    this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

    this.config = config;
  }

  private class OrderedMap {
    List<String> hashlist;
    HashMap<String, byte[]> keyDataMap;
    public OrderedMap(List<String> hashlist, HashMap<String, byte[]> keyDataMap) {
      this.hashlist = hashlist;
      this.keyDataMap = keyDataMap;
    }
  }


  public void shutdown() throws InterruptedException {
    metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  private static String sha256(String s) {
    MessageDigest digest = null;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      System.exit(2);
    }
    byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
    String encoded = Base64.getEncoder().encodeToString(hash);
    return encoded;		
  }

  private void ensure(boolean b) {
    if (b == false) {
      throw new RuntimeException("Assertion failed!");
    }
  }

  private static Block stringToBlock(String s) {
    Block.Builder builder = Block.newBuilder();

    try {
      builder.setData(ByteString.copyFrom(s, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

    builder.setHash(sha256(s));

    return builder.build();

  }

  /* Helper function for upload. Return hashlist with data */
  private OrderedMap computeHashlist(String filename) {
	
    File fileToUpload = new File(filename);
    // Parse the content of the file to byte array
    byte[] data;
    try {
      data = Files.readAllBytes(fileToUpload.toPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Initiate values for being built and returned.
    HashMap<String, byte[]> keyDataMap = new HashMap<>();
    List<String> orderedList = new ArrayList<>();

    // Break down into blocks 4KB and add to hashlist
    long filesize = data.length;
    int blockindex = 0;
    while (filesize > 4096) {
      byte[] block = new byte[4096];
      System.arraycopy(data, blockindex, block, 0, 4096);
      String str = new String(block);
      String hashValue = sha256(str);
      keyDataMap.put(hashValue, block);
      orderedList.add(hashValue);

      filesize -= 4096;
      blockindex += 4096;
    }

    // Add final block of <4KB to hashlist
    int filesize_remainder = (int)filesize;
    byte[] block = new byte[filesize_remainder];
    System.arraycopy(data, blockindex, block, 0, filesize_remainder);
    String str = new String(block);
    String hashValue = sha256(str);
    keyDataMap.put(hashValue, block);
    orderedList.add(hashValue);

    // Declare return value and return
    OrderedMap retval = new OrderedMap(orderedList, keyDataMap);
    return retval;
  }

  private void upload(String filenameWithPath) {
    // Get original file name, e.g. /home/test/abc.txt to abc.txt
    String[] arrFilename = filenameWithPath.split("/");
    String filename = arrFilename[arrFilename.length-1];

    // Firstly check if the file already exists
    FileInfo filereadRequest = FileInfo.newBuilder().setFilename(filename).build();
    FileInfo filereadResponse = metadataStub.readFile(filereadRequest);

    // Initiate values got from readFile response
    int fileVersion = filereadResponse.getVersion();
    List<String> fileHashlist = new ArrayList<String>();
    fileHashlist = filereadResponse.getBlocklistList();

    // Compute hashlist
    OrderedMap orderedKeyDataMap = computeHashlist(filename);
	
    // Build FileInfo for modifyFile RPC call
    FileInfo.Builder fileBuilder = FileInfo.newBuilder();
    fileBuilder.setFilename(filename);
    fileBuilder.setVersion(fileVersion+1);
    fileBuilder.addAllBlocklist(orderedKeyDataMap.hashlist);
    FileInfo fileRequest = fileBuilder.build();

    // Get the result by calling modifyFile
    WriteResult result = metadataStub.modifyFile(fileRequest);
    WriteResult.Result resultVal = result.getResult();

    // If there is a missing block, we are storing them into blockstore and calling modifyFile again with full block.
    if (resultVal == WriteResult.Result.MISSING_BLOCKS) {

      // Store missing blocks into blockstore
      List<String> missingBlocks = result.getMissingBlocksList();
      for (int i=0; i < missingBlocks.size(); i++) {
        String currHashValue = missingBlocks.get(i);
        byte[] currDataByte = orderedKeyDataMap.keyDataMap.get(currHashValue); 
        ByteString currData = ByteString.copyFrom(currDataByte);

        // build block for calling BlockStore rpc
        Block.Builder blockbuilder = Block.newBuilder();
        blockbuilder.setHash(currHashValue);
        blockbuilder.setData(currData);
        Block blockToStore = blockbuilder.build();
        blockStub.storeBlock(blockToStore);
      }

      // Once all blocks are uploaded..
      FileInfo.Builder fileBuilder_ = FileInfo.newBuilder();
      fileBuilder_.setFilename(filename);
      fileBuilder_.setVersion(fileVersion+1);
      fileBuilder_.addAllBlocklist(orderedKeyDataMap.hashlist);
      FileInfo fileRequest_ = fileBuilder_.build();
      metadataStub.modifyFile(fileRequest_);
    }
    System.out.println("OK");
  }

  /* Helper function for downloadWithPath - convert arraylist of byte to byte array */
  byte[] flattenArrayList(ArrayList<byte[]> byteArrayList){
    ArrayList<Byte> byteList = new ArrayList<>();

    //extract each byte in the each subarray
    for(byte[] array : byteArrayList){
      for(int i = 0; i < array.length; i++){
        byteList.add(array[i]);
      }
    }
    // Built byte array to return
    byte[] retval = new byte[byteList.size()];
    for(int i = 0; i < retval.length; i++){
      retval[i] = byteList.get(i).byteValue();
    }
    return retval;
  }

  /* Helper function for downloadWithPath - make rpc call to get data to be downloaded. */
  private ArrayList<byte[]> download(String filename) {
    //build the file for rpc	
    FileInfo fileReadRequest = FileInfo.newBuilder().setFilename(filename).build();
    //get a response from rpc 
    FileInfo fileReadResponse = metadataStub.readFile(fileReadRequest);

    //get the file data from the response
    int version = fileReadResponse.getVersion();
    List <String> hashList = fileReadResponse.getBlocklistList();
    ArrayList <byte[]> byteArrayList = new ArrayList<>();

    // Check if file doesn't exist
    if (version == 0 || (hashList.size() == 1 && hashList.get(0).equals("0"))) {
      System.out.println("Not Found");
      System.exit(2);
    }

    //get each block from the hashList and store in to byteArray
    for(String key : hashList){
      Block.Builder blockBuilder = Block.newBuilder();
      blockBuilder.setHash(key);
      Block blockToGet = blockBuilder.build();
      Block response = blockStub.getBlock(blockToGet);
      byte[] data = response.getData().toByteArray();
      byteArrayList.add(data);
    }
    return byteArrayList;
  }

  /* Directly called from client - Handle the path to download */
  private void downloadWithPath(String filename, String path){
    //convert to single byte array
    ArrayList<byte[]> byteArrayList = download(filename);
    byte[] flattenedData = flattenArrayList(byteArrayList);
    Path realPath = Paths.get(path + "/" + filename);

    // Create the file and write data into the file.
    try {
      Files.deleteIfExists(realPath);
      Files.createFile(realPath);
      Files.write(realPath, flattenedData);
		}
    catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  private void delete(String filename) {
    // Firstly call readFile to check if file exist
    FileInfo filereadRequest = FileInfo.newBuilder().setFilename(filename).build();
    FileInfo filereadResponse = metadataStub.readFile(filereadRequest);
    int fileVersion = filereadResponse.getVersion();
    List <String> hashList = filereadResponse.getBlocklistList();

    // If file is not found or deleted, print Not Found
    if (fileVersion == 0 || (hashList.size() == 1 && hashList.get(0).equals("0"))) {
      System.out.println("Not Found");
      System.exit(2);
    }

    // Build request and call DeleteFile
    FileInfo request = FileInfo.newBuilder().setFilename(filename).setVersion(fileVersion+1).build();
    WriteResult response = metadataStub.deleteFile(request);
    WriteResult.Result result = response.getResult();

    // Print OK to indicate successful deletion
    if (result == WriteResult.Result.OK) {
      System.out.println("OK");
    }
  }

  private void getversion(String filename) {
    // Build request and call getVersion rpc call to retrieve version.
    // When file doesn't exist, readFile will return 0.
    FileInfo request = FileInfo.newBuilder().setFilename(filename).build();
    FileInfo response = metadataStub.getVersion(request);
    int version = response.getVersion();
    System.out.println(version);
  }

  private void go(String function, String filename, String filepath) {
    logger.info("pinging");
    metadataStub.ping(Empty.newBuilder().build());
    logger.info("Successfully pinged the Metadata server");

    blockStub.ping(Empty.newBuilder().build());
    logger.info("Successfully pinged the Blockstore server");

    // Call the requested function.
    switch(function) {
      case "upload":	upload(filename);	
                      break;
      case "download":	downloadWithPath(filename, filepath);
                        break;
      case "delete":	delete(filename);
                      break;
      case "getversion":	getversion(filename);
                          break;
      default: System.exit(2);
    }
  }

  private static Namespace parseArgs(String[] args) {
    ArgumentParser parser = ArgumentParsers.newFor("Client").build()
      .description("Client for SurfStore");
    parser.addArgument("config_file").type(String.class)
      .help("Path to configuration file");
    parser.addArgument("function").type(String.class);
    parser.addArgument("filename").type(String.class);
    parser.addArgument("filepath").nargs("?").type(String.class).setDefault("");

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

    Client client = new Client(config);
    String function = new String(c_args.getString("function"));
    String filename = new String(c_args.getString("filename"));
    String filepath = new String(c_args.getString("filepath"));

    try {
      client.go(function, filename, filepath);
    } finally {
      client.shutdown();
    }
  }

}
