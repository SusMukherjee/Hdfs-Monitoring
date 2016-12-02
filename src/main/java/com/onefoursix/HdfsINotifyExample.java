package com.onefoursix;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent;
import org.apache.hadoop.hdfs.inotify.Event.UnlinkEvent;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONObject;

@SuppressWarnings("deprecation")
public class HdfsINotifyExample {

	@SuppressWarnings("unchecked")
	public static void main(final String[] args) throws IOException,
			InterruptedException, MissingEventsException {
		
		
	    Properties mainProperties = new Properties();
	    FileInputStream file;
	    String path = args[1];
	    file = new FileInputStream(path);
	    mainProperties.load(file);
	    file.close();
	    String ugi_arg = mainProperties.getProperty("ugi_arg");
	    final String create_owner_start = mainProperties.getProperty("create_owner_start");
	    final String  unlink_path_start= mainProperties.getProperty("unlink_path_start");
	    final String http_web = mainProperties.getProperty("http_web");		
		try {
			long lastReadTxid = 0;

			System.out.println("lastReadTxid = " + lastReadTxid);

			UserGroupInformation ugi = UserGroupInformation
					.createRemoteUser(ugi_arg);

			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {

					Configuration conf = new Configuration();
					conf.set("fs.hdfs.impl",
							org.apache.hadoop.hdfs.DistributedFileSystem.class
									.getName());
					conf.set("fs.file.impl",
							org.apache.hadoop.fs.LocalFileSystem.class
									.getName());
					HdfsAdmin admin = new HdfsAdmin(URI.create(args[0]), conf);
					DFSInotifyEventInputStream eventStream = admin
							.getInotifyEventStream();
					JSONObject obj = new JSONObject();

					while (true) {
						obj.clear();
						EventBatch batch = eventStream.take();
						System.out.println("TxId = " + batch.getTxid());

						for (Event event : batch.getEvents()) {
							System.out.println("event type = "
									+ event.getEventType());
							switch (event.getEventType()) {
							case CREATE:
								CreateEvent createEvent = (CreateEvent) event;
								System.out.println("  path = "
										+ createEvent.getPath());
								System.out.println("  owner = "
										+ createEvent.getOwnerName());
								System.out.println("  Event = "
										+ createEvent.getEventType());
								
								if (createEvent.getOwnerName().startsWith(create_owner_start)) {
									obj.put("path", createEvent.getPath());
									obj.put("owner", createEvent.getOwnerName());
									obj.put("Event", createEvent.getEventType());
									InBackground(obj,http_web);
								}

								break;

							case UNLINK:
								UnlinkEvent unlinkEvent = (UnlinkEvent) event;
								System.out.println("  path = "
										+ unlinkEvent.getPath());
								System.out.println("  timestamp = "
										+ unlinkEvent.getTimestamp());
								if (unlinkEvent.getPath().startsWith(unlink_path_start)) {
								obj.put("path", unlinkEvent.getPath());
								obj.put("Event", unlinkEvent.getEventType());
								InBackground(obj,http_web);
								}
								break;

							case APPEND:
							case CLOSE:
							case RENAME:
							default:
								break;
							}
						}
					}
				}
			});
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	protected static void InBackground(JSONObject obj,String http_web) {
		@SuppressWarnings("resource")
		HttpClient httpClient = new DefaultHttpClient();
		try {
			HttpPost request = new HttpPost(http_web);
			StringEntity se = new StringEntity(obj.toString().replace("\\", ""), "UTF-8");
			se.setContentType("application/json; charset=UTF-8");
			request.setEntity(se);
			request.setHeader("Content-Type", "application/json");
			HttpResponse response = httpClient.execute(request);
			System.out.println(response.getStatusLine());
		} catch (Exception ex) {
			System.out.println(ex);
		} finally {
			httpClient.getConnectionManager().shutdown(); // Deprecated
		}
	}
}
