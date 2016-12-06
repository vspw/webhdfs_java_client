package org.apache.hadoop.fs.http.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.hadoop.security.authentication.client.AuthenticationException;


public class KerberosWebHDFSConnectionTest {
	KerberosWebHDFSConnection conn = null;

	public void setUp() throws Exception {
		conn = new KerberosWebHDFSConnection("http://namnode.hdp.com.com:50070", "username@kerb.realm.com", "xxxxpassword");
	}


	public void tearDown() throws Exception {
	}


	public void getHomeDirectory() throws MalformedURLException, IOException, AuthenticationException {
		
		String json = conn.getHomeDirectory();
		System.out.println(json);
	}
	

	public void listStatus() throws MalformedURLException, IOException, AuthenticationException {
		String path= "user/t93ki8h";
		String json = conn.listStatus(path);
		System.out.println(json);
	}
	

	public void open() throws MalformedURLException, IOException, AuthenticationException {
		String path="user/t93ki8h/tmp.txt";
		FileOutputStream os = new  FileOutputStream(new File("/tmp/downloadfromhdfs.file"));
		String json = conn.open(path, os);
		System.out.println(json);
	}
	
	
	//@Test
	public void create() throws MalformedURLException, IOException, AuthenticationException {
		FileInputStream is = new FileInputStream(new File("/tmp/downloadfromhdfs.file"));
		String path="user/t93ki8h/newupload.file"; 
		String json = conn.create(path, is);
		System.out.println(json);
	}
	
	//@Test
	public void delete() throws MalformedURLException, IOException, AuthenticationException {
		String path="user/t93ki8h/bigfile.tar.gz-new"; 
		String json = conn.delete(path);
		System.out.println(json);
	}
	
    public static void main(String[] args) {

    	KerberosWebHDFSConnectionTest test= new KerberosWebHDFSConnectionTest();
        try {
        	test.setUp();
        	test.listStatus();
            System.out.println("Successfully Connected!");
        } catch (Exception e){
            e.printStackTrace();
        }
    }

}
