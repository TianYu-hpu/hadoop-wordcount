package cn.com.zenmaster.wordcount;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
	@Test
	public void testMkdir() {
		try {
			// 创建文件系统
			FileSystem fs = FileSystem.get(new URI("hdfs://主机名或ip地址:8020"), new Configuration());
			// 创建目录
			fs.mkdirs(new Path("/upload"));
			// 关闭资源
			fs.close();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testUpload() {
		try {
			// 创建文件系统
			FileSystem fs = FileSystem.get(new URI("hdfs://主机名或ip地址:8020"), new Configuration());
			// 上传文件
			FSDataOutputStream fsDataOutputStream = fs.create(new Path("/upload/profile"));
			FileInputStream in = new FileInputStream(new File("/etc/profile"));
			IOUtils.copyBytes(in, fsDataOutputStream, 2048, true);
			// 关闭资源
			fs.close();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDel() {
		try {
			// 创建文件系统
			FileSystem fs = FileSystem.get(new URI("hdfs://主机名或ip地址:8020"), new Configuration());
			fs.delete(new Path("/upload"), true);
			// 关闭资源
			fs.close();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
}
