package convert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 수집기 데이터 HDFS경로에 등록
 * 
 *  [스케줄러]
 *   1. 02:15 : 파일 Merge
 *    - 어제 날짜 경로의 File Read 후 하나의 MERGE파일(MEDIA.MERGE.YYYYMMDD.txt) 생성
 *    - 오늘 날짜 경로의 File Read 후 MEDIA.MERGE.YYYYMMDD.txt에 Append(하루치 데이터가 다음날 새벽까지 수집되기 때문에)
 *      
 *   2. 02:20 : Merge파일 Convert 후 HDFS 경로에 등록
 *    - MERGE한 파일인 MEDIA.MERG.YYYYMMDD.txt를 Read
 *    - Convert 작업 수행
 *    - Hdfs 경로에 YYYYMMDD.json 파일 생성
 *    - MEDIA.MERGE.YYYYMMDD.txt파일 삭제
 *  
 * @author ty
 * @since 2022-05-17
 * @version : v1.0
 * <pre>
 *  ******************************************
 *  수정 이력
 *
 *  수정일                  수정자                 수정내용
 *  ------------------------------------------
 *  2022-05-17    ty          최초 등록
 *
 *
 *  ******************************************
 * 
 * </pre>
 */
public class ConvertToJson {
	
    private static BufferedReader br = null;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    private static Calendar c1 = Calendar.getInstance();
    private static String strToday = sdf.format(c1.getTime());
    private static Configuration conf = null;
    
    private static String readFilePath = "/data/crawler-dump/"+strToday+"/";
    private static String uploadNewsFilePath = "/data/shared/news/"+strToday+"/"+strToday+".json";
    private static String uploadBlogFilePath = "/data/shared/blog/"+strToday+"/"+strToday+".json";
    
    //테스트용
//    private static String readFilePath = "/home/nrc/temp/"+strToday+"/";
//    private static String uploadNewsFilePath = "/hdfs_test/news/"+strToday+"/"+strToday+"news.json";
//    private static String uploadBlogFilePath = "/hdfs_test/news/"+strToday+"/"+strToday+"blog.json";
    
    
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		try {
			
			conf = new Configuration();
			
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			conf.set("fs.defaultFS", "hdfs://hadoop-cluster");
			conf.set("fs.default.name", "hdfs://hadoop-cluster");
			conf.set("dfs.nameservices", "hadoop-cluster");
			conf.set("dfs.ha.namenodes.hadoop-cluster", "nn1,nn2");
			conf.set("dfs.namenode.rpc-address.hadoop-cluster.nn1", "hmng1:8020");
			conf.set("dfs.namenode.rpc-address.hadoop-cluster.nn2", "hmng2:8020");
			conf.set("dfs.client.failover.proxy.provider.hadoop-cluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
			conf.set("dfs.ha.automatic-failover.enabled", "true");
			
			if(args.length > 0) {
				for (int i = 0; i < args.length; i++) {
					System.out.println("args["+i+"] : " + args[i] );
					strToday = args[i];
					
					readFilePath = "/data/crawler-dump/"+strToday+"/";
					uploadNewsFilePath = "/data/shared/news/"+strToday+"/"+strToday+".json";
					uploadBlogFilePath = "/data/shared/blog/"+strToday+"/"+strToday+".json";
					
					/*테스트용*/
//					readFilePath = "/home/nrc/temp/"+strToday+"/";
//					uploadNewsFilePath = "/hdfs_test/news/"+strToday+"/"+strToday+"news.json";
//					uploadBlogFilePath = "/hdfs_test/news/"+strToday+"/"+strToday+"blog.json";
					convsertToJson();
				}
			}
			else {
				convsertToJson();
			}
		} catch (Exception e) { 
			e.printStackTrace();
		} finally {
			resetReader();
		}
	}
	
	private static void resetReader() {
		try {
			if (br != null) {
				br.close();
            }
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			br = null;
        }
	}
	
	private static void convsertToJson() throws Exception {
		String[] media = {"NEWS","BLOG"};
		for (int i = 0; i < media.length; i++) {
			convertToJsonMedia(media[i]);
		}
	}
	
	private static void convertToJsonMedia(String media) throws Exception {
		System.out.println("== NRC JSON CONVERT START("+media+") ==");

		String mergeFile = readFilePath+media+".MERGE."+strToday+".txt";
		File fileExists = new File(mergeFile);
//		if(!fileExists.exists()) {
//			System.out.println("== CREATE FILE START("+media+") ==");
//			CreateFilesByDate.fileReadSFTP(readFilePath, media, strToday);
//			CreateFilesByDate.splitCreateFilesByDate(readFilePath + media + "." + strToday + ".txt",media,strToday);
//			System.out.println("== CREATE FILE END("+media+") ==");
//		}
		convertJsonParse(readFilePath+media+".MERGE."+strToday+".txt", media == "NEWS" ? uploadNewsFilePath : uploadBlogFilePath, media);
		
		System.out.println("== NRC JSON CONVERT END("+media+") ==");
		
		// merge한 파일 삭제
		fileExists.delete();
	}

	@SuppressWarnings("unchecked")
	private static void convertJsonParse(String mergeFile, String path, String media) throws IOException {
		System.out.println("== JSON PARSE START ==");
		System.out.println("== READ FILE : " + mergeFile);
		
		InputStream inputStream = new FileInputStream(mergeFile);
		br = new BufferedReader(new InputStreamReader(inputStream));
		
		JSONParser parser = new JSONParser();
		Object obj;
		String line = ""; // 텍스트 파일 한줄씩 담을 객체
		
		// FileSystem 설정
		FileSystem dfs  = null;
		FSDataOutputStream out = null;
		try {
			dfs  = FileSystem.get(conf);
			Path filenamePath = new Path(path);
			System.out.println("File Exists : " + dfs.exists(filenamePath));
			
			// Write data
			out = dfs.create(filenamePath);
			
			while ((line = br.readLine()) != null) {
				if(line.isEmpty()) continue;
				
				obj = parser.parse(line);
				JSONObject jsonObj = (JSONObject) obj;
				
				jsonObj.remove("documentId");
				jsonObj.remove("customStr2");
				jsonObj.remove("visitTime");
				jsonObj.remove("urlHash");
				jsonObj.remove("status");
				jsonObj.remove("regTime");
				jsonObj.remove("modTime");
				jsonObj.remove("collection");
				jsonObj.remove("vkDlOnly");
				
				if(media.equals("NEWS")) {
					jsonObj.remove("commentList");
					jsonObj.remove("customStr1");
					jsonObj.remove("customNum2");
					jsonObj.remove("imageLinks");
					jsonObj.remove("commentCount");
					jsonObj.remove("sourceContentUrl");
					jsonObj.remove("links");
				}
				
				if(media.equals("BLOG")) {
					jsonObj.remove("documentGroup");
					jsonObj.remove("totalVisit");
					jsonObj.remove("blogger");
					jsonObj.remove("bloggerUrl");
					jsonObj.remove("customNum1");
					jsonObj.remove("source");
				}
				
				/* href */
				if (jsonObj.containsKey("url")) {
					jsonObj.put("href", jsonObj.get("url"));
					jsonObj.remove("url");
				} else {
					jsonObj.put("href", "");
				}
				
				/* dt : yyyymmddhhmmss */
				if (jsonObj.containsKey("postTime")) {
					if(CreateFilesByDate.checkDate(jsonObj.get("postTime").toString())) {
						jsonObj.put("dt", new SimpleDateFormat("yyyyMMddHHmmss").format(
								new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(jsonObj.get("postTime").toString()))); // 20220413121200 형식으로 변환
						
					} else {
						continue;
					}
					
					jsonObj.remove("postTime");
				} else {
					continue;
				}
				
				/* content */
				if (!jsonObj.containsKey("content")) {
					continue;
				}
				
				/* title */
				if (!jsonObj.containsKey("title")) {
					continue;
				}
				
				if(media.equals("NEWS")) {
					/* press */
					if (jsonObj.containsKey("source")) {
						jsonObj.put("press", jsonObj.get("source"));
						jsonObj.remove("source");
					} else {
						jsonObj.put("press", "");
					}
					
					/* source */
					if (jsonObj.containsKey("documentGroup")) {
						// 소문자로 변경
						jsonObj.put("source", jsonObj.get("documentGroup").toString().toLowerCase().substring(0, jsonObj.get("documentGroup").toString().indexOf("_")));
						jsonObj.remove("documentGroup");
					} else {
						jsonObj.put("source", "");
					}
					
					/* sid1_name */
					if (jsonObj.containsKey("category")) {
						jsonObj.put("sid1_name", jsonObj.get("category"));
						jsonObj.remove("category");
					} else {
						jsonObj.put("sid1_name", "");
					}
					
					/* sid2_name */
					if (jsonObj.containsKey("category2")) {
						jsonObj.put("sid2_name", jsonObj.get("category2"));
						jsonObj.remove("category2");
					} else {
						jsonObj.put("sid2_name", "");
					}
					
				}
				/*
				 * StringEscapeUtils.unescapeJava 수행 시 escape 된 문자열도 처리되는 문제 발생
				 *  ex) "\"test"\" => ""test""
				 * escape 된 문자열 미리 치환 > unescapeJava 처리 > 다시 escape처리 
				 * */
				String unicodeExcept = jsonObj.toString()
						.replaceAll("\\\\\\\\\\\\\"", "unicode except double quotation convert")
						.replaceAll("\\\\\\\\\"", "\"")
						.replaceAll("\\\\\"", "unicode except double quotation convert")
						.replaceAll("\\\\n", " ");
				/*
				 *ASCII CODE
				 *0x00(0), NULL : NULL 문자
				 *0x0A(10), LF: 개행(Line Feed), 줄바꿈
				 *0x0D(13), CR: 복귀(Carriage Return)
				 *0x00~0x1F, 0x7F(0~31, 127): 제어문자 또는 비인쇄 문자
				 *0x20(32): space(공백)
				 *0x21~0x2F(33~47), 0x3A~0x40(58~64), 0x5B~0x60(91~96), 0x7B~0x7E(123~126): 특수 문자
				 *0x30~0x39(48~57): 0, 1, 3, 4, 5, 6, 7, 8, 9 숫자
				 *0x41~0x5A(65~90): A부터 Z까지 알파벳 대문자
				 *0x61~0x7A(97~122): a부터 z까지 알파벳 소문자
				 */
				try {
					out.write(StringEscapeUtils.unescapeJava(unicodeExcept + "\r\n")
							.replaceAll("[\\x00-\\x09\\x0B-\\x0F\\x10-\\x1F\\x7F]", "")
							.replaceAll("\\\\", "")
							.replaceAll("unicode except double quotation convert", "\\\\\"")
							.getBytes());
				} catch (Exception e) {
					System.out.println(line);
					e.printStackTrace();
					continue;
				}
				
			}
			System.out.println("== JSON PARSE END ==");
		} catch (IOException e) {
			System.out.println(line);
			e.printStackTrace();
		} catch (ParseException e) {
			System.out.println(line);
			e.printStackTrace();
		} catch (java.text.ParseException e) {
			System.out.println(line);
			e.printStackTrace();
		} 
//			catch (Exception e) {
//			System.out.println(line);
//			e.printStackTrace();
//		}
		finally {
			out.close();
			dfs.close();
			System.out.println("== FileSystem close("+media+") ==");
		}
		
	}

}