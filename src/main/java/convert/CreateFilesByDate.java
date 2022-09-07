package convert;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * 수집기 데이터 날짜별 파일 생성
 * 
 *  YYYYMMDDHHMMSI.json Read
 *  Create Merge File (MEDIA.YYYYMMDD.json)
 *  Create Files By date (MEDIA.MERGE.YYYYMMDD.json)
 *  Delete Merge File (MEDIA.YYYYMMDD.json)
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
public class CreateFilesByDate {

	private static BufferedReader br = null;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	private static Calendar c1 = Calendar.getInstance();	
	private static String strToday = "";
	private static String yesterDay = "";
	private static String dataFilePath = "/data/crawler-dump/";
	private static String readFilePath = dataFilePath + strToday + "/";
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		System.out.println("== CREATE FILE BY DATE START ==");
		
		Calendar c2 = Calendar.getInstance();
		c2.add(Calendar.DATE, -1);
		yesterDay = sdf.format(c2.getTime());
		
		if(args.length > 0) {
			for (int i = 0; i < args.length; i++) {
				strToday = args[i];
				readFilePath = dataFilePath+strToday+"/";
				createFiles();
			}
		}else {
			strToday = sdf.format(c1.getTime());
			createFiles();
		}
		System.out.println("== CREATE FILE BY DATE END ==");
	}

	public static void createFiles() throws Exception {
		String[] media = {"NEWS", "BLOG"};
		for (int i = 0; i < media.length; i++) {
			fileReadSFTP(readFilePath, media[i] , strToday);
			splitCreateFilesByDate(readFilePath + media[i] + "." + strToday + ".txt",media[i],strToday);
			File fileExists = new File(readFilePath+media[i]+".MERGE."+sdf.format(c1.getTime())+".txt");
			if(fileExists.exists()) {
				fileExists.delete();
			}
		}
	}

	public static void splitCreateFilesByDate(String mergeFileName, String media, String strToday) throws FileNotFoundException {
		System.out.println("== SPLIT FILE START ==");
		
		InputStream inputStream = new FileInputStream(mergeFileName);
		br = new BufferedReader(new InputStreamReader(inputStream));

		JSONParser parser = new JSONParser();
		Object obj;
		String line = ""; // 텍스트 파일 한줄씩 담을 객체

		BufferedOutputStream stream = null;

		File file = null;
		try {
			while ((line = br.readLine()) != null) {
				if (line.isEmpty())
					continue;
				obj = parser.parse(line);
				JSONObject jsonObj = (JSONObject) obj;

				String dt = null;

				if (jsonObj.containsKey("postTime")) {
					if (checkDate(jsonObj.get("postTime").toString())) {
						dt = new SimpleDateFormat("yyyyMMdd").format(
								new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(jsonObj.get("postTime").toString()));
						if(Integer.parseInt(dt) < Integer.parseInt(yesterDay)) continue;
//						if(Integer.parseInt(dt) < Integer.parseInt(sdf.format(c1.getTime()))-1) continue;
					} else {
						continue;
					}
				} else {
					continue;
				}
				
				file = new File(dataFilePath + dt + "/");
				if (!file.exists()) {
					System.out.println("CREATE DIR : " + dataFilePath + dt + "/");
					file.mkdirs();
				}
				
				stream = new BufferedOutputStream(new FileOutputStream(dataFilePath + dt + "/" +media+".MERGE."+ dt + ".txt", true));
				String data = jsonObj.toString() + "\r\n";
				stream.write(data.getBytes());
				stream.close();
			}
			File mergefile = new File(mergeFileName);
			mergefile.delete();
			System.out.println("== SPLIT FILE END ==");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			resetReader();
		}
	}

	public static void fileReadSFTP(String pathDir, String media, String strToday) throws Exception {
		System.out.println("== READ FILE START ==");
		
		final String source = media + "." + strToday;
		
		File dir = new File(pathDir);
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File f, String name) {
				return name.startsWith(source);
			}
		};

		File files[] = dir.listFiles(filter);
		String fileList[] = new String[files.length];
		for (int i = 0; i < files.length; i++) {
			System.out.println("file: " + files[i]);
			fileList[i] = files[i].toString();
		}
		merge(pathDir + source + ".txt", media, fileList, strToday);

		System.out.println("== READ FILE END ==");
	}

	public static void merge(String newFileName, String meida, String[] filenames, String strToday) throws Exception {
		
		System.out.println("== MERGE FILE START ==");
		
		FileOutputStream fout = null;
		FileInputStream in = null;
		BufferedInputStream bis = null;

		try {

			File newFile = new File(newFileName);
			fout = new FileOutputStream(newFile);
			for (String fileName : filenames) {
//				if(fileName.contains("000000.txt")) continue; //YYYYMMDD000000.txt은 다음날 read
				File splittedFile = new File(fileName);
				in = new FileInputStream(splittedFile);
				bis = new BufferedInputStream(in);
				int len = 0;
				byte[] buf = new byte[2048];
				while ((len = bis.read(buf, 0, 2048)) != -1) {
					fout.write(buf, 0, len);
				}
				fout.write('\r');
				fout.write('\n');
			}
			/**
			 * 
			 * 23:30~00:00시 데이터는 다음날로 수집되어 다음날 000000시에 등록된 데이터도 병합
			 * 다음날 YYYYMMDD000000.txt Read
			 */
//			String tomorrow = String.valueOf(Integer.parseInt(strToday)+1);
//			File fileExists = new File(dataFilePath+tomorrow+"/"+meida+"."+tomorrow+"000000.txt");
//			if(fileExists.exists()) {
//				in = new FileInputStream(fileExists);
//				bis = new BufferedInputStream(in);
//				int len = 0;
//				byte[] buf = new byte[2048];
//				while ((len = bis.read(buf, 0, 2048)) != -1) {
//					fout.write(buf, 0, len);
//				}
//				fout.write('\r');
//				fout.write('\n');
//			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;

		} finally {
			try {
				fout.close();
				in.close();
				bis.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}

		}
		
		System.out.println("== MERGE FILE END ==");

	}

	public static void resetReader() {
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

	public static boolean checkDate(String checkDate) {
		try {
			SimpleDateFormat dateFormatParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 검증할 날짜 포맷 설정
			dateFormatParser.setLenient(false); // false일경우 처리시 입력한 값이 잘못된 형식일 시 오류가 발생
			dateFormatParser.parse(checkDate); // 대상 값 포맷에 적용되는지 확인
			return true;
		} catch (Exception e) {
			return false;
		}
	}

}