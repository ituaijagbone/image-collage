package awsfront;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import com.dropbox.core.DbxAuthInfo;
import com.dropbox.core.DbxClient;
import com.dropbox.core.DbxEntry;
import com.dropbox.core.DbxException;
import com.dropbox.core.DbxPath;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.DbxWriteMode;
import com.dropbox.core.json.JsonReader;
import com.dropbox.core.util.IOUtil;

public class ImageToVideo {
	// constructor - accept an array of urls to download
	String imgId;
	File dir; 
	String clientId;
	String[] url;
	public ImageToVideo(String clientId, String[] url) {
		this.clientId = clientId;
		this.url = url;
	}
	
	// create tmp folder for user
	public void createImgFolder() {
		dir = new File("img_"+clientId);
		dir.mkdir();
	}
	
	// Download pictures using wget - pass it to system command executor
	public void downloadImgs() {
		for (int i = 0; i < url.length; i++) {
			try {
				processCommand("wget " + url[i] + " -P " + dir.getAbsolutePath());
			} catch(IOException e) {
				System.out.println("IOException: Cannot download file");
			} catch(InterruptedException e) {
				System.out.println("InterruptedException: Cannot download file");
			}
		}
	}
	
	// Rename pictures using java file system
	public void renameImgs() {
		File[] allFiles = dir.listFiles();
		String extension = "jpg";
		int counter = 1;
		for (int i = 0; i < allFiles.length; i++) {
			if (allFiles[i].isDirectory() || allFiles[i].isHidden()) {
				continue;
			}
			String path = allFiles[i].getAbsolutePath().toLowerCase();
			if (path.endsWith(extension) && (path.charAt(path.length() - extension.length() - 1) == '.')) {
				File rename = new File("img"+counter+".jpg");
				boolean success = allFiles[i].renameTo(rename);
				
				if (!success) {
					System.out.println("Failed to rename file: " + allFiles[i].getName());
				} else {
					counter++;
				}
			}
		}
	}
	
	// collage images - pass it to system command executor
	public String collageImgs() {
		imgId = UUID.randomUUID().toString();
		String result = "Failed to collage images";
		try {
			processCommand("ffmpeg -loop 1 -i img%d.jpg -c:v libx264 -t "
					+ "30 -pix_fmt yuv420p" +  dir.getAbsolutePath()+"/"+imgId+".mp4");
			int rcode = saveToDropbox();
			if (rcode == 1) {
				result = "Successfully saved collaged images to video and saved to dropbox account";
			} 
		} catch(IOException e) {
			System.out.println("IOException: Cannot download file");
		} catch(InterruptedException e) {
			System.out.println("InterruptedException: Cannot download file");
		} finally {
			if (dir.exists()) {
				deleteImgFolder(dir);
			}
		}
		return result;
	}
	// save to amazon s3 return url of image to user
	
	// save to dropbox
	public int saveToDropbox() {
		DbxAuthInfo authInfo;
		String authFile = "dropbox.auth";
		String dropboxPath = "photo-collage";
		try {
			authInfo = DbxAuthInfo.Reader.readFromFile(authFile);
		} catch (JsonReader.FileLoadException ex) {
			System.err.println("Error loading <auth-file>: " + ex.getMessage());
			return 1;
		}
		String pathError = DbxPath.findError(dropboxPath);
        if (pathError != null) {
            System.err.println("Invalid <dropbox-path>: " + pathError);
            return 1;
        }
        
        String userLocale = Locale.getDefault().toString();
        DbxRequestConfig requestConfig = new DbxRequestConfig("photo-collage/1.0", userLocale);
        DbxClient dbxClient = new DbxClient(requestConfig, authInfo.accessToken, authInfo.host);
        
        DbxEntry.File metaData;
        try {
        	InputStream in = new FileInputStream(dir.getAbsoluteFile() + "/" + imgId);
        	try {
				metaData = dbxClient.uploadFile(dropboxPath, DbxWriteMode.add(), -1, in);
			} catch (DbxException e) {
				System.out.println("Error uploading to Dropbox: " + e.getMessage());
                return 1;
			} finally {
                IOUtil.closeInput(in);
            }
        } catch (IOException ex) {
            System.out.println("Error reading from file \"" + imgId + "\": " + ex.getMessage());
            return 1;
        }

        System.out.print(metaData.toStringMultiline());
        return 0;
	}
	
	public void processCommand(String cmd) throws IOException, InterruptedException {
		List<String> commands = new ArrayList<String>();
		commands.add("/bin/sh");
		commands.add("-c");
		commands.add(cmd);

		// execute the command
		SystemCommandExecutor commandExecutor = new SystemCommandExecutor(
				commands);
		int result = commandExecutor.executeCommand();

		// get the stdout and stderr from the command that was run
		StringBuilder stdout = commandExecutor.getStandardOutputFromCommand();
		StringBuilder stderr = commandExecutor.getStandardErrorFromCommand();

		// print the stdout and stderr
		System.out.println("The numeric result of the command was: " + result);
		System.out.println("STDOUT:");
		System.out.println(stdout);
		System.out.println("STDERR:");
		System.out.println(stderr);
	}
	
	public void deleteImgFolder(File file) {
		if (file.isDirectory()) {
			if (file.list().length == 0) {
				file.delete();
			} else {
				String files[] = file.list();
				for (String tmp : files) {
					File delFile = new File(file, tmp);
					deleteImgFolder(delFile);
				}
				
				if (file.list().length == 0) {
					file.delete();
				}
			}
		} else {
			file.delete();
		}
	}
}
