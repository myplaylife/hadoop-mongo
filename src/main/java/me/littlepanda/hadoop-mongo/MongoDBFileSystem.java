package me.littlepanda.hadoop-mongo

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoderFactory;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBDecoder;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

/****************************************************************
 * Implement the FileSystem API for the MongoDB filesystem.
 *
 * v1.0 backlog：
 * 	1）调查Hadoop抽象文件系统其他几种存储方式的使用方法并写例子
 * 	2）将不影响返回结果的操作开辟新线程执行
 * 	3）增加命名空间权限判断
 *  	- 如果目录有r-x权限，则能看到，而且能获取子文件列表；如果只有r--权限，则只能看到文件，但不能获取子文件列表。
 *  	- 文件的权限语义：
 *  		- r（read）：可读取文件的实际内容
 *  		- w（write）：可编辑、新增或修改文件内容，（不含删除权限）
 *  		- x（eXecute）：可被系统执行（MongoDBFileSystem没有此权限）
 *  	- 目录的权限语义：
 *  		- r（read contents in directory）：可以读取此目录
 *  		- w（modify contents in directory）：
 *  			- 1）建立新的文件或目录；
 *  			- 2）删除已存在的文件或目录；
 *  			- 3）更名已存在的文件或目录；
 *  			- 4）搬移目录内的文件或目录。
 *  		- x（access directory）：能否进入该目录
 * 	4）增加setOwner方法
 * 	5）增加setPermission方法
 *  6）增加dubbo服务接口实现 - 已完成
 *  7）dubbo服务接口应该使用流式接口操作
 * 
 * v2.0 backlog：
 * 	1）实现hadoop2.x版本 - 目前的0.7-SNAPSHOT已经完成
 * 	2）猜测并发追加文件可能会有问题，需要解决
 * 	3）FSDataInputStream没有统计功能，为其加上统计功能
 * 	4）加入命名空间配额功能
 * 	5）加入存储空间配额功能
 *  6）支持kerbros
 *  7）支持HARFileSystem
 *  8）压缩文件流传输
 *
 * v0.6 release note：
 *  1）创建文件、目录、文件改名时，获取当前用户所在用户组（目前可取到用户名）
 *      - 还是采用默认的实现方式，采用操作系统用户名和组
 *      - 把应用系统本身作为用户，而不是应用系统中的业务用户
 *  2）完善HadoopGridFSDBFileStream，实现字节流的定位读取和获取当前位置功能
 *      - 需要为GridFSDBFile.MyInputStrea增加mark方法的支持
 *  3）完成append方法
 *  4）为各个方法添加statistic（统计数据）
 *  5）增加权限判断
 *      - 通过mongodb用户名和密码进行权限判断。
 *  6）修改各方法中“文件是否存在”的写法。
 *  7）修改bug，delete方法，删除根路径时，无法删除子路径
 *  8）修改bug，listStatus方法，如果参数为根路径，则找不到子文件和路径
 *  9）修改bug，修改登錄用戶名和所屬組不起作用
 *      - 因为FileSystem是单例，根据schema、authority、 ugi来判断是否唯一，而我们现在的实现没有根据用户名来修改 ugi
 *      - 去掉后添加的外部设置username和group的代码，应该把应用系统作为系统用户，而不是具体的业务用户，目前只需要提供操作系统级用户即可
 *
 * 获取FileSystem实例：
 * 	FileSystem fs = FileSystem.get(new Configuration());
 * 使用本包的所有应用系统如果部署在不同的linux机器中，应用所处的用户名和所属用户组需要保持一致
 *
 *****************************************************************/
public class MongoDBFileSystem extends FileSystem {

	/*
	 * 当前工作目录
	 */
	private Path workingDir;
	/*
	 * mongodb服务资源定位
	 */
	private URI uri;
	/*
	 * mongodb地址 格式为：${host}:${port}
	 */
	private static String server;
	/*
	 * mongodb库名
	 */
	private static String db;
	/*
	 * mongodb文件系统存储名称
	 */
	private static String storage;
	/**
	 * 下面是mongodb参数设置
	 */
	private static int connectionsPerHost; // mongodb连接池大小
	private static int threadsAllowedToBlockForConnectionMultiplier; // 这个值乘以connectionPerHost等于操作等待队列大小
	private static String description; // 这个MongoClient的描述
	private static int maxWaitTime; // 等待获取连接的超时时间
	private static int connectTimeout; // 连接超时时间
	private static int socketTimeout; // Socket超时时间
	private static boolean socketKeepAlive; // 是否一直保持着socket
	private static boolean autoConnectRetry; // 是否自动重试连接
	private static long maxAutoConnectRetryTime; // 最多自动重试次数
	private static ReadPreference readPreference; // 读策略
	private static DBDecoderFactory dbDecoderFactory; // 数据库解码器工厂
	private static DBEncoderFactory dbEncoderFactory; // 数据库编码器工厂
	private static WriteConcern writeConcern; // 写策略
	private static boolean cursorFinalizerEnabled; // 客户端不关闭的情况下，DBCursor对象的finalizer方法是否可用
	private static boolean alwaysUseMBeans; // 是否一致使用MBeans，不管java的版本是java6或更高
	private static SocketFactory socketFactory; // socket工厂

	/*
	 * 登录MongoDB用户名和密码
	 */
	private static String mongodb_username;
	private static String mongodb_password;

	/*
	 * 用户默认组
	 */
	private String defaultgroup;

	static {
		Configuration.addDefaultResource("mongo-site.xml");
	}

	public MongoDBFileSystem() {
	}

	/**
	 * 初始化
	 * 	1、创建mongodb资源定位符
	 * 	2、获取mongodb连接信息
	 * 	3、设置当前工作目录（默认是/user/${currentUser}）
	 */
	public void initialize(URI uri, Configuration conf) throws IOException {

		this.uri = URI.create(conf.get("fs.defaultFS",
				"mongodb://localhost:27017/giant/storage"));
		super.initialize(uri, conf);
		setConf(conf);

		server = this.uri.getAuthority();
		if (server == null) {
			throw new IOException("Incomplete MongoDB URI, no server: " + uri);
		}

		String[] dbInfo = this.getMongoInfo(this.uri, this.uri.getPath());
		db = dbInfo[0];
		storage = dbInfo[1];

		connectionsPerHost = conf.getInt("connectionsPerHost", 10);
		threadsAllowedToBlockForConnectionMultiplier = conf.getInt("threadsAllowedToBlockForConnectionMultiplier", 5);
		description = conf.get("description", "This is Hadoop abstract Filesystem use MongoDB implement!");
		maxWaitTime = conf.getInt("maxWaitTime", 1000 * 60 * 2);
		connectTimeout = conf.getInt("connectTimeout", 1000 * 10);
		socketTimeout = conf.getInt("socketTimeout", 0);
		socketKeepAlive = conf.getBoolean("socketKeepAlive", false);
		autoConnectRetry = conf.getBoolean("autoConnectRetry", false);
		maxAutoConnectRetryTime = conf.getLong("maxAutoConnectRetryTime", 0);
		readPreference = ReadPreference.valueOf(conf.get("readPreference", "primary"));
		writeConcern = WriteConcern.valueOf(conf.get("writeConcern", "acknowledged"));
		String dbDecoderFactoryName = conf.get("dbDecoderFactory", "");
		try {
			dbDecoderFactory = (DBDecoderFactory) (dbDecoderFactoryName
					.equals("") ? DefaultDBDecoder.FACTORY : ReflectionUtils
					.newInstance(Class.forName(dbDecoderFactoryName, true, this
							.getClass().getClassLoader()), conf));
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("can't find dbDecoderFactory class"
					+ dbDecoderFactoryName);
		}
		String dbEncoderFactoryName = conf.get("dbEncoderFactory", "");
		try {
			dbEncoderFactory = (DBEncoderFactory) (dbEncoderFactoryName
					.equals("") ? DefaultDBEncoder.FACTORY : ReflectionUtils
					.newInstance(Class.forName(dbEncoderFactoryName, true, this
							.getClass().getClassLoader()), conf));
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("can't find dbDecoderFactory class"
					+ dbEncoderFactory);
		}
		socketFactory = SocketFactory.getDefault();
		cursorFinalizerEnabled = conf.getBoolean("cursorFinalizerEnabled", true);
		alwaysUseMBeans = conf.getBoolean("alwaysUseMBeans", false);

		this.workingDir = getHomeDirectory();
		this.defaultgroup = conf.get("defaultgroup", "supergroup");
		mongodb_username = conf.get("username");
		mongodb_password = conf.get("password");
	}

	/**
	 * @param uri
	 *            mongodb资源定位符
	 * @param path
	 *            /${db}/${storage}
	 * @throws IOException
	 */
	private String[] getMongoInfo(URI uri, String path) throws IOException {
		String db = path.substring(1, path.indexOf("/", 2)).trim();
		if (db == null) {
			throw new IOException("Incomplete MongoDB URI, no db: " + uri);
		}

		String storage = path.substring(path.indexOf("/", 2));
		storage = storage.replaceAll("/", " ").trim();
		if (storage == null) {
			throw new IOException("Incomplete MongoDB URI, no storage: " + uri);
		}
		return new String[] { db, storage };
	}

	/**
	 * 获取文件或目录的状态 详情参见 @org.apache.hadoop.fs.FileStatus
	 * @param f 目标文件或目录
	 *
	 */
	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		f = this.makeAbsolute(f);

		GridFSDBFile gridFile = MongoDB.getGridFS().findOne(f.toUri().getPath());
		if (gridFile == null) {
			return null;
		}
		FileStatus fileStatus = makeFileStatusQualified(gridFile);

		statistics.incrementReadOps(1);
		return fileStatus;
	}

	/**
	 * 获取一组文件或目录的状态信息
	 * @param f 如果是文件，获取文件的状态；如果是目录，获取其直接子节点的状态信息
	 *
	 */
	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		FileStatus fileStatus = this.getFileStatus(f);
		if (fileStatus == null) {
			return new FileStatus[] {};
		}
		if (fileStatus.isDir()) {
			String parent = fileStatus.getPath().toString();
			// 找到所有直接子文件和目录
			Pattern pattern = null;
			if (parent.equals(Path.SEPARATOR)) {
				pattern = Pattern.compile("^/[^/]+$");
			} else {
				pattern = Pattern.compile("^" + parent + "/[^/]+$");
			}
			List<GridFSDBFile> list = MongoDB.getGridFS().find(
					new BasicDBObject("filename", pattern));
			int size = list.size();
			if (size == 0) {
				return new FileStatus[] {};
			}
			FileStatus[] fsList = new FileStatus[size];
			for (int i = 0; i < size; i++) {
				fsList[i] = makeFileStatusQualified(list.get(i));
			}
			statistics.incrementReadOps(1);
			return fsList;
		} else {
			statistics.incrementReadOps(1);
			return new FileStatus[] { fileStatus };
		}
	}

	/*
	 * 将MongoDB查询的文件格式转换为Hadoop系统文件状态对象
	 *
	 * @param gridFile
	 *
	 * @return Hadoop文件状态
	 *
	 */
	private FileStatus makeFileStatusQualified(GridFSDBFile gridFile) {
		DBObject obj = gridFile.getMetaData();
		String permission = obj.get("permission").toString();
		FsPermission fsPermission = FsPermission.valueOf(obj.get("permission")
				.toString());
		// permission的第一位是‘d’就是目录；‘-’就是文件
		boolean isDir = permission.charAt(0) == 'd' ? true : false;
		String owner = obj.get("owner").toString();
		String group = obj.get("group").toString();

		return new FileStatus(gridFile.getLength(), isDir, -1,
				gridFile.getChunkSize(), gridFile.getUploadDate().getTime(),
				System.currentTimeMillis(), fsPermission, owner, group,
				new Path(gridFile.getFilename()));
	}

	/**
	 * 创建文件 只返回流，用户自己决定如何写入数据
	 *
	 * @param f
	 *            文件路径
	 * @param permission
	 *            权限信息
	 * @param overwrite
	 *            如果存在，是否覆盖
	 * @param bufferSize
	 *            在此无意义
	 * @param replication
	 *            在此无意义
	 * @param blockSize
	 *            chunkSize
	 * @param progress
	 *            需要自定义进度处理，在此无意义
	 */
	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		if(blockSize > GridFS.MAX_CHUNKSIZE){
			throw new RuntimeException("Input chunkSize is " + blockSize + ";the max chunkSize is " + GridFS.MAX_CHUNKSIZE);
		}
		f = this.makeAbsolute(f);

		checkFilenameValid(f.toUri().getPath());

		FileStatus fs = this.getFileStatus(f);
		if (fs != null && !overwrite) {// 原文件存在且不覆盖时
			throw new RuntimeException("file is exist!");
		}

		// 迭代创建父目录
		boolean success = this.mkdirs(f.getParent(), permission);

		GridFSInputFile inputFile;
		if (success) {
			if (fs != null && overwrite) {// 如果创建文件存在，并需要覆盖
				MongoDB.getGridFS().remove(f.toUri().getPath());
			}
			inputFile = MongoDB.getGridFS().createFile(f.toUri().getPath());
			BasicDBObject metadata = new BasicDBObject();
			metadata.put("owner", this.getCurrentUsername());
			metadata.put("group", this.getCurrentUserMainGroup());
			// 文件的权限字符串第一位是'-'
			metadata.put("permission", "-" + permission.toString());
			inputFile.setMetaData(metadata);
			inputFile.setChunkSize(blockSize);
		} else {
			throw new RuntimeException("failure create parent directory!");
		}
		return new FSDataOutputStream(inputFile.getOutputStream(), statistics, 0);
	}

	/**
	 * 文件追加内容
	 * @param f 需要追加内容的文件
	 */
	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		FileStatus fileStatus = this.getFileStatus(f);
		if(fileStatus.isDir()){
			throw new RuntimeException("can't append content to dir");
		}
		GridFSDBFile append = MongoDB.getGridFS().findOne(f.toUri().getPath());
		return new FSDataOutputStream(append.getOutputStream(), statistics, append.getLength());
	}

	/**
	 * 创建目录 如果父目录不存在，会递归创建 目录实际上是length=0；chunkSize=1的文件
	 *
	 * @param f
	 *            目标目录
	 * @param permission
	 *            权限，创建的所有父目录与最终的子目录权限相同
	 * @needmodify “/”权限需要特殊设定
	 */
	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		f = this.makeAbsolute(f);

		checkFilenameValid(f.toUri().getPath());
		FileStatus fileStatus = this.getFileStatus(f);
		if (fileStatus != null) {// 如果目录不为空，则不需创建
			return true;
		}
		boolean success = true;
		if (!f.toUri().getPath().equals(Path.SEPARATOR)) {
			// 递归创建父级目录
			success = mkdirs(f.getParent(), permission);
		}

		if (success) {// 只有父级目录创建成功才创建本级
			GridFSInputFile gif = MongoDB.getGridFS().createFile(new byte[0]);
			gif.setFilename(f.toUri().getPath());
			BasicDBObject metadata = new BasicDBObject();
			metadata.put("owner", this.getCurrentUsername());
			metadata.put("group", this.getCurrentUserMainGroup());
			// 目录的权限字符串第一位是'd'
			metadata.put("permission", "d" + permission.toString());
			gif.setMetaData(metadata);
			gif.save(1);
		}
		statistics.incrementWriteOps(1);
		return true;
	}

	/**
	 * 不推荐使用，请使用 delete(Path f, boolean recursive);
	 */
	@Override
	@Deprecated
	public boolean delete(Path f) throws IOException {
		f = this.makeAbsolute(f);
		return this.delete(f, true);
	}

	/**
	 * 删除文件或目录
	 *
	 * @param f
	 *            目标文件或目录
	 * @param recursive
	 *            是否删除子级，如果是，则删除所有子文件和目录
	 */
	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		FileStatus fileStatus = this.getFileStatus(f);
		if (fileStatus == null) {
			return false;
		}

		if (!fileStatus.isDir()) {// 如果是文件直接删除
			MongoDB.getGridFS().remove(f.toUri().getPath());
			statistics.incrementWriteOps(1);
			return true;
		} else {
			String path = f.toUri().getPath();
			Pattern pattern = null;
			if (path.equals(Path.SEPARATOR)) {// 如果要删除的是根路径
				pattern = Pattern.compile("^/.+");
			} else {
				pattern = Pattern.compile("^" + path + "/.+");
			}
			if (this.count(new BasicDBObject("filename", pattern)) == 0) { // 如果没有子文件
				MongoDB.getGridFS().remove(new BasicDBObject("filename", path));
				statistics.incrementWriteOps(1);
				return true;
			} else if (recursive) {// 可能出现要删除/xx而连/xxyy一起删除的情况，所以先删所有子目录，再删本身
				MongoDB.getGridFS().remove(new BasicDBObject("filename", pattern));
				MongoDB.getGridFS().remove(path);
				statistics.incrementWriteOps(1);
				return true;
			} else {// 如果有子文件，而且不允许删除子文件，则抛异常
				throw new RuntimeException("path " + f.toUri().getPath()
						+ " have children， can't delete!");
			}
		}
	}

	/*
	 * 根据查询条件获取文件数量
	 *
	 * @param query
	 *
	 * @return
	 */
	private long count(DBObject query) {
		statistics.incrementReadOps(1);
		return MongoDB.getGridFS().getDB().getCollection(storage + ".files").count(query);
	}

	/**
	 * 检查文件或路径是否存在
	 *
	 * @param f
	 *            源文件或路径
	 */
	@Override
	public boolean exists(Path f) throws IOException {
		GridFSDBFile gridFile = MongoDB.getGridFS().findOne(f.toUri().getPath());
		statistics.incrementReadOps(1);
		if (gridFile == null) {
			return false;
		}
		return true;
	}

	/**
	 * 返回文件系统uri
	 */
	@Override
	public URI getUri() {
		return this.uri;
	}

	/**
	 * 返回当前工作路径
	 */
	@Override
	public Path getWorkingDirectory() {
		return this.workingDir;
	}

	/**
	 * 打开一个文件，读取流的操作由用户自己完成
	 *
	 * @needupdate 支持流的定位
	 */
	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		f = this.makeAbsolute(f);
		GridFSDBFile gridFile = MongoDB.getGridFS().findOne(f.toUri().getPath());
		statistics.incrementReadOps(1);
		if (gridFile == null) {
			throw new RuntimeException("File is not exist!");
		}
		FileStatus fileStatus = this.makeFileStatusQualified(gridFile);

		if (fileStatus.isDir()) {
			throw new RuntimeException(
					"open operation must fire on file, not directory!");
		}
		// Hadoop文件流操作需要实现seekable或positionable接口，因此需要我们自定义一个流
		GridFSDBFileInputStream gridFSDBFileStream = new GridFSDBFileInputStream(
				gridFile);
		/**
		 * 这里需要实现HadoopGridFSDBFileStream的getpos、seek方法
		 */
		statistics.incrementReadOps(1);
		return new FSDataInputStream(gridFSDBFileStream);
	}

	/**
	 * 文件或目录改名
	 *
	 * @param src
	 *            原路径
	 * @param dst
	 *            目标路径
	 */
	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		src = this.makeAbsolute(src);
		dst = this.makeAbsolute(dst);

		String srcPath = src.toUri().getPath();
		String dstPath = dst.toUri().getPath();
		if (srcPath.equals(dstPath)) {// 如果原路径与目标路径相同
			return true;
		}
		if (srcPath.equals(Path.SEPARATOR)) {// 如果原路径是根路径
			throw new RuntimeException("can't rename src '/'");
		}

		if (dstPath.startsWith(srcPath)
				&& dstPath.charAt(srcPath.length()) == Path.SEPARATOR_CHAR) {// 如果目标路径是原路径的子文件夹，文件改名不在此列
			throw new RuntimeException("failed to rename " + srcPath + " to "
					+ dstPath + " because destination starts with src");
		}

		FileStatus srcStatus = this.getFileStatus(src);
		if (srcStatus == null) {
			throw new RuntimeException("src " + src.toUri().getPath()
					+ " is not exist!");
		}

		checkFilenameValid(makeAbsolute(dst).toUri().getPath());

		if (srcStatus.isDir()) {
			FileStatus dstStatus = this.getFileStatus(dst);
			if (dstStatus != null) { // 目标路径已存在
				throw new RuntimeException("dst directory " + dstPath
						+ " already exist!");
			}
			boolean success = mkdirs(dst, srcStatus.getPermission());
			if (success) {
				Pattern pattern = Pattern.compile("^" + srcPath + "/.+");
				List<GridFSDBFile> list = MongoDB.getGridFS().find(
						new BasicDBObject("filename", pattern));
				statistics.incrementReadOps(1);
				for (GridFSDBFile dstTempPath : list) { // 所有子文件和目录全部改名
					String srcFilename = dstTempPath.getFilename();
					String dstFilename = srcFilename.replaceFirst(srcPath,
							dstPath);
					this.renameGridFile(new Path(srcFilename), new Path(
							dstFilename));
				}
				// 删除原目录
				MongoDB.getGridFS().remove(srcPath);
				statistics.incrementWriteOps(1);
			}
		} else {// 如果是文件，直接修改路径名
			if (mkdirs(dst.getParent(), srcStatus.getPermission())) {// 验证目标文件的父路径是否存在，如果不存在则创建
				this.renameGridFile(src, dst);
			}
		}
		return true;
	}

	/*
	 * 文件改名
	 *
	 * @param src 原路径
	 *
	 * @param dst 目标路径
	 *
	 * @return 成功：true；失败：false。
	 */
	private boolean renameGridFile(Path src, Path dst) throws IOException {
		DBCollection filesCollection = MongoDB.getGridFS().getDB().getCollection(
				storage + ".files");
		DBObject objCondition = new BasicDBObject("filename", src.toUri()
				.getPath());
		DBObject objValue = new BasicDBObject();
		objValue.put("filename", dst.toUri().getPath());
		objValue.put("metadata.owner", this.getCurrentUsername());
		objValue.put("metadata.group", this.getCurrentUserMainGroup());
		DBObject objSetValue = new BasicDBObject("$set", objValue);
		filesCollection.update(objCondition, objSetValue, false, true);
		statistics.incrementWriteOps(1);
		return true;
	}

	/**
	 * 设置工作目录
	 */
	@Override
	public void setWorkingDirectory(Path new_dir) {
		String result = makeAbsolute(new_dir).toUri().getPath();
		checkFilenameValid(result);
		workingDir = makeAbsolute(new_dir);
	}

	/**
	 * 检查路径是否合法
	 *
	 * @param path
	 */
	public void checkFilenameValid(String path) {
		if (!DFSUtil.isValidName(path)) {
			throw new IllegalArgumentException("Invalid DFS directory name "
					+ path);
		}
	}

	/**
	 * 暂不支持获取使用空间方法
	 */
	public long getUsed() throws IOException{
	    throw new IOException("不支持此方法。");
	  }

	/*
	 * 将相对路径改成绝对路径
	 *
	 * @param f
	 *
	 * @return
	 */
	private Path makeAbsolute(Path f) {
		if (f.isAbsolute()) {
			return new Path(f.toUri().toString()).makeQualified(this);
		} else {
			return new Path(workingDir, f);
		}
	}

	/**
	 * @return 当前用户，默认是操作系统用户
	 * @throws IOException
	 */
	private String getCurrentUsername() throws IOException{
		return UserGroupInformation.getCurrentUser().getUserName();
	}

	/**
	 * @return 当前用户所在组，默认是Linux用户所在的主组，windows会返回空
	 * @throws IOException
	 */
	private String getCurrentUserMainGroup() throws IOException{
		String[] groups = UserGroupInformation.getCurrentUser().getGroupNames();
		return groups.length == 0 ? defaultgroup : groups[0];
	}

	/**
	 * MongoDB操作类
	 */
	static final class MongoDB {
		private static Log log = LogFactory.getLog(MongoDB.class);
		private static Mongo mongo;
		private static GridFS gridFS;
		static {
			synchronized (MongoDB.class) {
				try {
					MongoClientOptions mco = MongoClientOptions
							.builder()
							.connectionsPerHost(connectionsPerHost)
							.threadsAllowedToBlockForConnectionMultiplier(
									threadsAllowedToBlockForConnectionMultiplier)
							.writeConcern(writeConcern)
							.readPreference(readPreference)
							.description(description).maxWaitTime(maxWaitTime)
							.connectTimeout(connectTimeout)
							.socketTimeout(socketTimeout)
							.socketKeepAlive(socketKeepAlive)
							.autoConnectRetry(autoConnectRetry)
							.maxAutoConnectRetryTime(maxAutoConnectRetryTime)
							.dbDecoderFactory(dbDecoderFactory)
							.dbEncoderFactory(dbEncoderFactory)
							.socketFactory(socketFactory)
							.cursorFinalizerEnabled(cursorFinalizerEnabled)
							.alwaysUseMBeans(alwaysUseMBeans).build();
					mongo = new MongoClient(server, mco);
					gridFS = new GridFS(getDB(), storage);
				} catch (Exception e) {
					log.error("Connect mongodb servers failed: " + server, e);
				}
			}
		}

		static DB getDB(String dbname) {
			return mongo.getDB(dbname);
		}

		static DB getDB() {
			if(mongodb_username == null || mongodb_password == null){
				throw new RuntimeException("MongoDB op need username and password.");
			}
			DB mdb = mongo.getDB(db);
			boolean login = mdb.authenticate(mongodb_username, mongodb_password.toCharArray());
			if(!login){
				throw new RuntimeException("MongoDB authentiate failed.");
			}
			return mdb;
		}

		static GridFS getGridFS(){
			return gridFS;
		}
	}
}
