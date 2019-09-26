# KafkaConsumerStormThymeleaf
SpringBoot项目整合Thymeleaf与Hbase
本文内容为之前的[SpringBoot项目](https://github.com/butalways1121/SpringBoot-Kafka-Storm)与Thymeleaf引擎模板和Hbase整合成[KafaConsumerStormThymeleaf项目](https://github.com/butalways1121/KafkaConsumerStormThymeleaf.git)，最终实现的是在前端页面对mysql数据库的增删改查操作，另外，也将hbase数据库的数据展示到前端并实现新增数据功能，同时又将从Hbase数据库拿到的数据插入mysql数据库，主要的一些步骤如下。
<!-- more -->
### 一、SpringBoot项目整合Thymeleaf
**1、pom.xml中添加Thymeleaf依赖**
如下：
```bash
<dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-thymeleaf</artifactId>
        </dependency>
```
**2、创建Thymeleaf的资源文件**
Thymeleaf的资源文件是放在src/main/resources目录下，其中resources目录下的的static目录用于放置静态内容，比如css、js、jpg图片等，emplates目录用于放置项目使用的页面模板，也就是.html文件。
**3、编写Controller实现页面的跳转以及对数据库的操作**
主要代码如下，至于对数据库操作的增删改查方法就不一一列出了：
```bash
	@RequestMapping("/list")
	public String toList(Model model) {
		List<InfoGet> infos = infoGetService.findAll();
		model.addAttribute("infos",infos);
		return "user/list";
	}
	@RequestMapping("/toAdd")
	public String toAdd(Model model) {
		return "user/add";
	}
	@RequestMapping("/addUser")
	public String addUser(InfoGet infoGet) {
		infoGetService.add(infoGet);
		return "redirect:/list";
	}
	@RequestMapping("/delete")
	public String delete(int id) {
		infoGetService.delete(id);
		return "redirect:/list";
	}
	@RequestMapping("/toEdit")
	public String toEdit(Model model,int id) {
		InfoGet info = infoGetService.findById(id);
	     model.addAttribute("info", info);
		return "user/edit";
	}
	@RequestMapping("/edit")
	public String edit(InfoGet infoGet) {
		infoGetService.update(infoGet);
		return "redirect:/list";
	}
```
### 二、SpringBoot整合Hbase
**1、pom.xml中添加hbase依赖**
如下，其中jdk.tools的systemPath需改成本机的路径：
```bash
<!-- Hadoop依赖 -->
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-common</artifactId>
			<version>2.7.7</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-client</artifactId>
			<version>2.7.7</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-hdfs</artifactId>
			<version>2.7.7</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-mapreduce-client-core</artifactId>
			<version>2.7.7</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-yarn-common</artifactId>
			<version>2.7.7</version>
		</dependency>

		<!--HBase相关jar -->
		<dependency>
			<groupId>org.apache.hbase</groupId>
			<artifactId>hbase-hadoop-compat</artifactId>
			<version>1.3.1</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hbase</groupId>
			<artifactId>hbase-server</artifactId>
			<version>1.1.2</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hbase</groupId>
			<artifactId>hbase-client</artifactId>
			<version>1.1.2</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hbase</groupId>
			<artifactId>hbase-common</artifactId>
			<version>1.1.2</version>
		</dependency>

		<dependency>
			<groupId>jdk.tools</groupId>
			<artifactId>jdk.tools</artifactId>
			<version>1.8</version>
			<scope>system</scope>
			<systemPath>D:\Program Files\Java\jdk1.8.0_211/lib/tools.jar</systemPath>
		</dependency>
```
**2、添加HbaseUtil工具类**
该HbaseUtil类的作用是对于Hbase数据库进行一系列操作的方法，在其中有设置相对应的hbase的书记IP端口等等，代码如下：
```bash
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSONObject;

//HBase工具类
public class HBaseUtil {
	/** hadoop 连接 */
	private static Configuration conf = null;
	/** hbase 连接 */
	private static Connection con = null;
	/** 会话 */
	private static Admin admin = null;

	private static String ip = "master";
	private static String port = "2181";
	private static String port1 = "9001";

	// 初始化连接
	static {
		// 获得配制文件对象
		conf = HBaseConfiguration.create();
		// 设置配置参数
		conf.set("hbase.zookeeper.quorum", ip);
		conf.set("hbase.zookeeper.property.clientPort", port);
		// 如果hbase是集群，这个必须加上
		// 这个ip和端口是在hadoop/mapred-site.xml配置文件配置的
		conf.set("hbase.master", ip + ":" + port1);
	}

	/**
	 * 获取连接
	 * 
	 * @return
	 */
	public synchronized static Connection getConnection() {
		try {
			if (null == con || con.isClosed()) {
				// 获得连接对象
				con = ConnectionFactory.createConnection(conf);
			}
		} catch (IOException e) {
			System.out.println("获取连接失败!");
			e.printStackTrace();
		}

		return con;
	}

	/**
	 * 连接关闭
	 */
	public static void close() {
		try {
			if (admin != null) {
				admin.close();
			}
			if (con != null) {
				con.close();
			}
		} catch (IOException e) {
			System.out.println("连接关闭失败！");
			e.printStackTrace();
		}
	}

	/**
	 * 创建表
	 * 
	 * @param tableName    表名
	 * @param columnFamily 列族
	 */
	public static void creatTable(String tableName, String[] columnFamily) {
		if (null == tableName || tableName.length() == 0) {
			return;
		}
		if (null == columnFamily || columnFamily.length == 0) {
			return;
		}
		// 创建表名对象
		TableName tn = TableName.valueOf(tableName);
		// a.判断数据库是否存在
		try {
			// 获取会话
			admin = getConnection().getAdmin();
			if (admin.tableExists(tn)) {
				System.out.println(tableName + " 表存在，删除表....");
				// 先使表设置为不可编辑
				admin.disableTable(tn);
				// 删除表
				admin.deleteTable(tn);
				System.out.println("表删除成功.....");
			}
			// 创建表结构对象
			HTableDescriptor htd = new HTableDescriptor(tn);
			for (String str : columnFamily) {
				// 创建列族结构对象
				HColumnDescriptor hcd = new HColumnDescriptor(str);
				htd.addFamily(hcd);
			}
			// 创建表
			admin.createTable(htd);
			System.out.println(tableName + " 表创建成功！");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}

	/**
	 * 数据单条插入或更新
	 * 
	 * @param tableName 表名
	 * @param rowKey    行健 (主键)
	 * @param family    列族
	 * @param qualifier 列
	 * @param value     存入的值
	 * @return
	 * @throws IOException
	 */
	public static void insert(String tableName, String rowKey, String family, String qualifier, String value)
			throws IOException {
		admin = getConnection().getAdmin();
		// 如果表存在直接插入数据
		if (admin.tableExists(TableName.valueOf(tableName))) {
			Table t = null;
			try {
				t = getConnection().getTable(TableName.valueOf(tableName));
				Put put = new Put(Bytes.toBytes(rowKey));
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
				t.put(put);
				System.out.println(tableName + " 更新成功!");
			} catch (IOException e) {
				System.out.println(tableName + " 更新失败!");
				e.printStackTrace();
			} finally {
				close();
			}
		}
		// 如果表不存在,先根据表名和列族创建表
		else {
			HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
			// 创建列族结构对象
			HColumnDescriptor hcd = new HColumnDescriptor(family);
			htd.addFamily(hcd);
			// 创建表
			admin.createTable(htd);
			System.out.println(tableName + " 表创建成功！");
			// 插入数据
			Table t = null;
			try {
				t = getConnection().getTable(TableName.valueOf(tableName));
				Put put = new Put(Bytes.toBytes(rowKey));
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
				t.put(put);
				System.out.println(tableName + " 更新成功!");
			} catch (IOException e) {
				System.out.println(tableName + " 更新失败!");
				e.printStackTrace();
			} finally {
				close();
			}
		}
	}

	/**
	 * 数据批量插入或更新
	 * 
	 * @param tableName 表名
	 * @param list      hbase的数据
	 * @return
	 */
	public static void insertBatch(String tableName, List<?> list) {
		if (null == tableName || tableName.length() == 0) {
			return;
		}
		if (null == list || list.size() == 0) {
			return;
		}
		Table t = null;
		Put put = null;
		JSONObject json = null;
		List<Put> puts = new ArrayList<Put>();
		try {
			t = getConnection().getTable(TableName.valueOf(tableName));
			for (int i = 0, j = list.size(); i < j; i++) {
				json = (JSONObject) list.get(i);
				put = new Put(Bytes.toBytes(json.getString("rowKey")));
				put.addColumn(Bytes.toBytes(json.getString("family")), Bytes.toBytes(json.getString("qualifier")),
						Bytes.toBytes(json.getString("value")));
				puts.add(put);
			}
			t.put(puts);
			System.out.println(tableName + " 更新成功!");
		} catch (IOException e) {
			System.out.println(tableName + " 更新失败!");
			e.printStackTrace();
		} finally {
			close();
		}
	}

	/**
	 * 数据删除
	 * 
	 * @param tableName 表名
	 * @param rowKey    行健
	 * @return
	 */
	public static void delete(String tableName, String rowKey) {
		delete(tableName, rowKey, "", "");
	}

	/**
	 * 数据删除
	 * 
	 * @param tableName 表名
	 * @param rowKey    行健
	 * @param family    列族
	 * @return
	 */
	public static void delete(String tableName, String rowKey, String family) {
		delete(tableName, rowKey, family, "");
	}

	/**
	 * 数据删除
	 * 
	 * @param tableName 表名
	 * @param rowKey    行健
	 * @param family    列族
	 * @param qualifier 列
	 * @return
	 */
	public static void delete(String tableName, String rowKey, String family, String qualifier) {
		if (null == tableName || tableName.length() == 0) {
			return;
		}
		if (null == rowKey || rowKey.length() == 0) {
			return;
		}
		Table t = null;
		try {
			t = getConnection().getTable(TableName.valueOf(tableName));
			Delete del = new Delete(Bytes.toBytes(rowKey));
			// 如果列族不为空
			if (null != family && family.length() > 0) {
				// 如果列不为空
				if (null != qualifier && qualifier.length() > 0) {
					del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
				} else {
					del.addFamily(Bytes.toBytes(family));
				}
			}
			t.delete(del);
		} catch (IOException e) {
			System.out.println("删除失败!");
			e.printStackTrace();
		} finally {
			close();
		}
	}

	/**
	 * 查询该表中的所有数据
	 * 
	 * @param tableName 表名
	 * @return
	 */
	public static List<Map<String, Object>> select(String tableName) {
		if (null == tableName || tableName.length() == 0) {
			return null;
		}
		Table t = null;
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			t = getConnection().getTable(TableName.valueOf(tableName));
			// 读取操作
			Scan scan = new Scan();
			// 得到扫描的结果集
			ResultScanner rs = t.getScanner(scan);
			if (null == rs) {
				return null;
			}
			for (Result result : rs) {
				// 得到单元格集合
				List<Cell> cs = result.listCells();
				if (null == cs || cs.size() == 0) {
					continue;
				}
				for (Cell cell : cs) {
					Map<String, Object> map = new HashMap<String, Object>();
					map.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// 取行健
					map.put("timestamp", cell.getTimestamp());// 取到时间戳
					map.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));// 取到列族
					map.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));// 取到列
					map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));// 取到值
					list.add(map);
				}
			}
			System.out.println("查询的数据:" + list);
		} catch (IOException e) {
			System.out.println("查询失败!");
			e.printStackTrace();
		} finally {
			close();
		}
		return list;
	}

	/**
	 * 根据表名和行健查询
	 * 
	 * @param tableName
	 * @param rowKey
	 */
	public static void select(String tableName, String rowKey) {
		select(tableName, rowKey, "", "");
	}

	/**
	 * 根据表名、行健和列族查询
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 */
	public static void select(String tableName, String rowKey, String family) {
		select(tableName, rowKey, family, "");
	}

	/**
	 * 根据条件明细查询
	 * 
	 * @param tableName 表名
	 * @param rowKey    行健 (主键)
	 * @param family    列族
	 * @param qualifier 列
	 */
	public static void select(String tableName, String rowKey, String family, String qualifier) {
		Table t = null;
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			t = getConnection().getTable(TableName.valueOf(tableName));
			// 通过HBase中的 get来进行查询
			Get get = new Get(Bytes.toBytes(rowKey));
			// 如果列族不为空
			if (null != family && family.length() > 0) {
				// 如果列不为空
				if (null != qualifier && qualifier.length() > 0) {
					get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
				} else {
					get.addFamily(Bytes.toBytes(family));
				}
			}
			Result r = t.get(get);
			List<Cell> cs = r.listCells();
			if (null == cs || cs.size() == 0) {
				return;
			}
			for (Cell cell : cs) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// 取行健
				map.put("timestamp", cell.getTimestamp());// 取到时间戳
				map.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));// 取到列族
				map.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));// 取到列
				map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));// 取到值
				list.add(map);
			}
			System.out.println("查询的数据:" + list);
		} catch (IOException e) {
			System.out.println("查询失败!");
			e.printStackTrace();
		} finally {
			close();
		}
	}

	// 获取表名
	public static TableName[] getListTable() throws IOException {
		admin = getConnection().getAdmin();// getAdmin()检索管理实现来管理HBase集群,返回用于集群管理的管理实例
		return admin.listTableNamesByNamespace("default");// Get list of table names by namespace.return The list of
															// table names in the namespace
	}
}
```
**3、创建HbaseEntity实体类**
该实体类的属性与hbase数据库的属性相对应，主要代码如下：
```bash
public class HbaseEntity {

	private String tableName;
	private String qualifier;
	private String family;
	private String value;
	private String rowKey;
	private String timestamp;
	//省略get和set方法
}
```
**4、在控制层添加页面跳转及实现Hbase数据库操作的查询、插入**
主要代码如下，在其中比较复杂的就是将hbase数据库的表名添加给每一条数据，最后一起给赋给前端页面进行展示，同时也传给dao层进而插入到mysql数据库：
```bash
@RequestMapping("/toHbase")
	public String toHbase(Model model) throws IOException {
		TableName[] tables = HBaseUtil.getListTable();  //获取数组形式的所有表名
		List<HbaseEntity> results =new ArrayList<HbaseEntity>();  //定义一个数组形式的hbaseEntity对象result用以存储查询到的结果
		for (TableName table : tables) {//遍历表名，将每一个表名添加到select()出的数组对象
			//HBaseUtil的select()方法查询到没有表名属性的对象数组entitys，select()传入的参数是String，使用tableName自带的方法转换成String类型
			List<Map<String, Object>> entitys =  HBaseUtil.select(table.getNameAsString());
			for (Map<String, Object> entity : entitys) {//对于每一个table中entitys的每个对象，添加tableName属性的值
				String jsonString = JSON.toJSONString(entity); //先将entity转换成JsonString
				HbaseEntity parseObject = JSONObject.parseObject(jsonString,HbaseEntity.class);//在将JsonString转换成对象
				parseObject.setTableName(table.getNameAsString());//添加对象tableName属性的值
				results.add(parseObject);//将对象存储到reslut中
			}
		}
		model.addAttribute("entitys", results);
		for(HbaseEntity result:results) {
			infoGetService.addToHbase(result);
		}
		return "user/hbase";
	}
	@RequestMapping("/toHbaseInsert")
	public String toHbaseInsert(Model model) {
		return "user/hbaseInsert";
	}
	
	@RequestMapping("/insertHbase")
	public String insertHbase(HbaseEntity hbaseEntity) throws IOException {
		HBaseUtil.insert(hbaseEntity.getTableName(),hbaseEntity.getRowKey(),hbaseEntity.getFamily(), hbaseEntity.getQualifier(),hbaseEntity.getValue());
		return "redirect:/toHbase";//redirect重定向到方法
	}
```
### 三、测试
启动程序，在浏览器中输入[http://localhost:8088/list]()，主页面展示的就是mysql数据库的数据，如下图，点击编辑或者添加用户会跳转到相应的页面实现数据库的修改及插入，点击删除则直接将该条数据删除：
![](https://raw.githubusercontent.com/butalways1121/img-Blog/master/70.png)
点击跳转到Hbasey页面，就会看到hbase数据库的所有数据，如下图，同时会将展示出来的所有数据插入到数据库，点击添加信息可以进行对Hbase数据库的添加操作：
![](https://raw.githubusercontent.com/butalways1121/img-Blog/master/71.png)
## 至此，OVER！
