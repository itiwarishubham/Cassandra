package praadis.test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import praadis.util.CassandraConnector;

public class Create {


	static void create(){
	Session session=CassandraConnector.getSession();
 StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
  .append("praadis.test").append("(") .append("id uuid PRIMARY KEY, ")
 .append("title text,") .append("subject text);");
 
  String query = sb.toString(); session.execute(query);
}
	
	static void insert()
	{
		Session session=CassandraConnector.getSession();
		 StringBuilder sb = new StringBuilder("INSERT INTO praadis.books (id,title) VALUES (uuid(),'ganesha3');");
		 
		 String query = sb.toString();
		 session.execute(query);
	
	}
	
	static void update()
	{
		Session session=CassandraConnector.getSession();
		 StringBuilder sb = new StringBuilder("UPDATE praadis.books SET title='Lord Ganesha';");
		 
		 String query = sb.toString();
		 session.execute(query);
	
	}
	
	static void delete()
	{
		Session session=CassandraConnector.getSession();
		 StringBuilder sb = new StringBuilder("DROP TABLE praadis.test;");
		 String query = sb.toString();
		 session.execute(query);
	
	}
	
	static void select()
	{
		Session session=CassandraConnector.getSession();
		 StringBuilder sb = new StringBuilder("select * from praadis.books;");
		 
		 String query = sb.toString();
		ResultSet rs= session.execute(query);
		System.out.println(rs.getExecutionInfo());
		
		 rs.forEach(r -> {
		
		 System.out.println(r.getUUID("id")+"  |  "+r.getString(1)+"  |  " +r.getString(2));
		 
		 });
		
	}
public static void main(final String[] args) {
	//create();
	//insert();
	//select();
	//delete();
	update();
}
}
