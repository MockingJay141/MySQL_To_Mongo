package edu.rit.ibd.a4;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.DBObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static java.util.Collections.sort;

public class IMDBSQLToMongo {

	public static void main(String[] args) throws Exception {
		final String dbURL = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String mongoDBURL = args[3];
		final String mongoDBName = args[4];

		System.out.println(new Date() + " -- Started");

		Connection con = DriverManager.getConnection(dbURL, user, pwd);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);


		// TODO 0: Your code here!

		/*
		 *
		 * Everything in MongoDB is a document (both data and queries). To create a document, I use primarily two options but there are others
		 * 	if you ask the Internet. You can use org.bson.Document as follows:
		 *
		 * 		Document d = new Document();
		 * 		d.append("name_of_the_field", value);
		 *
		 * 	The type of the field will be the conversion of the Java type of the value.
		 *
		 * 	Another option is to parse a string representing the document:
		 *
		 * 		Document d = Document.parse("{ _id:1, name:\"Name\" }");
		 *
		 * 	It will parse only well-formed documents. Note that the previous approach will use the Java data types as the types of the pieces of
		 * 		data to insert in MongoDB. However, the latter approach will not have that info as everything is a string; therefore, be mindful
		 * 		of these differences and use the approach it will fit better for you.
		 *
		 * If you wish to create an embedded document, you can use the following:
		 *
		 * 		Document outer = new Document();
		 * 		Document inner = new Document();
		 * 		outer.append("doc", inner);
		 *
		 * To connect to a MongoDB database server, use the getClient method above. If your server is local, just provide "None" as input.
		 *
		 * You must extract data from MySQL and load it into MongoDB. Note that, in general, the data in MongoDB is denormalized, which means that it includes
		 * 	redundancy. You must think of ways of extracting such redundant data in batches, that is, you should think of a bunch of queries that will retrieve
		 * 	the whole database in a format it will be convenient for you to load in MongoDB. Performing many small SQL queries will not work.
		 *
		 * If you execute a SQL query that retrieves large amounts of data, all data will be retrieved at once and stored in main memory. To avoid such behavior,
		 * 	the JDBC URL will have the following parameter: 'useCursorFetch=true' (already added by the grading software). Then, you can control the number of
		 * 	tuples that will be retrieved and stored in memory as follows:
		 *
		 * 		PreparedStatement st = con.prepareStatement("SELECT ...");
		 * 		st.setFetchSize(batchSize);
		 *
		 * where batchSize is the number of rows.
		 *
		 * Null values in MySQL must be translated as documents without such fields.
		 *
		 * Once you have composed a specific document with the data retrieved from MySQL, insert the document into the appropriate collection as follows:
		 *
		 * 		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		 *
		 * 		...
		 *
		 * 		Document d = ...
		 *
		 * 		...
		 *
		 * 		col.insertOne(d);
		 *
		 * You should focus first on inserting all the documents you need (movies and people). Once those documents are already present, you should deal with
		 * 	the mapping relations. To do so, MongoDB is optimized to make small updates of documents referenced by their keys (different than MySQL). As a
		 * 	result, it is a good idea to update one document at a time as follows:
		 *
		 * 		PreparedStatement st = con.prepareStatement("SELECT ..."); // Select from mapping table.
		 * 		st.setFetchSize(batchSize);
		 * 		ResultSet rs = st.executeQuery();
		 * 		while (rs.next()) {
		 * 			col.updateOne(Document.parse("{ _id : "+rs.get(...)+" }"), Document.parse(...));
		 * 			...
		 *
		 * The updateOne method updates one single document based on the filter criterion established in the first document (the _id of the document to fetch
		 * 	in this case). The second document provided as input is the update operation to perform. There are several updates operations you can perform (see
		 * 	https://docs.mongodb.com/v3.6/reference/operator/update/). If you wish to update arrays, $push and $addToSet are the best options but have slightly
		 * 	different semantics. Make sure you read and understand the differences between them.
		 *
		 * When dealing with arrays, another option instead of updating one by one is gathering all values for a specific document and perform a single update.
		 *
		 * Note that array fields that are empty are not allowed, so you should not generate them.
		 *
		 */


		MongoCollection<Document> col = db.getCollection("Movies");
		col.drop();

		//Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
		PreparedStatement st = con.prepareStatement("SELECT * FROM movie");
		PreparedStatement genre = con.prepareStatement("SELECT * FROM moviegenre JOIN genre ON gid = id ORDER BY mid ASC");
		st.setFetchSize(/* Batch size */ 1000);
		ResultSet rs = st.executeQuery();
		ResultSet grs = genre.executeQuery();
		grs.next();
		while (rs.next()) {

			Document d = new Document();
			if (rs.getInt("id") != 0)
				d.append("_id", rs.getInt("id"));
			if (rs.getString("otitle") != null)
				d.append("otitle", rs.getString("otitle"));
			if (rs.getString("ptitle") != null)
				d.append("ptitle", rs.getString("ptitle"));
			d.append("adult", rs.getBoolean("adult"));
			if (rs.getInt("year") != 0)
				d.append("year", rs.getInt("year"));
			if (rs.getInt("runtime") != 0)
				d.append("runtime", rs.getInt("runtime"));
			if (rs.getBigDecimal("rating") != null) {
				Decimal128 x = new Decimal128(rs.getBigDecimal("rating"));
				x.toString();
				d.append("rating", x);
			}
			if (rs.getInt("totalvotes") != 0)
				d.append("totalvotes", rs.getInt("totalvotes"));

			List<String> array = new ArrayList<>();
			if (!grs.isLast()) {
				if (rs.getInt("id") == grs.getInt("mid")) {
					while (rs.getInt("id") == grs.getInt("mid")) {
						array.add(grs.getString("name"));
						grs.next();
					}
				}
			}
			if (!array.isEmpty())
				d.put("genre", array);
			col.insertOne(d);

			// If something is NULL, then, do not include the field!

			// To deal with float attributes, use the code below to retrieve big decimals for attribute x in MySQL and create Decimal128 in MongoDB.
		}

		rs.close();
		grs.close();
		st.close();

		MongoCollection<Document> colPeople = db.getCollection("People");
		colPeople.drop();

		// Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
		PreparedStatement stPeople = con.prepareStatement("SELECT * FROM person");
		stPeople.setFetchSize(/* Batch size */ 1);
		ResultSet rsPeople = stPeople.executeQuery();
		while (rsPeople.next()) {
			Document d = new Document();
			if (rsPeople.getInt("id") != 0)
				d.append("_id", rsPeople.getInt("id"));
			if (rsPeople.getString("name") != null)
				d.append("name", rsPeople.getString("name"));
			if (rsPeople.getInt("byear") != 0)
				d.append("byear", rsPeople.getInt("byear"));
			if (rsPeople.getInt("dyear") != 0)
				d.append("dyear", rsPeople.getInt("dyear"));
			colPeople.insertOne(d);

//			// If something is NULL, then, do not include the field!
//
//			// To deal with float attributes, use the code below to retrieve big decimals for attribute x in MySQL and create Decimal128 in MongoDB.
////			Decimal128 x =  new Decimal128(rs.getBigDecimal("id"));
////			x.toString();
}
//
			rsPeople.close();
			stPeople.close();


			st = con.prepareStatement("SELECT * FROM movie");
			rs = st.executeQuery();
			while (rs.next())
				continue;
			//col.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);
			rs.close();
			st.close();


			MongoCollection<Document> colMovieDe = db.getCollection("MoviesDenorm");
			colMovieDe.drop();

			// Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
			PreparedStatement stMovieDe = con.prepareStatement("SELECT * FROM movie");
			PreparedStatement actor = con.prepareStatement("SELECT * FROM actor ORDER BY mid ASC");
			PreparedStatement director = con.prepareStatement("SELECT * FROM director ORDER BY mid ASC");
			PreparedStatement writer = con.prepareStatement("SELECT * FROM writer ORDER BY mid ASC");
			PreparedStatement producer = con.prepareStatement("SELECT * FROM producer ORDER BY mid ASC");
			stMovieDe.setFetchSize(/* Batch size */ 50000);
			ResultSet movie = stMovieDe.executeQuery();
			ResultSet a = actor.executeQuery();
			ResultSet dir = director.executeQuery();
			ResultSet w = writer.executeQuery();
			ResultSet p = producer.executeQuery();
			a.next();
			dir.next();
			w.next();
			p.next();

			while (movie.next()) {
				Document d = new Document();
				if (movie.getInt("id") != 0)
					d.append("_id", movie.getInt("id"));
				List<Integer> actorArray = new ArrayList<>();
				if (movie.getInt("id") == a.getInt("mid")) {
					while (movie.getInt("id") == a.getInt("mid")) {
						actorArray.add(a.getInt("pid"));
						if (!a.isLast())
							a.next();
						else
							break;
					}
				}
				if (!actorArray.isEmpty())
					d.put("actors", actorArray);


				List<Integer> directorArray = new ArrayList<>();
				if (movie.getInt("id") == dir.getInt("mid")) {
					while (movie.getInt("id") == dir.getInt("mid")) {
						directorArray.add(dir.getInt("pid"));
						if (!dir.isLast())
							dir.next();
						else
							break;
					}
				}

				if (!directorArray.isEmpty())
					d.put("directors", directorArray);

				List<Integer> producerArray = new ArrayList<>();
				if (movie.getInt("id") == p.getInt("mid")) {
					while (movie.getInt("id") == p.getInt("mid")) {
						producerArray.add(p.getInt("pid"));
						if (!p.isLast())
							p.next();
						else
							break;
					}
				}

				if (!producerArray.isEmpty())
					d.put("Producers", producerArray);

				List<Integer> writerArray = new ArrayList<>();
				if (movie.getInt("id") == w.getInt("mid")) {
					while (movie.getInt("id") == w.getInt("mid")) {
						writerArray.add(w.getInt("pid"));
						if (!w.isLast())
							w.next();
						else
							break;
					}
				}

				if (!writerArray.isEmpty())
					d.put("Writers", writerArray);

				colMovieDe.insertOne(d);
			}
			a.close();
			movie.close();
			stMovieDe.close();


			MongoCollection<Document> colPeopleDe = db.getCollection("PeopleDenorm");
			colPeopleDe.drop();

			// Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
			PreparedStatement stpeopleDe = con.prepareStatement("SELECT id FROM person ORDER BY id ASC");
			PreparedStatement actorForPeople = con.prepareStatement("SELECT * FROM actor ORDER BY pid ASC");
			PreparedStatement directorForPeople = con.prepareStatement("SELECT * FROM director ORDER BY pid ASC");
			PreparedStatement knownForPeople = con.prepareStatement("SELECT * FROM knownfor ORDER BY pid ASC");
			PreparedStatement writerForPeople = con.prepareStatement("SELECT * FROM writer ORDER BY pid ASC");
			PreparedStatement producerForPeople = con.prepareStatement("SELECT * FROM producer ORDER BY pid ASC");
			stpeopleDe.setFetchSize(/* Batch size */ 50000);
			ResultSet peopleForPeople = stpeopleDe.executeQuery();
			ResultSet aForPeople = actorForPeople.executeQuery();
			ResultSet dirForPeople = directorForPeople.executeQuery();
			ResultSet kForPeople = knownForPeople.executeQuery();
			ResultSet wForPeople = writerForPeople.executeQuery();
			ResultSet proForPeople = producerForPeople.executeQuery();
			aForPeople.next();
			dirForPeople.next();
			kForPeople.next();
			wForPeople.next();
			proForPeople.next();
			while (peopleForPeople.next()) {
				Document d = new Document();
				if (peopleForPeople.getInt("id") != 0)
					d.append("_id", peopleForPeople.getInt("id"));
				List<Integer> actorArray = new ArrayList<>();
				if (peopleForPeople.getInt("id") == aForPeople.getInt("pid")) {
					while (peopleForPeople.getInt("id") == aForPeople.getInt("pid")) {
						actorArray.add(aForPeople.getInt("mid"));
						if (!aForPeople.isLast())
							aForPeople.next();
						else
							break;
					}
				}
				if (!actorArray.isEmpty())
					d.put("actors", actorArray);

				List<Integer> directorArray = new ArrayList<>();
				if (peopleForPeople.getInt("id") == dirForPeople.getInt("pid")) {
					while (peopleForPeople.getInt("id") == dirForPeople.getInt("pid")) {
						directorArray.add(dirForPeople.getInt("mid"));
						if (!dirForPeople.isLast())
							dirForPeople.next();
						else
							break;
					}
				}
				if (!directorArray.isEmpty())
					d.put("directors", directorArray);

				List<Integer> knownArray = new ArrayList<>();
				if (peopleForPeople.getInt("id") == kForPeople.getInt("pid")) {
					while (peopleForPeople.getInt("id") == kForPeople.getInt("pid")) {
						knownArray.add(kForPeople.getInt("mid"));
						if (!kForPeople.isLast())
							kForPeople.next();
						else
							break;
					}
				}
				if (!knownArray.isEmpty())
					d.put("Known For", knownArray);


				List<Integer> writerArray = new ArrayList<>();
				if (peopleForPeople.getInt("id") == wForPeople.getInt("pid")) {
					while (peopleForPeople.getInt("id") == wForPeople.getInt("pid")) {
						writerArray.add(wForPeople.getInt("mid"));
						if (!wForPeople.isLast())
							wForPeople.next();
						else
							break;
					}
				}
				if (!writerArray.isEmpty())
					d.put("Writers", writerArray);

				List<Integer> producerArray = new ArrayList<>();
				if (peopleForPeople.getInt("id") == proForPeople.getInt("pid")) {
					while (peopleForPeople.getInt("id") == proForPeople.getInt("pid")) {
						producerArray.add(proForPeople.getInt("mid"));
						if (!proForPeople.isLast())
							proForPeople.next();
						else
							break;
					}
				}
				if (!producerArray.isEmpty()) {
					sort(producerArray);
					d.put("Producers", producerArray);
				}

				colPeopleDe.insertOne(d);


			}
			aForPeople.close();
			dirForPeople.close();
			wForPeople.close();
			proForPeople.close();
			peopleForPeople.close();
			// TODO 0: End of your code.

			client.close();
			con.close();
		}

		private static MongoClient getClient (String mongoDBURL){
			MongoClient client = null;
			if (mongoDBURL.equals("None"))
				client = new MongoClient();
			else
				client = new MongoClient(new MongoClientURI(mongoDBURL));
			return client;
		}

	}

