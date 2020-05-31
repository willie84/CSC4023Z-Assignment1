
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Java meets MongoDB
 *
 */
public class Mongodb {

    public static void main( String[] args ) {

        String MONGODB_HOST = "localhost";
        int MONGODB_PORT = 27017;

        try {

            /**
             *  Connect to MongoDB
             */
            MongoClient mongo = new MongoClient(MONGODB_HOST, MONGODB_PORT);


            /**
             * Get DB
             *
             * If it doesn't exist, MongoDB will create it
             */
            MongoDatabase db = mongo.getDatabase("TvShows");

            /**
             * Get Collection
             *
             * If it doesn't exist, MongoDB will create it
             */
            MongoCollection<Document>  collection = db.getCollection("Shows");
            MongoCollection<Document>  collection2 = db.getCollection("Episodes");


           // MongoCursor<Document> cursor = collection.find().iterator();
            try {
              // System.out.println("here");
                /**
                 * READ OPERATION
                 */

                FindIterable<Document> iterDoc = collection.find().limit(5);
               // FindIterable<Document> iterDoc2 = collection2.find(new Document().append("runtime",new Document().append("$gte", "30").append("runtime",new Document("$lte", "60"))));
                List<DBObject> criteria = new ArrayList<DBObject>();
                criteria.add(new BasicDBObject("runtime", new BasicDBObject("$gte", 30)));
                criteria.add(new BasicDBObject("runtime", new BasicDBObject("$lte", 60)));
                criteria.add(new BasicDBObject("premiered", new BasicDBObject("$eq", "2015-06-24")));

                FindIterable<Document> iterDoc2 = collection.find(new BasicDBObject("$and", criteria));
                int i = 1;int i2=0;
                // Getting the iterator
                Iterator it = iterDoc.iterator();
                Iterator it2 = iterDoc2.iterator();
                while (it.hasNext()) {
                   it.next();
                    i++;
                }
                System.out.println("-----------------------------------------------------------------------");
                System.out.println("-----------------------READ Query----------------------------------------");
                System.out.println("Checking the show whose runtime is less than 60 but greater than 30 and premeired on \"2015-06-24\" : ");
                System.out.println(".................Database read request sent......Loading...............loading.............loading........ ");
                while (it2.hasNext()) {
                    i2++;
                    System.out.println(it2.next().toString());

                }
                System.out.println("-------------------------------------------------------------------------");
                System.out.println(" ");
                System.out.println("-----------------------UPDATE Query----------------------------------------");
                System.out.println("UPDATING NAME OF EPISODES with name eps1.0_hellofriend.mov to Black Window: ");
                System.out.println(".................Database update request sent......Loading...............loading.............loading........ ");
                Document query = new Document();
                query.append("name","eps1.0_hellofriend.mov");
                Document setData = new Document();
                setData.append("name" ,"Black Widow");
                Document update = new Document();
                update.append("$set", setData);
                collection2.updateMany(query, update);
                List<DBObject> criteria2= new ArrayList<DBObject>();
                criteria2.add(new BasicDBObject("name", new BasicDBObject("$eq", "Black Widow")));

                FindIterable<Document> iterDoc3= collection2.find(new BasicDBObject("$and", criteria2));
                Iterator it3 = iterDoc3.iterator();
                while (it3.hasNext()) {

                    System.out.println(it3.next().toString());

                }
                System.out.println("-------------------------------------------------------------------------");
                System.out.println(" ");
                System.out.println("-----------------------CREATE Query----------------------------------------");
                System.out.println("Creating two new Episodes for the Better Call Saul Show: ");
                System.out.println(".................Database create request sent......Loading...............loading.............loading........ ");
                Document episode = new Document("_id", new ObjectId());
                Document episode2 = new Document("_id", new ObjectId());
                List <Document> docs=new ArrayList<>();
                episode.append("id","109598").append("showname","Better Call Saul").append("url","http://www.tvmaze.com/episodes/109598/better-call-saul")
                        .append("season",1).append("number",11).append("airdate","2015-04-16").append(
                                "runtime",60);
                episode2.append("id","1000000").append("showname","Better Call Saul").append("url","http://www.tvmaze.com/episodes/109599/better-call-saul")
                        .append("season",1).append("number",12).append("airdate","2015-04-23").append(
                        "runtime",60);
                docs.add(episode2);docs.add(episode);
                collection2.insertMany(docs);

                List<DBObject> criteria3= new ArrayList<DBObject>();
                criteria3.add(new BasicDBObject("id", new BasicDBObject("$eq", "1000000")));

                FindIterable<Document> iterDoc4= collection2.find(new BasicDBObject("$and", criteria3));
                Iterator it4 = iterDoc4.iterator();
                while (it4.hasNext()) {

                    System.out.println(it4.next().toString());

                }
                System.out.println("-------------------------------------------------------------------------");
                System.out.println(" ");

                System.out.println("-----------------------DELETE Query----------------------------------------");
                System.out.println("Deleting Tvshow with name Narcos: ");
                System.out.println(".................Database delete request sent......Loading...............loading.............loading........ ");
                BasicDBObject theQuery = new BasicDBObject();
                theQuery.put("name", "Narcos");
                DeleteResult result = collection.deleteMany(theQuery);
                System.out.println("The Numbers of Deleted Document(s) : " + result.getDeletedCount());
                List<DBObject> criteria4= new ArrayList<DBObject>();
                criteria4.add(new BasicDBObject("name", new BasicDBObject("$eq", "Narcos")));

                FindIterable<Document> iterDoc5= collection.find(new BasicDBObject("$and", criteria4));
                Iterator it5 = iterDoc5.iterator();
                int in2=0;
                while (it5.hasNext()) {

                    System.out.println(it5.next().toString());
                    in2++;

                }
                System.out.println("Confirming documents have been deleted by making a read query: " );
                System.out.println("Shows Documents found with Name Narcos are :"  +in2);
            } finally {
               System.out.println("");
            }

            System.out.println("Database operations ran sucessfully");


        } catch (MongoException e) {
            e.printStackTrace();
        }

    }
}