package ru.altarix;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.*;

public class App
{
    public static void main( String[] args )
    {
        MongoClient client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        MongoDatabase db = client.getDatabase("students");
        MongoCollection<Document> grades = db.getCollection("grades");

        Integer prevStudentId = null;
        MongoIterable<Document> sortedGrades = grades.find(eq("type", "homework"))
            .projection(include("student_id", "score"))
            .sort(descending("student_id", "score"));

        int removedCount = 0;
        for (Document grade : sortedGrades) {
            Integer studentId = grade.getInteger("student_id");
            if (studentId.equals(prevStudentId)) {
                grades.deleteOne(new Document("_id", grade.getObjectId("_id")));
                removedCount++;
            }

            prevStudentId = studentId;
        }

        System.out.println("Removed " + removedCount + " documents");
    }
}
