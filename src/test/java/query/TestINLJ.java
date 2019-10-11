package query;

import database.Database;
import database.DatabaseException;
import databox.*;
import index.BPlusTree;
import index.BPlusTreeException;
import table.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;

import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
//import java.lang.System;

import static org.junit.Assert.assertEquals;

public class TestINLJ {
    public static final String TestDir = "testDatabase";
    private Database db;
    private String filename;
    private File file;
    private String btree_filename = "TestBPlusTree";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.db = new Database(filename);
        this.db.deleteAllTables();
        this.file = tempFolder.newFile(btree_filename);
    }

    @After
    public void afterEach() {
        this.db.deleteAllTables();
        this.db.close();
    }

    private BPlusTree getBPlusTree(Type keySchema, int order) throws BPlusTreeException {
        return new BPlusTree(file.getAbsolutePath(), keySchema, order);
    }


    @Test
    public void testINLJ_SJoinE() throws DatabaseException, BPlusTreeException, IOException, QueryPlanException {


        // create second table
        String table1Name = "student";
        String table2Name = "enrollment";

        Database.Transaction t1 = db.beginTransaction();

        BPlusTree rightBtree = loadStudent(t1);
        loadEnrollment(t1);

        // ******************** WRITE YOUR CODE BELOW ************************
        // init INLJ Operator
        SequentialScanOperator leftSCO = new SequentialScanOperator(t1, table2Name);
        BtreeIndexScanOperator rightBTO = new BtreeIndexScanOperator(t1, table1Name, rightBtree);
        INLJOperator inljOperator = new INLJOperator(leftSCO, rightBTO, "sid", "sid", t1);

        Iterator<Record> recordIterator =  inljOperator.iterator();

        // loop and print result

        while (recordIterator.hasNext()){
            Record record = recordIterator.next();
            System.out.println(record);
        }

        // ******************** WRITE YOUR CODE ABOVE ************************
        //throw new UnsupportedOperationException("TODO: implement");


    }

    @Test
    public void testINLJ_SJoinEJoinC() throws DatabaseException, BPlusTreeException, IOException, QueryPlanException {

        // create second table
        String table1Name = "student";
        String table2Name = "enrollment";
        String table3Nmae = "course";

        Database.Transaction t1 = db.beginTransaction();

        BPlusTree studentBtree = loadStudent(t1);
        loadEnrollment(t1);


        // ******************** WRITE YOUR CODE BELOW ************************
        BPlusTree courseBtree = loadCourse(t1);

        // init BtreeIndexScanOperator
        SequentialScanOperator leftSCO = new SequentialScanOperator(t1, table2Name);
        BtreeIndexScanOperator rightBTO = new BtreeIndexScanOperator(t1, table1Name, studentBtree);

        // init INLJ Operator
        INLJOperator inljOperator = new INLJOperator(leftSCO, rightBTO, "sid", "sid", t1);

        Iterator<Record> recordIterator =  inljOperator.iterator();

        List<Record> student_enrollment = new ArrayList<>();

        // loop and print result
        while (recordIterator.hasNext()){
            Record record = recordIterator.next();
            student_enrollment.add(record);
        }
        // ******************** WRITE YOUR CODE ABOVE ************************


        // ******************** WRITE YOUR CODE BELOW ************************
        // use TestSourceOperator create a new DataSource that contains the join result
        // schema
        List<String> names = Arrays.asList("cid", "cname", "dept");
        List<Type> types = Arrays.asList(Type.intType(), Type.stringType(20), Type.stringType(20));
        Schema s = new Schema(names, types);

        TestSourceOperator sourceOperator = new TestSourceOperator(student_enrollment, s, student_enrollment.size());
        // ******************** WRITE YOUR CODE ABOVE ************************

        // ******************** WRITE YOUR CODE BELOW ************************
        // init BtreeIndexScanOperator
        BtreeIndexScanOperator courseBTO = new BtreeIndexScanOperator(t1, table3Nmae, courseBtree);

        // init INLJ
        INLJOperator inljOperator2 = new INLJOperator(sourceOperator, courseBTO, "cid", "cid", t1);

        Iterator<Record> recordIterator1 =  inljOperator2.iterator();

        // loop and print result

        while (recordIterator1.hasNext()){
            Record record = recordIterator1.next();
            System.out.println(record);
        }
        // ******************** WRITE YOUR CODE ABOVE ************************
        //throw new UnsupportedOperationException("TODO: implement");

    }

    private BPlusTree loadStudent(Database.Transaction t1) throws DatabaseException, BPlusTreeException, IOException {


        // Create student table/Schema
        List<String> names = Arrays.asList("sid", "cid", "major", "gpa");
        List<Type> types = Arrays.asList(Type.intType(), Type.stringType(20),
                Type.stringType(20), Type.floatType());
        Schema s = new Schema(names, types);

        // create b+ tree on id
        BPlusTree tree = getBPlusTree(Type.intType(), 2);

        // create table student
        String tableName = "student";
        db.createTable(s, tableName);

        // read from csv file
        List<String> studentLines = Files.readAllLines(Paths.get("students.csv"), Charset.defaultCharset());


        // add each line to record and put in a b+tree
        for (String line : studentLines) {
            String[] splits = line.split(",");
            ArrayList<DataBox> values = new ArrayList<>();

            values.add(new IntDataBox(Integer.parseInt(splits[0])));
            values.add(new StringDataBox(splits[1].trim(), 20));
            values.add(new StringDataBox(splits[2].trim(), 20));
            values.add(new FloatDataBox(Float.parseFloat(splits[3])));

            RecordId rid = t1.addRecord(tableName, values);
            tree.put(values.get(0), rid);
        }
        return tree;
        //throw new UnsupportedOperationException("TODO: implement");
    }

    private void loadEnrollment(Database.Transaction t1) throws DatabaseException, BPlusTreeException, IOException {


        // Create student table/Schema
        List<String> names = Arrays.asList("sid", "cid");
        List<Type> types = Arrays.asList(Type.intType(), Type.intType());
        Schema s = new Schema(names, types);

        // create table student
        String tableName = "enrollment";
        db.createTable(s, tableName);

        // read from csv file
        List<String> studentLines = Files.readAllLines(Paths.get("enrollments.csv"), Charset.defaultCharset());

        // add each line to record
        for (String line : studentLines) {
            String[] splits = line.split(",");
            ArrayList<DataBox> values = new ArrayList<>();

            values.add(new IntDataBox(Integer.parseInt(splits[0])));
            values.add(new IntDataBox(Integer.parseInt(splits[1])));

            t1.addRecord(tableName, values);
        }
        //throw new UnsupportedOperationException("TODO: implement");
    }

    private BPlusTree loadCourse(Database.Transaction t1) throws DatabaseException, BPlusTreeException, IOException {




        // Create student table/Schema
        List<String> names = Arrays.asList("cid", "cname", "dept");
        List<Type> types = Arrays.asList(Type.intType(), Type.stringType(20), Type.stringType(20));
        Schema s = new Schema(names, types);

        // create table student
        String tableName = "course";
        db.createTable(s, tableName);

        // create b+ tree on id
        BPlusTree tree = getBPlusTree(Type.intType(), 2);

        // read from csv file
        List<String> courseLines = Files.readAllLines(Paths.get("courses.csv"), Charset.defaultCharset());

        // add each line to record and put in a b+tree
        for (String line : courseLines) {
            String[] splits = line.split(",");
            ArrayList<DataBox> values = new ArrayList<>();

            values.add(new IntDataBox(Integer.parseInt(splits[0])));
            values.add(new StringDataBox(splits[1].trim(), 20));
            values.add(new StringDataBox(splits[2].trim(), 20));

            RecordId rid = t1.addRecord(tableName, values);
            tree.put(values.get(0), rid);
        }

        return tree;
        //throw new UnsupportedOperationException("TODO: implement");

        }
    }



