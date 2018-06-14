package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class TestJoinOperator {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test(timeout=5000)
  public void testOperatorSchema() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    List<String> expectedSchemaNames = new ArrayList<String>();
    expectedSchemaNames.add("bool");
    expectedSchemaNames.add("int");
    expectedSchemaNames.add("string");
    expectedSchemaNames.add("float");
    expectedSchemaNames.add("bool");
    expectedSchemaNames.add("int");
    expectedSchemaNames.add("string");
    expectedSchemaNames.add("float");

    List<Type> expectedSchemaTypes = new ArrayList<Type>();
    expectedSchemaTypes.add(Type.boolType());
    expectedSchemaTypes.add(Type.intType());
    expectedSchemaTypes.add(Type.stringType(5));
    expectedSchemaTypes.add(Type.floatType());
    expectedSchemaTypes.add(Type.boolType());
    expectedSchemaTypes.add(Type.intType());
    expectedSchemaTypes.add(Type.stringType(5));
    expectedSchemaTypes.add(Type.floatType());

    Schema expectedSchema = new Schema(expectedSchemaNames, expectedSchemaTypes);

    assertEquals(expectedSchema, joinOperator.getOutputSchema());
  }

  @Test(timeout=5000)
  public void testSimpleJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  public void testEmptyJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator leftSourceOperator = new TestSourceOperator();

    List<Integer> values = new ArrayList<Integer>();
    TestSourceOperator rightSourceOperator = TestUtils.createTestSourceOperatorWithInts(values);
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();

    assertFalse(outputIterator.hasNext());
  }

  @Test(timeout=5000)
  public void testSimpleJoinPNLJ() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new PNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  public void testSimpleJoinBNLJ() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new BNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);

    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }




  @Test(timeout=10000)
  public void testSimplePNLJOutputOrder() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath());
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataBox> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataBox> r2Vals = r2.getValues();

    List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
    for (int i = 0; i < 2; i++) {
      for (DataBox val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataBox val: r2Vals) {
        expectedRecordValues2.add(val);
      }
    }

    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

    for (int i = 0; i < 288; i++) {
      List<DataBox> vals;
      if (i < 144) {
        vals = r1Vals;
      } else {
        vals = r2Vals;
      }
      transaction.addRecord("leftTable", vals);
      transaction.addRecord("rightTable", vals);
    }

    for (int i = 0; i < 288; i++) {
      if (i < 144) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new PNLJOperator(s1, s2, "int", "int", transaction);

    int count = 0;
    Iterator<Record> outputIterator = joinOperator.iterator();

    while (outputIterator.hasNext()) {
      if (count < 20736) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*2) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*3) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*4) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*5) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*6) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*7) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else {
        assertEquals(expectedRecord1, outputIterator.next());
      }
      count++;
    }

    assertTrue(count == 165888);
  }

  @Test(timeout=5000)
  public void testSimpleSortMergeJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SortMergeOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataBox> expectedRecordValues = new ArrayList<DataBox>();
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    expectedRecordValues.add(new BoolDataBox(true));
    expectedRecordValues.add(new IntDataBox(1));
    expectedRecordValues.add(new StringDataBox("abcde", 5));
    expectedRecordValues.add(new FloatDataBox(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }


  @Test(timeout=10000)
  public void testSortMergeJoinUnsortedInputs() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataBox> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataBox> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataBox> r3Vals = r3.getValues();
    Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
    List<DataBox> r4Vals = r4.getValues();
    List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();

    for (int i = 0; i < 2; i++) {
      for (DataBox val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataBox val: r2Vals) {
        expectedRecordValues2.add(val);
      }
      for (DataBox val: r3Vals) {
        expectedRecordValues3.add(val);
      }
      for (DataBox val: r4Vals) {
        expectedRecordValues4.add(val);
      }
    }
    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    Record expectedRecord3 = new Record(expectedRecordValues3);
    Record expectedRecord4 = new Record(expectedRecordValues4);
    List<Record> leftTableRecords = new ArrayList<>();
    List<Record> rightTableRecords = new ArrayList<>();
    for (int i = 0; i < 288*2; i++) {
      Record r;
      if (i % 4 == 0) {
        r = r1;
      } else if (i % 4  == 1) {
        r = r2;
      } else if (i % 4  == 2) {
        r = r3;
      } else {
        r = r4;
      }
      leftTableRecords.add(r);
      rightTableRecords.add(r);
    }
    Collections.shuffle(leftTableRecords, new Random(10));
    Collections.shuffle(rightTableRecords, new Random(20));
    for (int i = 0; i < 288*2; i++) {
      transaction.addRecord("leftTable", leftTableRecords.get(i).getValues());
      transaction.addRecord("rightTable", rightTableRecords.get(i).getValues());
    }


    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");

    JoinOperator joinOperator = new SortMergeOperator(s1, s2, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;
    Record expectedRecord;


    while (outputIterator.hasNext()) {
      if (numRecords < (288*288/4)) {
        expectedRecord = expectedRecord1;
      } else if (numRecords < (288*288/2)) {
        expectedRecord = expectedRecord2;
      } else if (numRecords < 288*288 - (288*288/4)) {
        expectedRecord = expectedRecord3;
      } else {
        expectedRecord = expectedRecord4;
      }
      Record r = outputIterator.next();
      assertEquals(expectedRecord, r);
      numRecords++;
    }

    assertEquals(288*288, numRecords);
  }




  @Test(timeout=10000)
  public void testBNLJDiffOutPutThanPNLJ() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 4);
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataBox> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataBox> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataBox> r3Vals = r3.getValues();
    Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
    List<DataBox> r4Vals = r4.getValues();
    List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
    List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();

    for (int i = 0; i < 2; i++) {
      for (DataBox val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataBox val: r2Vals) {
        expectedRecordValues2.add(val);
      }
      for (DataBox val: r3Vals) {
        expectedRecordValues3.add(val);
      }
      for (DataBox val: r4Vals) {
        expectedRecordValues4.add(val);
      }
    }
    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    Record expectedRecord3 = new Record(expectedRecordValues3);
    Record expectedRecord4 = new Record(expectedRecordValues4);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
    for (int i = 0; i < 2*288; i++) {
      if (i < 144) {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r3Vals);
      } else if (i < 288) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r4Vals);
      } else if (i < 432) {
        transaction.addRecord("leftTable", r3Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else {
        transaction.addRecord("leftTable", r4Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }
    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new BNLJOperator(s1, s2, "int", "int", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    int count = 0;
    while (outputIterator.hasNext()) {
      Record r = outputIterator.next();
      if (count < 144 * 144) {
        assertEquals(expectedRecord3, r);
      } else if (count < 2 * 144 * 144) {
        assertEquals(expectedRecord4, r);
      } else if (count < 3 * 144 * 144) {
        assertEquals(expectedRecord1, r);
      } else {
        assertEquals(expectedRecord2, r);
      }
      count++;
    }
    assertTrue(count == 82944);

  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////
  //Student-developed test-cases

  @Test
  public void sortMergeLectureExampleSimple() throws QueryPlanException, DatabaseException, IOException {
    //See Slide 17, Lecture 12
    //for full example

    //Create our temp database
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();

    //Create our first table
    List<String> tableOneSchemaNames = new ArrayList<String>();
    tableOneSchemaNames.add("sid");
    tableOneSchemaNames.add("sname");
    List<Type> tableOneSchemaTypes = new ArrayList<Type>();
    tableOneSchemaTypes.add(Type.intType());
    tableOneSchemaTypes.add(Type.stringType(5));
    Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
    d.createTable(tableOneSchema, "leftTable");

    //Create our second table
    List<String> tableTwoSchemaNames = new ArrayList<String>();
    tableTwoSchemaNames.add("sid");
    tableTwoSchemaNames.add("bid");
    List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
    tableTwoSchemaTypes.add(Type.intType());
    tableTwoSchemaTypes.add(Type.intType());
    Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
    d.createTable(tableTwoSchema, "rightTable");

    //Add values to first table
    List<DataBox> line1 = new ArrayList<DataBox>();
    line1.add(new IntDataBox(28));
    line1.add(new StringDataBox("yuppy", 5));
    transaction.addRecord("leftTable", line1);

    //Add values to second table
    List<DataBox> line = new ArrayList<DataBox>();
    line.add(new IntDataBox(28));
    line.add(new IntDataBox(103));
    transaction.addRecord("rightTable", line);

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new SortMergeOperator(s1, s2, "sid", "sid", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertEquals("[28, yuppy, 28, 103]", outputIterator.next().toString());
    assertFalse(outputIterator.hasNext());
  }

  @Test
  public void diffColumnMergeNamesSortMerge() throws QueryPlanException, DatabaseException, IOException {
    //See Slide 17, Lecture 12
    //for full example

    //Create our temp database
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();

    //Create our first table
    List<String> tableOneSchemaNames = new ArrayList<String>();
    tableOneSchemaNames.add("sid");
    tableOneSchemaNames.add("sname");
    List<Type> tableOneSchemaTypes = new ArrayList<Type>();
    tableOneSchemaTypes.add(Type.intType());
    tableOneSchemaTypes.add(Type.stringType(5));
    Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
    d.createTable(tableOneSchema, "leftTable");

    //Create our second table
    List<String> tableTwoSchemaNames = new ArrayList<String>();
    tableTwoSchemaNames.add("studentid");
    tableTwoSchemaNames.add("bid");
    List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
    tableTwoSchemaTypes.add(Type.intType());
    tableTwoSchemaTypes.add(Type.intType());
    Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
    d.createTable(tableTwoSchema, "rightTable");

    //Add values to first table
    List<DataBox> line1 = new ArrayList<DataBox>();
    line1.add(new IntDataBox(28));
    line1.add(new StringDataBox("yuppy", 5));
    transaction.addRecord("leftTable", line1);

    //Add values to second table
    List<DataBox> line = new ArrayList<DataBox>();
    line.add(new IntDataBox(28));
    line.add(new IntDataBox(103));
    transaction.addRecord("rightTable", line);

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new SortMergeOperator(s1, s2, "sid", "studentid", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertEquals("[28, yuppy, 28, 103]", outputIterator.next().toString());
    assertFalse(outputIterator.hasNext());
  }

  @Test
  public void sortMergeLectureExampleFull() throws QueryPlanException, DatabaseException, IOException {
    //See Slide 17, Lecture 12
    //for full example

    //Create our temp database
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();

    //Create our first table
    List<String> tableOneSchemaNames = new ArrayList<String>();
    tableOneSchemaNames.add("sid");
    tableOneSchemaNames.add("sname");
    List<Type> tableOneSchemaTypes = new ArrayList<Type>();
    tableOneSchemaTypes.add(Type.intType());
    tableOneSchemaTypes.add(Type.stringType(7));
    Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
    d.createTable(tableOneSchema, "leftTable");

    //Create our second table
    List<String> tableTwoSchemaNames = new ArrayList<String>();
    tableTwoSchemaNames.add("sid");
    tableTwoSchemaNames.add("bid");
    List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
    tableTwoSchemaTypes.add(Type.intType());
    tableTwoSchemaTypes.add(Type.intType());
    Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
    d.createTable(tableTwoSchema, "rightTable");

    //Add values to first table
    List<DataBox> line1 = new ArrayList<DataBox>();
    line1.add(new IntDataBox(22));
    line1.add(new StringDataBox("dustin", 7));
    List<DataBox> line2 = new ArrayList<DataBox>();
    line2.add(new IntDataBox(28));
    line2.add(new StringDataBox("yuppy", 7));
    List<DataBox> line3 = new ArrayList<DataBox>();
    line3.add(new IntDataBox(31));
    line3.add(new StringDataBox("lubber", 7));
    List<DataBox> line4 = new ArrayList<DataBox>();
    line4.add(new IntDataBox(31));
    line4.add(new StringDataBox("lubber2", 7));
    List<DataBox> line5 = new ArrayList<DataBox>();
    line5.add(new IntDataBox(44));
    line5.add(new StringDataBox("guppy", 7));
    List<DataBox> line6 = new ArrayList<DataBox>();
    line6.add(new IntDataBox(58));
    line6.add(new StringDataBox("rusty", 7));
    transaction.addRecord("leftTable", line1);
    transaction.addRecord("leftTable", line2);
    transaction.addRecord("leftTable", line3);
    transaction.addRecord("leftTable", line4);
    transaction.addRecord("leftTable", line5);
    transaction.addRecord("leftTable", line6);

    //Add values to second table
    List<DataBox> line7 = new ArrayList<DataBox>();
    line7.add(new IntDataBox(28));
    line7.add(new IntDataBox(103));
    List<DataBox> line8 = new ArrayList<DataBox>();
    line8.add(new IntDataBox(28));
    line8.add(new IntDataBox(104));
    List<DataBox> line9 = new ArrayList<DataBox>();
    line9.add(new IntDataBox(31));
    line9.add(new IntDataBox(101));
    List<DataBox> line10 = new ArrayList<DataBox>();
    line10.add(new IntDataBox(31));
    line10.add(new IntDataBox(102));
    List<DataBox> line11 = new ArrayList<DataBox>();
    line11.add(new IntDataBox(42));
    line11.add(new IntDataBox(142));
    List<DataBox> line12 = new ArrayList<DataBox>();
    line12.add(new IntDataBox(58));
    line12.add(new IntDataBox(107));
    transaction.addRecord("rightTable", line7);
    transaction.addRecord("rightTable", line8);
    transaction.addRecord("rightTable", line9);
    transaction.addRecord("rightTable", line10);
    transaction.addRecord("rightTable", line11);
    transaction.addRecord("rightTable", line12);

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new SortMergeOperator(s1, s2, "sid", "sid", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertEquals("[28, yuppy  , 28, 103]", outputIterator.next().toString());
    assertEquals("[28, yuppy  , 28, 104]", outputIterator.next().toString());
    assertEquals("[31, lubber , 31, 101]", outputIterator.next().toString());
    assertEquals("[31, lubber , 31, 102]", outputIterator.next().toString());
    assertEquals("[31, lubber2, 31, 101]", outputIterator.next().toString());
    assertEquals("[31, lubber2, 31, 102]", outputIterator.next().toString());
    assertEquals("[58, rusty  , 58, 107]", outputIterator.next().toString());
    assertFalse(outputIterator.hasNext());
  }

  @Test
  public void EmptySortMerge() throws QueryPlanException, DatabaseException, IOException {
    //Add no values and see if we get the right exception.
    try {
      File tempDir = tempFolder.newFolder("joinTest");
      Database d = new Database(tempDir.getAbsolutePath(), 3);
      Database.Transaction transaction = d.beginTransaction();

      //Create our first table
      List<String> tableOneSchemaNames = new ArrayList<String>();
      tableOneSchemaNames.add("sid");
      tableOneSchemaNames.add("sname");
      List<Type> tableOneSchemaTypes = new ArrayList<Type>();
      tableOneSchemaTypes.add(Type.intType());
      tableOneSchemaTypes.add(Type.stringType(7));
      Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
      d.createTable(tableOneSchema, "leftTable");

      //Create our second table
      List<String> tableTwoSchemaNames = new ArrayList<String>();
      tableTwoSchemaNames.add("sid");
      tableTwoSchemaNames.add("bid");
      List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
      tableTwoSchemaTypes.add(Type.intType());
      tableTwoSchemaTypes.add(Type.intType());
      Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
      d.createTable(tableTwoSchema, "rightTable");
      QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
      QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
      QueryOperator joinOperator = new SortMergeOperator(s1, s2, "sid", "sid", transaction);
      Iterator<Record> outputIterator = joinOperator.iterator();
      outputIterator.next();

      throw new AssertionError("Failed to throw NoSuchElementException");
    } catch (NoSuchElementException e) {
      //We successfully caught the exception.
      //Do nothing.
    }
  }

  @Test
  public void BNLJLectureExampleFull() throws QueryPlanException, DatabaseException, IOException {
    //See Slide 17, Lecture 12
    //for full example

    //Create our temp database
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();

    //Create our first table
    List<String> tableOneSchemaNames = new ArrayList<String>();
    tableOneSchemaNames.add("sid");
    tableOneSchemaNames.add("sname");
    List<Type> tableOneSchemaTypes = new ArrayList<Type>();
    tableOneSchemaTypes.add(Type.intType());
    tableOneSchemaTypes.add(Type.stringType(7));
    Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
    d.createTable(tableOneSchema, "leftTable");

    //Create our second table
    List<String> tableTwoSchemaNames = new ArrayList<String>();
    tableTwoSchemaNames.add("sid");
    tableTwoSchemaNames.add("bid");
    List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
    tableTwoSchemaTypes.add(Type.intType());
    tableTwoSchemaTypes.add(Type.intType());
    Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
    d.createTable(tableTwoSchema, "rightTable");

    //Add values to first table
    List<DataBox> line1 = new ArrayList<DataBox>();
    line1.add(new IntDataBox(22));
    line1.add(new StringDataBox("dustin", 7));
    List<DataBox> line2 = new ArrayList<DataBox>();
    line2.add(new IntDataBox(28));
    line2.add(new StringDataBox("yuppy", 7));
    List<DataBox> line3 = new ArrayList<DataBox>();
    line3.add(new IntDataBox(31));
    line3.add(new StringDataBox("lubber", 7));
    List<DataBox> line4 = new ArrayList<DataBox>();
    line4.add(new IntDataBox(31));
    line4.add(new StringDataBox("lubber2", 7));
    List<DataBox> line5 = new ArrayList<DataBox>();
    line5.add(new IntDataBox(44));
    line5.add(new StringDataBox("guppy", 7));
    List<DataBox> line6 = new ArrayList<DataBox>();
    line6.add(new IntDataBox(58));
    line6.add(new StringDataBox("rusty", 7));
    transaction.addRecord("leftTable", line1);
    transaction.addRecord("leftTable", line2);
    transaction.addRecord("leftTable", line3);
    transaction.addRecord("leftTable", line4);
    transaction.addRecord("leftTable", line5);
    transaction.addRecord("leftTable", line6);

    //Add values to second table
    List<DataBox> line7 = new ArrayList<DataBox>();
    line7.add(new IntDataBox(28));
    line7.add(new IntDataBox(103));
    List<DataBox> line8 = new ArrayList<DataBox>();
    line8.add(new IntDataBox(28));
    line8.add(new IntDataBox(104));
    List<DataBox> line9 = new ArrayList<DataBox>();
    line9.add(new IntDataBox(31));
    line9.add(new IntDataBox(101));
    List<DataBox> line10 = new ArrayList<DataBox>();
    line10.add(new IntDataBox(31));
    line10.add(new IntDataBox(102));
    List<DataBox> line11 = new ArrayList<DataBox>();
    line11.add(new IntDataBox(42));
    line11.add(new IntDataBox(142));
    List<DataBox> line12 = new ArrayList<DataBox>();
    line12.add(new IntDataBox(58));
    line12.add(new IntDataBox(107));
    transaction.addRecord("rightTable", line7);
    transaction.addRecord("rightTable", line8);
    transaction.addRecord("rightTable", line9);
    transaction.addRecord("rightTable", line10);
    transaction.addRecord("rightTable", line11);
    transaction.addRecord("rightTable", line12);

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new BNLJOperator(s1, s2, "sid", "sid", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertEquals("[28, yuppy  , 28, 103]", outputIterator.next().toString());
    assertEquals("[28, yuppy  , 28, 104]", outputIterator.next().toString());
    assertEquals("[31, lubber , 31, 101]", outputIterator.next().toString());
    assertEquals("[31, lubber , 31, 102]", outputIterator.next().toString());
    assertEquals("[31, lubber2, 31, 101]", outputIterator.next().toString());
    assertEquals("[31, lubber2, 31, 102]", outputIterator.next().toString());
    assertEquals("[58, rusty  , 58, 107]", outputIterator.next().toString());
    assertFalse(outputIterator.hasNext());
  }

  @Test
  public void SNLJLectureExampleFull() throws QueryPlanException, DatabaseException, IOException {
    //See Slide 17, Lecture 12
    //for full example

    //Create our temp database
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();

    //Create our first table
    List<String> tableOneSchemaNames = new ArrayList<String>();
    tableOneSchemaNames.add("sid");
    tableOneSchemaNames.add("sname");
    List<Type> tableOneSchemaTypes = new ArrayList<Type>();
    tableOneSchemaTypes.add(Type.intType());
    tableOneSchemaTypes.add(Type.stringType(7));
    Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
    d.createTable(tableOneSchema, "leftTable");

    //Create our second table
    List<String> tableTwoSchemaNames = new ArrayList<String>();
    tableTwoSchemaNames.add("sid");
    tableTwoSchemaNames.add("bid");
    List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
    tableTwoSchemaTypes.add(Type.intType());
    tableTwoSchemaTypes.add(Type.intType());
    Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
    d.createTable(tableTwoSchema, "rightTable");

    //Add values to first table
    List<DataBox> line1 = new ArrayList<DataBox>();
    line1.add(new IntDataBox(22));
    line1.add(new StringDataBox("dustin", 7));
    List<DataBox> line2 = new ArrayList<DataBox>();
    line2.add(new IntDataBox(28));
    line2.add(new StringDataBox("yuppy", 7));
    List<DataBox> line3 = new ArrayList<DataBox>();
    line3.add(new IntDataBox(31));
    line3.add(new StringDataBox("lubber", 7));
    List<DataBox> line4 = new ArrayList<DataBox>();
    line4.add(new IntDataBox(31));
    line4.add(new StringDataBox("lubber2", 7));
    List<DataBox> line5 = new ArrayList<DataBox>();
    line5.add(new IntDataBox(44));
    line5.add(new StringDataBox("guppy", 7));
    List<DataBox> line6 = new ArrayList<DataBox>();
    line6.add(new IntDataBox(58));
    line6.add(new StringDataBox("rusty", 7));
    transaction.addRecord("leftTable", line1);
    transaction.addRecord("leftTable", line2);
    transaction.addRecord("leftTable", line3);
    transaction.addRecord("leftTable", line4);
    transaction.addRecord("leftTable", line5);
    transaction.addRecord("leftTable", line6);

    //Add values to second table
    List<DataBox> line7 = new ArrayList<DataBox>();
    line7.add(new IntDataBox(28));
    line7.add(new IntDataBox(103));
    List<DataBox> line8 = new ArrayList<DataBox>();
    line8.add(new IntDataBox(28));
    line8.add(new IntDataBox(104));
    List<DataBox> line9 = new ArrayList<DataBox>();
    line9.add(new IntDataBox(31));
    line9.add(new IntDataBox(101));
    List<DataBox> line10 = new ArrayList<DataBox>();
    line10.add(new IntDataBox(31));
    line10.add(new IntDataBox(102));
    List<DataBox> line11 = new ArrayList<DataBox>();
    line11.add(new IntDataBox(42));
    line11.add(new IntDataBox(142));
    List<DataBox> line12 = new ArrayList<DataBox>();
    line12.add(new IntDataBox(58));
    line12.add(new IntDataBox(107));
    transaction.addRecord("rightTable", line7);
    transaction.addRecord("rightTable", line8);
    transaction.addRecord("rightTable", line9);
    transaction.addRecord("rightTable", line10);
    transaction.addRecord("rightTable", line11);
    transaction.addRecord("rightTable", line12);

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new SNLJOperator(s1, s2, "sid", "sid", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertEquals("[28, yuppy  , 28, 103]", outputIterator.next().toString());
    assertEquals("[28, yuppy  , 28, 104]", outputIterator.next().toString());
    assertEquals("[31, lubber , 31, 101]", outputIterator.next().toString());
    assertEquals("[31, lubber , 31, 102]", outputIterator.next().toString());
    assertEquals("[31, lubber2, 31, 101]", outputIterator.next().toString());
    assertEquals("[31, lubber2, 31, 102]", outputIterator.next().toString());
    assertEquals("[58, rusty  , 58, 107]", outputIterator.next().toString());
    assertFalse(outputIterator.hasNext());
  }

  @Test
  public void PNLJLectureExampleFull() throws QueryPlanException, DatabaseException, IOException {
    //See Slide 17, Lecture 12
    //for full example

    //Create our temp database
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();

    //Create our first table
    List<String> tableOneSchemaNames = new ArrayList<String>();
    tableOneSchemaNames.add("sid");
    tableOneSchemaNames.add("sname");
    List<Type> tableOneSchemaTypes = new ArrayList<Type>();
    tableOneSchemaTypes.add(Type.intType());
    tableOneSchemaTypes.add(Type.stringType(7));
    Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
    d.createTable(tableOneSchema, "leftTable");

    //Create our second table
    List<String> tableTwoSchemaNames = new ArrayList<String>();
    tableTwoSchemaNames.add("sid");
    tableTwoSchemaNames.add("bid");
    List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
    tableTwoSchemaTypes.add(Type.intType());
    tableTwoSchemaTypes.add(Type.intType());
    Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
    d.createTable(tableTwoSchema, "rightTable");

    //Add values to first table
    List<DataBox> line1 = new ArrayList<DataBox>();
    line1.add(new IntDataBox(22));
    line1.add(new StringDataBox("dustin", 7));
    List<DataBox> line2 = new ArrayList<DataBox>();
    line2.add(new IntDataBox(28));
    line2.add(new StringDataBox("yuppy", 7));
    List<DataBox> line3 = new ArrayList<DataBox>();
    line3.add(new IntDataBox(31));
    line3.add(new StringDataBox("lubber", 7));
    List<DataBox> line4 = new ArrayList<DataBox>();
    line4.add(new IntDataBox(31));
    line4.add(new StringDataBox("lubber2", 7));
    List<DataBox> line5 = new ArrayList<DataBox>();
    line5.add(new IntDataBox(44));
    line5.add(new StringDataBox("guppy", 7));
    List<DataBox> line6 = new ArrayList<DataBox>();
    line6.add(new IntDataBox(58));
    line6.add(new StringDataBox("rusty", 7));
    transaction.addRecord("leftTable", line1);
    transaction.addRecord("leftTable", line2);
    transaction.addRecord("leftTable", line3);
    transaction.addRecord("leftTable", line4);
    transaction.addRecord("leftTable", line5);
    transaction.addRecord("leftTable", line6);

    //Add values to second table
    List<DataBox> line7 = new ArrayList<DataBox>();
    line7.add(new IntDataBox(28));
    line7.add(new IntDataBox(103));
    List<DataBox> line8 = new ArrayList<DataBox>();
    line8.add(new IntDataBox(28));
    line8.add(new IntDataBox(104));
    List<DataBox> line9 = new ArrayList<DataBox>();
    line9.add(new IntDataBox(31));
    line9.add(new IntDataBox(101));
    List<DataBox> line10 = new ArrayList<DataBox>();
    line10.add(new IntDataBox(31));
    line10.add(new IntDataBox(102));
    List<DataBox> line11 = new ArrayList<DataBox>();
    line11.add(new IntDataBox(42));
    line11.add(new IntDataBox(142));
    List<DataBox> line12 = new ArrayList<DataBox>();
    line12.add(new IntDataBox(58));
    line12.add(new IntDataBox(107));
    transaction.addRecord("rightTable", line7);
    transaction.addRecord("rightTable", line8);
    transaction.addRecord("rightTable", line9);
    transaction.addRecord("rightTable", line10);
    transaction.addRecord("rightTable", line11);
    transaction.addRecord("rightTable", line12);

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new PNLJOperator(s1, s2, "sid", "sid", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertEquals("[28, yuppy  , 28, 103]", outputIterator.next().toString());
    assertEquals("[28, yuppy  , 28, 104]", outputIterator.next().toString());
    assertEquals("[31, lubber , 31, 101]", outputIterator.next().toString());
    assertEquals("[31, lubber , 31, 102]", outputIterator.next().toString());
    assertEquals("[31, lubber2, 31, 101]", outputIterator.next().toString());
    assertEquals("[31, lubber2, 31, 102]", outputIterator.next().toString());
    assertEquals("[58, rusty  , 58, 107]", outputIterator.next().toString());
    assertFalse(outputIterator.hasNext());
  }

  @Test
  public void diffColumnMergeNamesBNLJ() throws QueryPlanException, DatabaseException, IOException {
    //See Slide 17, Lecture 12
    //for full example

    //Create our temp database
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();

    //Create our first table
    List<String> tableOneSchemaNames = new ArrayList<String>();
    tableOneSchemaNames.add("sid");
    tableOneSchemaNames.add("sname");
    List<Type> tableOneSchemaTypes = new ArrayList<Type>();
    tableOneSchemaTypes.add(Type.intType());
    tableOneSchemaTypes.add(Type.stringType(5));
    Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
    d.createTable(tableOneSchema, "leftTable");

    //Create our second table
    List<String> tableTwoSchemaNames = new ArrayList<String>();
    tableTwoSchemaNames.add("studentid");
    tableTwoSchemaNames.add("bid");
    List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
    tableTwoSchemaTypes.add(Type.intType());
    tableTwoSchemaTypes.add(Type.intType());
    Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
    d.createTable(tableTwoSchema, "rightTable");

    //Add values to first table
    List<DataBox> line1 = new ArrayList<DataBox>();
    line1.add(new IntDataBox(28));
    line1.add(new StringDataBox("yuppy", 5));
    transaction.addRecord("leftTable", line1);

    //Add values to second table
    List<DataBox> line = new ArrayList<DataBox>();
    line.add(new IntDataBox(28));
    line.add(new IntDataBox(103));
    transaction.addRecord("rightTable", line);

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new BNLJOperator(s1, s2, "sid", "studentid", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertEquals("[28, yuppy, 28, 103]", outputIterator.next().toString());
    assertFalse(outputIterator.hasNext());
  }

  @Test
  public void diffColumnMergeNamesPNLJ() throws QueryPlanException, DatabaseException, IOException {
    //See Slide 17, Lecture 12
    //for full example

    //Create our temp database
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();

    //Create our first table
    List<String> tableOneSchemaNames = new ArrayList<String>();
    tableOneSchemaNames.add("sid");
    tableOneSchemaNames.add("sname");
    List<Type> tableOneSchemaTypes = new ArrayList<Type>();
    tableOneSchemaTypes.add(Type.intType());
    tableOneSchemaTypes.add(Type.stringType(5));
    Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
    d.createTable(tableOneSchema, "leftTable");

    //Create our second table
    List<String> tableTwoSchemaNames = new ArrayList<String>();
    tableTwoSchemaNames.add("studentid");
    tableTwoSchemaNames.add("bid");
    List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
    tableTwoSchemaTypes.add(Type.intType());
    tableTwoSchemaTypes.add(Type.intType());
    Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
    d.createTable(tableTwoSchema, "rightTable");

    //Add values to first table
    List<DataBox> line1 = new ArrayList<DataBox>();
    line1.add(new IntDataBox(28));
    line1.add(new StringDataBox("yuppy", 5));
    transaction.addRecord("leftTable", line1);

    //Add values to second table
    List<DataBox> line = new ArrayList<DataBox>();
    line.add(new IntDataBox(28));
    line.add(new IntDataBox(103));
    transaction.addRecord("rightTable", line);

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new PNLJOperator(s1, s2, "sid", "studentid", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertEquals("[28, yuppy, 28, 103]", outputIterator.next().toString());
    assertFalse(outputIterator.hasNext());
  }

  @Test
  public void diffColumnMergeNamesSNLJ() throws QueryPlanException, DatabaseException, IOException {
    //See Slide 17, Lecture 12
    //for full example

    //Create our temp database
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();

    //Create our first table
    List<String> tableOneSchemaNames = new ArrayList<String>();
    tableOneSchemaNames.add("sid");
    tableOneSchemaNames.add("sname");
    List<Type> tableOneSchemaTypes = new ArrayList<Type>();
    tableOneSchemaTypes.add(Type.intType());
    tableOneSchemaTypes.add(Type.stringType(5));
    Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
    d.createTable(tableOneSchema, "leftTable");

    //Create our second table
    List<String> tableTwoSchemaNames = new ArrayList<String>();
    tableTwoSchemaNames.add("studentid");
    tableTwoSchemaNames.add("bid");
    List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
    tableTwoSchemaTypes.add(Type.intType());
    tableTwoSchemaTypes.add(Type.intType());
    Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
    d.createTable(tableTwoSchema, "rightTable");

    //Add values to first table
    List<DataBox> line1 = new ArrayList<DataBox>();
    line1.add(new IntDataBox(28));
    line1.add(new StringDataBox("yuppy", 5));
    transaction.addRecord("leftTable", line1);

    //Add values to second table
    List<DataBox> line = new ArrayList<DataBox>();
    line.add(new IntDataBox(28));
    line.add(new IntDataBox(103));
    transaction.addRecord("rightTable", line);

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new SNLJOperator(s1, s2, "sid", "studentid", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertEquals("[28, yuppy, 28, 103]", outputIterator.next().toString());
    assertFalse(outputIterator.hasNext());
  }

  @Test
  public void EmptyBNLJ() throws QueryPlanException, DatabaseException, IOException {
    //Add no values and see if we get the right exception.
    try {
      File tempDir = tempFolder.newFolder("joinTest");
      Database d = new Database(tempDir.getAbsolutePath(), 3);
      Database.Transaction transaction = d.beginTransaction();

      //Create our first table
      List<String> tableOneSchemaNames = new ArrayList<String>();
      tableOneSchemaNames.add("sid");
      tableOneSchemaNames.add("sname");
      List<Type> tableOneSchemaTypes = new ArrayList<Type>();
      tableOneSchemaTypes.add(Type.intType());
      tableOneSchemaTypes.add(Type.stringType(7));
      Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
      d.createTable(tableOneSchema, "leftTable");

      //Create our second table
      List<String> tableTwoSchemaNames = new ArrayList<String>();
      tableTwoSchemaNames.add("sid");
      tableTwoSchemaNames.add("bid");
      List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
      tableTwoSchemaTypes.add(Type.intType());
      tableTwoSchemaTypes.add(Type.intType());
      Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
      d.createTable(tableTwoSchema, "rightTable");
      QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
      QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
      QueryOperator joinOperator = new BNLJOperator(s1, s2, "sid", "sid", transaction);
      Iterator<Record> outputIterator = joinOperator.iterator();
      outputIterator.next();

      throw new AssertionError("Failed to throw NoSuchElementException");
    } catch (NoSuchElementException e) {
      //We successfully caught the exception.
      //Do nothing.
    }
  }

  @Test
  public void EmptyPNLJ() throws QueryPlanException, DatabaseException, IOException {
    //Add no values and see if we get the right exception.
    try {
      File tempDir = tempFolder.newFolder("joinTest");
      Database d = new Database(tempDir.getAbsolutePath(), 3);
      Database.Transaction transaction = d.beginTransaction();

      //Create our first table
      List<String> tableOneSchemaNames = new ArrayList<String>();
      tableOneSchemaNames.add("sid");
      tableOneSchemaNames.add("sname");
      List<Type> tableOneSchemaTypes = new ArrayList<Type>();
      tableOneSchemaTypes.add(Type.intType());
      tableOneSchemaTypes.add(Type.stringType(7));
      Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
      d.createTable(tableOneSchema, "leftTable");

      //Create our second table
      List<String> tableTwoSchemaNames = new ArrayList<String>();
      tableTwoSchemaNames.add("sid");
      tableTwoSchemaNames.add("bid");
      List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
      tableTwoSchemaTypes.add(Type.intType());
      tableTwoSchemaTypes.add(Type.intType());
      Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
      d.createTable(tableTwoSchema, "rightTable");
      QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
      QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
      QueryOperator joinOperator = new PNLJOperator(s1, s2, "sid", "sid", transaction);
      Iterator<Record> outputIterator = joinOperator.iterator();
      outputIterator.next();

      throw new AssertionError("Failed to throw NoSuchElementException");
    } catch (NoSuchElementException e) {
      //We successfully caught the exception.
      //Do nothing.
    }
  }

  @Test
  public void EmptySNLJ() throws QueryPlanException, DatabaseException, IOException {
    //Add no values and see if we get the right exception.
    try {
      File tempDir = tempFolder.newFolder("joinTest");
      Database d = new Database(tempDir.getAbsolutePath(), 3);
      Database.Transaction transaction = d.beginTransaction();

      //Create our first table
      List<String> tableOneSchemaNames = new ArrayList<String>();
      tableOneSchemaNames.add("sid");
      tableOneSchemaNames.add("sname");
      List<Type> tableOneSchemaTypes = new ArrayList<Type>();
      tableOneSchemaTypes.add(Type.intType());
      tableOneSchemaTypes.add(Type.stringType(7));
      Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
      d.createTable(tableOneSchema, "leftTable");

      //Create our second table
      List<String> tableTwoSchemaNames = new ArrayList<String>();
      tableTwoSchemaNames.add("sid");
      tableTwoSchemaNames.add("bid");
      List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
      tableTwoSchemaTypes.add(Type.intType());
      tableTwoSchemaTypes.add(Type.intType());
      Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
      d.createTable(tableTwoSchema, "rightTable");
      QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
      QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
      QueryOperator joinOperator = new SNLJOperator(s1, s2, "sid", "sid", transaction);
      Iterator<Record> outputIterator = joinOperator.iterator();
      outputIterator.next();

      throw new AssertionError("Failed to throw NoSuchElementException");
    } catch (NoSuchElementException e) {
      //We successfully caught the exception.
      //Do nothing.
    }
  }

  @Test
  public void sortMergeLectureExampleFullAndShuffled() throws QueryPlanException, DatabaseException, IOException {
    //See Slide 17, Lecture 12
    //for full example

    //Same as original, but the data entries are shuffled up before table insertion.
    //Check to ensure that it is sorting the tables properly before merging.

    //Create our temp database
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();

    //Create our first table
    List<String> tableOneSchemaNames = new ArrayList<String>();
    tableOneSchemaNames.add("sid");
    tableOneSchemaNames.add("sname");
    List<Type> tableOneSchemaTypes = new ArrayList<Type>();
    tableOneSchemaTypes.add(Type.intType());
    tableOneSchemaTypes.add(Type.stringType(7));
    Schema tableOneSchema = new Schema(tableOneSchemaNames, tableOneSchemaTypes);
    d.createTable(tableOneSchema, "leftTable");

    //Create our second table
    List<String> tableTwoSchemaNames = new ArrayList<String>();
    tableTwoSchemaNames.add("sid");
    tableTwoSchemaNames.add("bid");
    List<Type> tableTwoSchemaTypes = new ArrayList<Type>();
    tableTwoSchemaTypes.add(Type.intType());
    tableTwoSchemaTypes.add(Type.intType());
    Schema tableTwoSchema = new Schema(tableTwoSchemaNames, tableTwoSchemaTypes);
    d.createTable(tableTwoSchema, "rightTable");

    //Add values to first table
    List<DataBox> line1 = new ArrayList<DataBox>();
    line1.add(new IntDataBox(22));
    line1.add(new StringDataBox("dustin", 7));
    List<DataBox> line2 = new ArrayList<DataBox>();
    line2.add(new IntDataBox(28));
    line2.add(new StringDataBox("yuppy", 7));
    List<DataBox> line3 = new ArrayList<DataBox>();
    line3.add(new IntDataBox(31));
    line3.add(new StringDataBox("lubber", 7));
    List<DataBox> line4 = new ArrayList<DataBox>();
    line4.add(new IntDataBox(31));
    line4.add(new StringDataBox("lubber2", 7));
    List<DataBox> line5 = new ArrayList<DataBox>();
    line5.add(new IntDataBox(44));
    line5.add(new StringDataBox("guppy", 7));
    List<DataBox> line6 = new ArrayList<DataBox>();
    line6.add(new IntDataBox(58));
    line6.add(new StringDataBox("rusty", 7));
    transaction.addRecord("leftTable", line6);
    transaction.addRecord("leftTable", line3);
    transaction.addRecord("leftTable", line1);
    transaction.addRecord("leftTable", line5);
    transaction.addRecord("leftTable", line2);
    transaction.addRecord("leftTable", line4);

    //Add values to second table
    List<DataBox> line7 = new ArrayList<DataBox>();
    line7.add(new IntDataBox(28));
    line7.add(new IntDataBox(103));
    List<DataBox> line8 = new ArrayList<DataBox>();
    line8.add(new IntDataBox(28));
    line8.add(new IntDataBox(104));
    List<DataBox> line9 = new ArrayList<DataBox>();
    line9.add(new IntDataBox(31));
    line9.add(new IntDataBox(101));
    List<DataBox> line10 = new ArrayList<DataBox>();
    line10.add(new IntDataBox(31));
    line10.add(new IntDataBox(102));
    List<DataBox> line11 = new ArrayList<DataBox>();
    line11.add(new IntDataBox(42));
    line11.add(new IntDataBox(142));
    List<DataBox> line12 = new ArrayList<DataBox>();
    line12.add(new IntDataBox(58));
    line12.add(new IntDataBox(107));
    transaction.addRecord("rightTable", line11);
    transaction.addRecord("rightTable", line7);
    transaction.addRecord("rightTable", line8);
    transaction.addRecord("rightTable", line12);
    transaction.addRecord("rightTable", line10);
    transaction.addRecord("rightTable", line9);

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new SortMergeOperator(s1, s2, "sid", "sid", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertEquals("[28, yuppy  , 28, 103]", outputIterator.next().toString());
    assertEquals("[28, yuppy  , 28, 104]", outputIterator.next().toString());
    assertEquals("[31, lubber , 31, 102]", outputIterator.next().toString());
    assertEquals("[31, lubber , 31, 101]", outputIterator.next().toString());
    assertEquals("[31, lubber2, 31, 102]", outputIterator.next().toString());
    assertEquals("[31, lubber2, 31, 101]", outputIterator.next().toString());
    assertEquals("[58, rusty  , 58, 107]", outputIterator.next().toString());
    assertFalse(outputIterator.hasNext());
  }

}
