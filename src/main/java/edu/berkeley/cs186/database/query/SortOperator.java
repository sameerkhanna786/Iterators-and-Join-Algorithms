package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.Page;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.ArrayBacktrackingIterator;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.common.Bits;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.io.PageAllocator.PageIterator;

import java.util.*;


public class SortOperator  {
  private Database.Transaction transaction;
  private String tableName;
  private Comparator<Record> comparator;
  private Schema operatorSchema;
  private int numBuffers;

  public SortOperator(Database.Transaction transaction, String tableName, Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
    this.transaction = transaction;
    this.tableName = tableName;
    this.comparator = comparator;
    this.operatorSchema = this.computeSchema();
    this.numBuffers = this.transaction.getNumMemoryPages();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }


  public class Run {
    private String tempTableName;

    public Run() throws DatabaseException {
      this.tempTableName = SortOperator.this.transaction.createTempTable(SortOperator.this.operatorSchema);
    }

    public void addRecord(List<DataBox> values) throws DatabaseException {
      SortOperator.this.transaction.addRecord(this.tempTableName, values);
    }

    public void addRecords(List<Record> records) throws DatabaseException {
      for (Record r: records) {
        this.addRecord(r.getValues());
      }
    }

    public Iterator<Record> iterator() throws DatabaseException {
      return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
    }

    public String tableName() {
      return this.tempTableName;
    }
  }


  /**
   * Returns a NEW run that is the sorted version of the input run.
   * Can do an in memory sort over all the records in this run
   * using one of Java's built-in sorting methods.
   * Note: Don't worry about modifying the original run.
   * Returning a new run would bring one extra page in memory beyond the
   * size of the buffer, but it is done this way for ease.
   */
  public Run sortRun(Run run) throws DatabaseException {
    //throw new UnsupportedOperationException("hw3: TODO");
    while (run != null) {
      Run r = new Run();
      List<Record> records = new ArrayList<>();
      Iterator<Record> rec = run.iterator();
      while (rec.hasNext()) {
        records.add(rec.next());
      }
      records.sort(comparator);
      r.addRecords(records);
      return r;
    }
    return null;
  }

  /**
   * Given a list of sorted runs, returns a new run that is the result
   * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
   * to determine which record should be should be added to the output run next.
   * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
   * where a Pair (r, i) is the Record r with the smallest value you are
   * sorting on currently unmerged from run i.
   */
  public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
    //throw new UnsupportedOperationException("hw3: TODO");
    Queue<Pair<Record, Integer>> recordPairPQ = new PriorityQueue<>(((pair1, pair2) -> new RecordPairComparator().compare(pair1, pair2)));
    List<Iterator<Record>> recordIterator = new ArrayList<>();
    int i = 0;

    // Prime the queue with the first element from each run
    for (Run r : runs) {
      recordIterator.add(i, r.iterator());

      if (recordIterator.get(i).hasNext()) {
        recordPairPQ.add(new Pair<>(recordIterator.get(i).next(), i));
        i++;
      }
    }
    Run r = new Run();
    while (recordPairPQ.size() > 0) {
      Pair<Record, Integer> nextRecord = recordPairPQ.remove();
      r.addRecord(nextRecord.getFirst().getValues());
      if (recordIterator.get(nextRecord.getSecond()).hasNext()) {
        recordPairPQ.add(new Pair<>(recordIterator.get(nextRecord.getSecond()).next(),
                nextRecord.getSecond()));
      }
    }

    return r;
  }

  /**
   * Given a list of N sorted runs, returns a list of
   * sorted runs that is the result of merging (numBuffers - 1)
   * of the input runs at a time.
   */
  public List<Run> mergePass(List<Run> runs) throws DatabaseException {
    List<Run> out = new ArrayList<>();
    int stepSize = numBuffers - 1;

    for (int i = 0; i < runs.size(); i += stepSize) {
      out.add(mergeSortedRuns(runs.subList(i, i+stepSize)));
    }

    return out;
  }


  /**
   * Does an external merge sort on the table with name tableName
   * using numBuffers.
   * Returns the name of the table that backs the final run.
   */
  public String sort() throws DatabaseException {
    BacktrackingIterator<Page> pageIter = transaction.getPageIterator(tableName);
    pageIter.next(); // consume header page.
    List<Run> sortedRuns = new ArrayList<>();

    while (pageIter.hasNext()) {
      Iterator<Record> bIter =
              transaction.getBlockIterator(tableName, pageIter, numBuffers-1);
      List<Record> records = new ArrayList<>();
      while (bIter.hasNext()) {
        records.add(bIter.next());
      }
      Run unsortedRun = new Run() {{
        addRecords(records);
      }};

      sortedRuns.add(sortRun(unsortedRun));
    }

    while (sortedRuns.size() > 1) {
      sortedRuns = mergePass(sortedRuns);
    }

    return sortedRuns.get(0).tableName();
  }


  private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
    public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
      return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
    }
  }

  public Run createRun() throws DatabaseException {
    return new Run();
  }



}
