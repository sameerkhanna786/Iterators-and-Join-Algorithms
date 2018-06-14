package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might be a useful reference).
   * 
   * Also, see discussion slides for week 7. 
   */
  private class SortMergeIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    private RecordIterator leftIter;
    private RecordIterator rightIter;
    private Record leftRecord;
    private Record nextRecord;
    private int rightRecord;
    private final Comparator<Record> cmp = new Comparator<Record>() {
      @Override
      public int compare(Record record1, Record record2) {
        return record1.getValues().get(getLeftColumnIndex())
                .compareTo(record2.getValues().get(getRightColumnIndex()));
      }
    };
    private boolean empty = false;


    // private String leftTableName;
    // private String rightTableName;
    // private RecordIterator leftIterator;
    // private RecordIterator rightIterator;
    // private Record leftRecord;
    // private Record nextRecord;
    // private Record rightRecord;
    // private boolean marked;

    public DataBox recordLeftVal(Record r) {
      return r.getValues().get(getLeftColumnIndex());
    }

    public DataBox recordRightVal(Record r) {
      return r.getValues().get(getRightColumnIndex());
    }

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();
      Comparator<Record> leftCompator =
              Comparator.comparing(r -> recordLeftVal(r));
      Comparator<Record> rightCompator =
              Comparator.comparing(r -> recordRightVal(r));

      SortOperator left = new SortOperator(getTransaction(),
              getLeftTableName(),
              leftCompator);
      SortOperator right = new SortOperator(getTransaction(),
              getRightTableName(),
              rightCompator);

      try {
        leftIter = getRecordIterator(left.sort());
        rightIter = getRecordIterator(right.sort());
        rightRecord = -1;
      } catch (IndexOutOfBoundsException e) {
        //The sort method will throw this exception iff either left or right is empty.
        empty = true;
      }
      //throw new UnsupportedOperationException("hw3: TODO");
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (empty) {
        return false;
      }
      if (nextRecord != null) {
        return true;
      }
      if ((!(leftIter.hasNext())) && (!(leftRecord != null))) {
        return false;
      }

      if (rightRecord == 0) {
        rightIter.mark();
        rightRecord = 1;
      }

      Record left = leftRecord != null ? leftRecord : leftIter.next();
      Record right;

      if (!rightIter.hasNext()) {
        if (leftIter.hasNext()) {
          leftRecord = leftIter.next();
        } else {
          return false;
        }

        rightIter.reset();
        return hasNext();
      } else {
        right = rightIter.next();
      }

      if (rightRecord == -1) {
        rightRecord = 1;
        rightIter.mark();
      }

      if (leftRecord == null) {
        leftRecord = left;
      }

      while (cmp.compare(left, right) != 0) {
        if (cmp.compare(left, right) < 0) {
          // Need to advance left side; left smaller than right

          //If we cannot advance, then there are no more possibilities
          if (!leftIter.hasNext()) {
            return false;
          }

          left = leftIter.next();
          leftRecord = left;

          //We need to compare all items in right table to the new left value.
          //Reset right iterator
          rightIter.reset();
          right = rightIter.next();
        } else {
          // Need to advance right side; right smaller than left

          //If we cannot advance, then there are no more possibilities
          if (!rightIter.hasNext()) {
            return false;
          }

          right = rightIter.next();
          //Get it set so that we can mark the iterator here.
          //See higher up for if loop where this is done.
          rightRecord = 0;
        }
      }

      // Left == Right so join their values
      List<DataBox> leftValues = new ArrayList<>(left.getValues());
      List<DataBox> rightValues = new ArrayList<>(right.getValues());
      //rightValues.remove(getRightColumnIndex());
      leftValues.addAll(rightValues);
      nextRecord = new Record(leftValues);
      return true;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (hasNext()) {
        Record out = nextRecord;
        nextRecord = null;
        return out;
      }
      throw new NoSuchElementException();
      //throw new UnsupportedOperationException("hw3: TODO");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record firstRecord, Record secondRecord) {
        return firstRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                secondRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record firstRecord, Record secondRecord) {
        return firstRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                secondRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }

    /**
    * Left-Right Record comparator
    * firstRecord : leftRecord
    * secondRecord: rightRecord
    */
    private class LR_RecordComparator implements Comparator<Record> {
      public int compare(Record firstRecord, Record secondRecord) {
        return firstRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                secondRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
