package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class BNLJOperator extends JoinOperator {

  protected int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }


  /**
   * BNLJ: Block Nested Loop Join
   *  See lecture slides.
   *
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might prove to be a useful reference).
   */
  private class BNLJIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    private final BacktrackingIterator<Page> LPIter;
    private BacktrackingIterator<Page> RPIter;
    private Page[] currentLeftPages;
    private Page currentRightPage;
    private BacktrackingIterator<Record> LBIter;
    private BacktrackingIterator<Record> RBIter;

    private Iterator<Record> leftIterator = null;
    private Iterator<Record> rightIterator = null;
    private BacktrackingIterator<Record> leftRecordIterator = null;
    private BacktrackingIterator<Record> rightRecordIterator = null;
    private Record leftRecord = null;
    private Record nextRecord = null;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      LPIter = getPageIterator(getLeftTableName());
      RPIter = getPageIterator(getRightTableName());

      // Consume header pages
      RPIter.next();
      LPIter.next();

      // Read in numPages
      currentLeftPages = new Page[numBuffers];
      for (int i = 0; i < numBuffers; i++) {
        currentLeftPages[i] = LPIter.hasNext() ? LPIter.next() : null;
      }

      // Remove null values from currentLeftPages
      currentLeftPages = Arrays.stream(currentLeftPages)
              .filter(x -> x != null)
              .toArray(Page[]::new);

      currentRightPage = RPIter.next();
      LBIter = getBlockIterator(getLeftTableName(), currentLeftPages);
      //throw new UnsupportedOperationException("hw3: TODO");
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (nextRecord != null) {
        return true;
      }

      try {
        while (true) {
          if (leftRecord == null) {
            if (LBIter.hasNext()) {
              // If left page still has records to give, then just get the
              // next one and restart right block iter.

              leftRecord = LBIter.next();
              RBIter = getBlockIterator(getRightTableName(),
                      new Page[]{currentRightPage});
            } else {
              // Current left page is exhausted. Restart it with the next
              // right page (if there is one).

              if (!RPIter.hasNext()) {
                // Right page relation is exhausted. Need to restart it with
                // LPIter on the next page (if there is one).

                currentLeftPages = new Page[numBuffers];
                for (int i = 0; i < numBuffers; i++) {
                  currentLeftPages[i] = LPIter.hasNext() ? LPIter.next() : null;
                }

                // Remove null values from currentLeftPages
                currentLeftPages = Arrays.stream(currentLeftPages)
                        .filter(x -> x != null)
                        .toArray(Page[]::new);

                LBIter = getBlockIterator(getLeftTableName(), currentLeftPages);
                if (!LBIter.hasNext()) {
                  return false;
                }

                leftRecord = LBIter.next();
                RPIter = getPageIterator(getRightTableName()); // Restart RP
                RPIter.next(); // Consume header page
              } else {
                LBIter = getBlockIterator(getLeftTableName(), currentLeftPages);
                assert LBIter.hasNext() : "LBIter degenerate";
                leftRecord = LBIter.next();
              }

              currentRightPage = RPIter.next();
              RBIter = getBlockIterator(getRightTableName(),
                      new Page[]{currentRightPage});
            }
          }
          while (RBIter.hasNext()) {
            Record rightRecord = RBIter.next();
            DataBox leftJoinValue = leftRecord.getValues().get(getLeftColumnIndex());
            DataBox rightJoinValue = rightRecord.getValues().get(getRightColumnIndex());
            if (leftJoinValue.equals(rightJoinValue)) {
              List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
              List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
              leftValues.addAll(rightValues);
              nextRecord = new Record(leftValues);
              return true;
            }
          }
          leftRecord = null;
        }
      } catch (DatabaseException e) {
        System.err.println("Caught database error " + e.getMessage());
        return false;
      }
      //throw new UnsupportedOperationException("hw3: TODO");
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (hasNext()) {
        if (nextRecord != null) {
          Record out = nextRecord;
          nextRecord = null;
          return out;
        }
      }
      throw new NoSuchElementException();
      //throw new UnsupportedOperationException("hw3: TODO");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
