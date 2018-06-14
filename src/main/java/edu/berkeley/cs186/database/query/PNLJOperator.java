package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  /**
   * PNLJ: Page Nested Loop Join
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
  private class PNLJIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    private final BacktrackingIterator<Page> LPIter;
    private BacktrackingIterator<Page> RPIter;
    private Page currentLeftPage;
    private Page currentRightPage;
    private BacktrackingIterator<Record> LBIter;
    private BacktrackingIterator<Record> RBIter;
    private Record leftRecord;
    private Record nextRecord;
    private ArrayList<Record> totalJoin = new ArrayList<>();
    private boolean hasCreated = false;

    private List<DataBox> leftValues;
    private List<DataBox> rightValues;

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      LPIter = getPageIterator(getLeftTableName());
      RPIter = getPageIterator(getRightTableName());
      // First pages will be header pages
      // Remove these, as these will not be needed for the join.
      RPIter.next();
      LPIter.next();
      //Set the current pages to be the first non-header pages
      currentLeftPage = LPIter.next();
      currentRightPage = RPIter.next();
      LBIter = getBlockIterator(getLeftTableName(),
              new Page[]{currentLeftPage});
      //throw new UnsupportedOperationException("hw3: TODO");
    }

    /**
     * Iterate through every record in every page.
     * Once a page is finished iterating through, call the next page in the respective page iterator.
     *
     * Do this for both first and second page iterators.
     * If our check of first and second records in a given tuple returns true,
     * return true.
     *
     * Once we exhaust all possible remaining tuples and none of them return true,
     * return false.
     */

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      //If we already know the next record, no need for any computation.
      //Return true.
      try {
        if (nextRecord != null) {
          return true;
        }
        while (true) {
          //Check if we need another record fed in.
          if (leftRecord == null) {
            //Check if we can still feed in more pages.
            if (LBIter.hasNext()) {
              leftRecord = LBIter.next();
              RBIter = getBlockIterator(getRightTableName(),
                      new Page[]{currentRightPage});
            } else {
              // Current left page is exhausted. Restart it with the next
              // right page (if there is one).

              if (!RPIter.hasNext()) {
                // Right page relation is exhausted. Need to restart it with
                // LPIter on the next page (if there is one).

                if (LPIter.hasNext()) {
                  currentLeftPage = LPIter.next();
                  LBIter = getBlockIterator(getLeftTableName(),
                          new Page[]{currentLeftPage});
                  if (!LBIter.hasNext()) {
                    return false;
                  }
                  leftRecord = LBIter.next();
                } else {
                  // Outermost relation exhausted so we're done.
                  return false;
                }

                RPIter = getPageIterator(getRightTableName()); // Restart RP
                RPIter.next(); // Consume header page
              } else {
                LBIter = getBlockIterator(getLeftTableName(),
                        new Page[]{currentLeftPage});
                if (!LBIter.hasNext()) {
                  return false;
                }
                leftRecord = LBIter.next();
              }

              currentRightPage = RPIter.next();
              RBIter = getBlockIterator(getRightTableName(),
                      new Page[]{currentRightPage});
            }
          }
          while (RBIter.hasNext()) {
            Record rightRecord = RBIter.next();
            //Get values from given record tuple
            DataBox leftJoinValue = leftRecord.getValues().get(getLeftColumnIndex());
            DataBox rightJoinValue = rightRecord.getValues().get(getRightColumnIndex());
            //Use built-in equals method from DataBox to check values from record tuple
            if (leftJoinValue.equals(rightJoinValue)) {
              //Get values in List form from each record
              leftValues = new ArrayList<>(leftRecord.getValues());
              rightValues = new ArrayList<>(rightRecord.getValues());
              leftValues.addAll(rightValues);
              //Add values into first list from second in order to ensure all data is merged
              //Set this to be the record returned
              nextRecord = new Record(leftValues);
              //We found AND created the desired return value.
              //We can return true :D!
              return true;
            }
          }
          leftRecord = null;
        }
      } catch (Exception e) {
        return false;
      }
      //throw new UnsupportedOperationException("hw3: TODO");
    }

    public boolean hAsNext() {
      try {
        if (totalJoin.size() == 0 && !hasCreated) {
          for (BacktrackingIterator<Page> it = LPIter; it.hasNext(); ) {
            Page left = it.next();
            LBIter = getBlockIterator(getLeftTableName(),
                    new Page[]{left});
            for (BacktrackingIterator<Page> it1 = RPIter; it1.hasNext(); ) {
              Page right = it1.next();
              RBIter = getBlockIterator(getLeftTableName(),
                      new Page[]{right});
              for (; LBIter.hasNext(); ) {
                Record leftRecord = LBIter.next();
                for (; RBIter.hasNext(); ) {
                  Record rightRecord = RBIter.next();

                  DataBox leftJoinValue = leftRecord.getValues().get(getLeftColumnIndex());
                  DataBox rightJoinValue = rightRecord.getValues().get(getRightColumnIndex());
                  //Use built-in equals method from DataBox to check values from record tuple
                  if (leftJoinValue.equals(rightJoinValue)) {
                    //Get values in List form from each record
                    leftValues = new ArrayList<>(leftRecord.getValues());
                    rightValues = new ArrayList<>(rightRecord.getValues());
                    leftValues.addAll(rightValues);
                    //Add values into first list from second in order to ensure all data is merged
                    //Set this to be the record returned
                    nextRecord = new Record(leftValues);
                    //We found AND created the desired return value.
                    // Add to our growing ArrayList
                    totalJoin.add(nextRecord);
                  }
                }
                RBIter = getBlockIterator(getLeftTableName(),
                        new Page[]{right});
              }
              LBIter = getBlockIterator(getLeftTableName(),
                      new Page[]{left});
            }
            RPIter = getPageIterator(getRightTableName());
            RPIter.next();
          }
          hasCreated = true;
          //System.out.println(totalJoin.size());
        }
        if (totalJoin.size() == 0) {
          return false;
        }
        else {
          return true;
        }
      } catch (DatabaseException e) {
        System.err.println("ERROR: DB Exception: " + e.getMessage());
      }

      return false;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      try {
        if (!hasNext()) {
          //hasNext found nothing, so there is no "next" element
          throw new NoSuchElementException();
        }
        //We called hasNext already; nextRecord should be primed and ready to go!
        //We need to reset nextRecord in order to be primed for next run through hasNext().
        //Use temp variable in order to do this.
        if (nextRecord != null) {
          Record temp = nextRecord;
          nextRecord = null;
          return temp;
        }
        throw new NoSuchElementException();
        //throw new UnsupportedOperationException("hw3: TODO");
      } catch (Exception e) {
        return null;
      }
    }

    public Record nExt() {
      if (hasNext()) {
        return totalJoin.remove(totalJoin.size() - 1);
      }
      return null;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
