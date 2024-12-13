package rel;

import java.util.*;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

public class PSort extends Sort implements PRel{

    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
            ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }

    private class CustomComparator implements Comparator<Object[]> {
        @Override
        public int compare(Object[] row1, Object[] row2) {
            for (RelFieldCollation fc : collation.getFieldCollations()) {
                int index = fc.getFieldIndex();
                if (fc.direction.isDescending()) {
                    if (((Comparable) row1[index]).compareTo(row2[index]) < 0) {
                        return 1;
                    }
                    else if (((Comparable) row1[index]).compareTo(row2[index]) > 0) {
                        return -1;
                    }
                }
                else {
                    if (((Comparable) row1[index]).compareTo(row2[index]) > 0) {
                        return 1;
                    }
                    else if (((Comparable) row1[index]).compareTo(row2[index]) < 0) {
                        return -1;
                    }
                }
            }
            return 0;
        }

    }

    private PRel inputRel = null;
    private boolean processed = false;

//    private final LinkedList<Object[]> sortedRows = new LinkedList<>();
    private final PriorityQueue<Object[]> sortedRows = new PriorityQueue<>(new CustomComparator());

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        /* Write your code here */
        inputRel = (PRel) input;
        processed = false;
        return inputRel.open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        /* Write your code here */
        inputRel.close();
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        /* Write your code here */
        if (inputRel == null) {
            return false;
        }
        if (!processed) {
            processed = true;
            int row_cnt = 0;
            while (inputRel.hasNext()) {
                Object[] row = inputRel.next();
                if ((offset != null) && (row_cnt < RexLiteral.intValue(offset))) {
                    row_cnt++;
                    continue;
                }
                if ((fetch != null) && (offset != null) && (row_cnt >= RexLiteral.intValue(offset) + RexLiteral.intValue(fetch))) {
                    break;
                }
                row_cnt++;
                sortedRows.add(row);
            }
        }
        return !sortedRows.isEmpty();
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        /* Write your code here */
        return sortedRows.poll();
    }

}
