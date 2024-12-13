package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;

import convention.PConvention;
import org.apache.calcite.util.Pair;

import java.util.*;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {

    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    private PRel inputRel = null;
    private boolean processed = false;

    // HashMap to store the grouped rows
    private final HashMap<String, List<Object[]>> groupedRows = new HashMap<>();

    // Queue to store the results of the aggregate functions
    private final Queue<Object[]> results = new LinkedList<>();

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PAggregate");
        /* Write your code here */
        inputRel = (PRel) input;
        processed = false;
        return inputRel.open();
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        /* Write your code here */
        inputRel.close();
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        /* Write your code here */
        if (inputRel == null) {
            return false;
        }
        if (!processed) {
            processed = true;
            int num_cols = 0;
            while (inputRel.hasNext()) {
                Object[] row = inputRel.next();
                if (num_cols == 0) {
                    num_cols = row.length;
                }
                for (int gid = 0; gid < groupSets.size(); gid++) {
                    ImmutableBitSet groupSet = groupSets.get(gid);
                    StringBuilder groupKey = new StringBuilder();
                    groupKey.append(gid).append(":");
                    for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
                        groupKey.append(row[i]).append(",");
                    }
                    String group = groupKey.toString();

                    if (!groupedRows.containsKey(group)) {
                        groupedRows.put(group, new ArrayList<>());
                    }
                    groupedRows.get(group).add(row);
                }
            }
            for (Map.Entry<String, List<Object[]>> entry : groupedRows.entrySet()) {
                String key = entry.getKey();
                List<Object[]> group = entry.getValue();

                int grp_id = Integer.parseInt(key.split(":")[0]);
                Object[] nextRow = new Object[groupSet.cardinality() + aggCalls.size()];
                List<Integer> aggInds = new ArrayList<>();
                for (int i = groupSets.get(grp_id).nextSetBit(0); i >= 0; i = groupSets.get(grp_id).nextSetBit(i+1)) {
                    nextRow[i] = group.get(0)[i];
                }
                int i = groupSet.cardinality();
                for (AggregateCall aggCall : aggCalls) {
                    aggInds.add(i);
                    switch (aggCall.getAggregation().getName()) {
                        case "COUNT":
                        case "SUM":
                        case "AVG":
                            nextRow[i] = 0;
                            break;
                        case "MIN":
                        case "MAX":
                            nextRow[i] = null;
                            break;
                        default:
                            break;
                    }
                    i++;
                }
                int row_cnt = 0;
                HashSet<Pair<Integer, Object>> seen = new HashSet<>();
                for (Object[] row : group) {
                    int cnt = 0;
                    for (AggregateCall aggCall : aggCalls) {
                        int aggInd = -1;
                        if (!aggCall.getArgList().isEmpty()) {
                            aggInd = aggCall.getArgList().get(0);
                        }
                        int rowInd = aggInds.get(cnt++);
                        switch (aggCall.getAggregation().getName()) {
                            case "COUNT":
                                if (aggCall.isDistinct()) {
                                    if (seen.contains(new Pair<>(rowInd, row[aggInd]))) {
                                        break;
                                    }
                                    seen.add(new Pair<>(rowInd, row[aggInd]));
                                }
                                nextRow[rowInd] = (Integer) nextRow[rowInd] + 1;
                                break;
                            case "MIN":
                                if (row[aggInd] != null) {
                                    if (nextRow[rowInd] == null) {
                                        nextRow[rowInd] = row[aggInd];
                                    }
                                    nextRow[rowInd] = (((Comparable) row[aggInd]).compareTo(nextRow[aggInd]) < 0) ? row[aggInd] : nextRow[aggInd];
                                }
                                break;
                            case "MAX":
                                if (row[aggInd] != null) {
                                    if (nextRow[rowInd] == null) {
                                        nextRow[rowInd] = row[aggInd];
                                    }
                                    nextRow[rowInd] = (((Comparable) row[aggInd]).compareTo(nextRow[aggInd]) > 0) ? row[aggInd] : nextRow[aggInd];
                                }
                                break;
                            case "SUM":
                                if (row[aggInd] != null) {
                                    if (aggCall.isDistinct()) {
                                        if (seen.contains(new Pair<>(rowInd, row[aggInd]))) {
                                            break;
                                        }
                                        seen.add(new Pair<>(rowInd, row[aggInd]));
                                    }
                                    Number sm = (Number) nextRow[rowInd];
                                    Number rowValue = (Number) row[aggInd];
                                    nextRow[rowInd] = sm.doubleValue() + rowValue.doubleValue();
                                }
                                break;
                            case "AVG":
                                if (row[aggInd] != null) {
                                    if (aggCall.isDistinct()) {
                                        if (seen.contains(new Pair<>(rowInd, row[aggInd]))) {
                                            break;
                                        }
                                        seen.add(new Pair<>(rowInd, row[aggInd]));
                                    }
                                    Number av = (Number) nextRow[rowInd];
                                    Number rowValue = (Number) row[aggInd];
                                    nextRow[rowInd] = (av.doubleValue() * row_cnt + rowValue.doubleValue()) / (double) (row_cnt + 1);
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    row_cnt++;
                }
                results.add(nextRow);
            }
        }
        return !results.isEmpty();
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        /* Write your code here */
        return results.poll();
    }

}