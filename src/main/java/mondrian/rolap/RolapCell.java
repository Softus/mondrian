/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2016 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;

import org.apache.log4j.Logger;

import org.olap4j.AllocationPolicy;
import org.olap4j.Scenario;

import java.sql.*;
import java.util.*;

/**
 * <code>RolapCell</code> implements {@link mondrian.olap.Cell} within a
 * {@link RolapResult}.
 */
public class RolapCell implements Cell {
    /**
     * @see mondrian.util.Bug#olap4jUpgrade Use
     * {@link mondrian.xmla.XmlaConstants}.ActionType.DRILLTHROUGH when present
     */
    private static final int MDACTION_TYPE_DRILLTHROUGH = 0x100;

    private final RolapResult result;
    protected final int[] pos;
    protected RolapResult.CellInfo ci;

    /**
     * Creates a RolapCell.
     *
     * @param result Result cell belongs to
     * @param pos Coordinates of cell
     * @param ci Cell information, containing value et cetera
     */
    RolapCell(RolapResult result, int[] pos, RolapResult.CellInfo ci) {
        this.result = result;
        this.pos = pos;
        this.ci = ci;
    }

    public List<Integer> getCoordinateList() {
        return new AbstractList<Integer>() {
            public Integer get(int index) {
                return pos[index];
            }

            public int size() {
                return pos.length;
            }
        };
    }

    public Object getValue() {
        if (ci.value == Util.nullValue) {
            return null;
        }
        return ci.value;
    }

    public String getCachedFormatString() {
        return ci.formatString;
    }

    public String getFormattedValue() {
        return ci.getFormatValue();
    }

    public boolean isNull() {
        return (ci.value == Util.nullValue);
    }

    public boolean isError() {
        return (ci.value instanceof Throwable);
    }

    /**
     * Create an sql query that, when executed, will return the drill through
     * data for this cell. If the parameter extendedContext is true, then the
     * query will include all the levels (i.e. columns) of non-constraining
     * members (i.e. members which are at the "All" level).
     * If the parameter extendedContext is false, the query will exclude
     * the levels (coulmns) of non-constraining members.
     */
    public String getDrillThroughSQL(boolean extendedContext) {
        RolapAggregationManager aggMan = AggregationManager.instance();
        final Member[] currentMembers = getMembersForDrillThrough();
        CellRequest cellRequest =
            RolapAggregationManager.makeDrillThroughRequest(
                currentMembers, extendedContext, result.getCube());
        return (cellRequest == null)
            ? null
            : aggMan.getDrillThroughSql(cellRequest, false);
    }


    public int getDrillThroughCount() {
        RolapAggregationManager aggMan = AggregationManager.instance();
        final Member[] currentMembers = getMembersForDrillThrough();
        CellRequest cellRequest =
            RolapAggregationManager.makeDrillThroughRequest(
                currentMembers, false, result.getCube());
        if (cellRequest == null) {
            return -1;
        }
        RolapConnection connection =
            (RolapConnection) result.getQuery().getConnection();
        final String sql = aggMan.getDrillThroughSql(cellRequest, true);
        final SqlStatement stmt =
            RolapUtil.executeQuery(
                connection.getDataSource(),
                sql,
                "RolapCell.getDrillThroughCount",
                "Error while counting drill-through");
        try {
            ResultSet rs = stmt.getResultSet();
            rs.next();
            ++stmt.rowCount;
            return rs.getInt(1);
        } catch (SQLException e) {
            throw stmt.handle(e);
        } finally {
            stmt.close();
        }
    }

    /**
     * Returns whether it is possible to drill through this cell.
     * Drill-through is possible if the measure is a stored measure
     * and not possible for calculated measures.
     *
     * @return true if can drill through
     */
    public boolean canDrillThrough() {
        // get current members
        final Member[] currentMembers = getMembersForDrillThrough();
        Cube x = chooseDrillThroughCube(currentMembers, result.getCube());
        return x != null;
    }

    public static RolapCube chooseDrillThroughCube(
        Member[] currentMembers,
        RolapCube defaultCube)
    {
        if (defaultCube != null && defaultCube.isVirtual()) {
            List<RolapCube> cubes = new ArrayList<RolapCube>();
            for (RolapMember member : defaultCube.getMeasuresMembers()) {
                if (member instanceof RolapVirtualCubeMeasure) {
                    RolapVirtualCubeMeasure measure =
                        (RolapVirtualCubeMeasure) member;
                    cubes.add(measure.getCube());
                }
            }
            defaultCube = cubes.get(0);
            assert !defaultCube.isVirtual();
        }
        final DrillThroughVisitor visitor =
            new DrillThroughVisitor();
        try {
            for (Member member : currentMembers) {
                visitor.handleMember(member);
            }
        } catch (RuntimeException e) {
            if (e == DrillThroughVisitor.bomb) {
                // No cubes left
                return null;
            } else {
                throw e;
            }
        }
        return visitor.cube == null
             ? defaultCube
             : visitor.cube;
    }

    private RolapEvaluator getEvaluator() {
        return result.getCellEvaluator(pos);
    }

    private Member[] getMembersForDrillThrough() {
        final Member[] currentMembers = result.getCellMembers(pos);

        // replace member if we're dealing with a trivial formula
        if (currentMembers[0]
            instanceof RolapHierarchy.RolapCalculatedMeasure)
        {
            RolapHierarchy.RolapCalculatedMeasure measure =
                (RolapHierarchy.RolapCalculatedMeasure)currentMembers[0];
            if (measure.getFormula().getExpression() instanceof MemberExpr) {
                currentMembers[0] =
                    ((MemberExpr) measure.getFormula().getExpression())
                    .getMember();
            }
        }
        return currentMembers;
    }

    /**
     * Generates an executes a SQL statement to drill through this cell.
     *
     * <p>Throws if this cell is not drillable.
     *
     * <p>Enforces limits on the starting and last row.
     *
     * <p>If tabFields is not null, returns the specified columns. (This option
     * is deprecated.)
     *
     * @param maxRowCount Maximum number of rows to retrieve, <= 0 if unlimited
     * @param firstRowOrdinal Ordinal of row to skip to (1-based), or 0 to
     *   start from beginning
     * @param fields            List of field expressions to return as the
     *                          result set columns.
     * @param extendedContext   If true, add non-constraining columns to the
     *                          query for levels below each current member.
     *                          This additional context makes the drill-through
     *                          queries easier for humans to understand.
     * @param logger Logger. If not null and debug is enabled, log SQL here
     * @return executed SQL statement
     */
    public SqlStatement drillThroughInternal(
        int maxRowCount,
        int firstRowOrdinal,
        List<OlapElement> fields,
        boolean extendedContext,
        Logger logger)
    {
        if (!canDrillThrough()) {
            throw Util.newError("Cannot do DrillThrough operation on the cell");
        }

        // Generate SQL.
        String sql = getDrillThroughSQL(/*fields,*/ extendedContext);
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug("drill through sql: " + sql);
        }

        // Choose the appropriate scrollability. If we need to start from an
        // offset row, it is useful that the cursor is scrollable, but not
        // essential.
        int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        return
            RolapUtil.executeQuery(
                result.getQuery().getConnection().getDataSource(),
                sql,
                maxRowCount,
                "RolapCell.drillThrough",
                "Error in drill through",
                resultSetType,
                resultSetConcurrency);
    }

    public Object getPropertyValue(String propertyName) {
        final boolean matchCase =
            MondrianProperties.instance().CaseSensitive.get();
        Property property = Property.lookup(propertyName, matchCase);
        Object defaultValue = null;
        if (property != null) {
            switch (property.ordinal) {
            case Property.CELL_ORDINAL_ORDINAL:
                return result.getCellOrdinal(pos);
            case Property.VALUE_ORDINAL:
                return getValue();
            case Property.FORMAT_STRING_ORDINAL:
                if (ci.formatString == null) {
                    ci.formatString = getEvaluator().getFormatString();
                }
                return ci.formatString;
            case Property.FORMATTED_VALUE_ORDINAL:
                return getFormattedValue();
            case Property.FONT_FLAGS_ORDINAL:
                defaultValue = 0;
                break;
            case Property.SOLVE_ORDER_ORDINAL:
                defaultValue = 0;
                break;
            default:
                // fall through
            }
        }
        return getEvaluator().getProperty(propertyName, defaultValue);
    }

    public Member getContextMember(Dimension dimension) {
        return result.getMember(pos, dimension);
    }

    public void setValue(
        Scenario scenario,
        Object newValue,
        AllocationPolicy allocationPolicy,
        Object... allocationArgs)
    {
        if (allocationPolicy == null) {
            // user error
            throw Util.newError(
                "Allocation policy must not be null");
        }
        final RolapMember[] members = result.getCellMembers(pos);
        for (int i = 0; i < members.length; i++) {
            Member member = members[i];
            if (ScenarioImpl.isScenario(member.getHierarchy())) {
                scenario =
                    (Scenario) member.getPropertyValue(Property.SCENARIO.name);
                members[i] = (RolapMember) member.getHierarchy().getAllMember();
            } else if (member.isCalculated()) {
                throw Util.newError(
                    "Cannot write to cell: one of the coordinates ("
                    + member.getUniqueName()
                    + ") is a calculated member");
            }
        }
        if (scenario == null) {
            throw Util.newError("No active scenario");
        }
        if (allocationArgs == null) {
            allocationArgs = new Object[0];
        }
        final Object currentValue = getValue();
        double doubleCurrentValue;
        if (currentValue == null) {
            doubleCurrentValue = 0d;
        } else if (currentValue instanceof Number) {
            doubleCurrentValue = ((Number) currentValue).doubleValue();
        } else {
            // Cell is not a number. Likely it is a string or a
            // MondrianEvaluationException. Do not attempt to change the value
            // in this case. (REVIEW: Is this the correct behavior?)
            return;
        }
        double doubleNewValue = ((Number) newValue).doubleValue();
        ((ScenarioImpl) scenario).setCellValue(
            result.getQuery().getConnection(),
            Arrays.asList(members),
            doubleNewValue,
            doubleCurrentValue,
            allocationPolicy,
            allocationArgs);
    }

    /**
     * Visitor that walks over a cell's expression and checks whether the
     * cell should allow drill-through. If not, throws the {@link #bomb}
     * exception.
     *
     * <p>Examples:</p>
     * <ul>
     * <li>Literal 1 is drillable</li>
     * <li>Member [Measures].[Unit Sales] is drillable</li>
     * <li>Calculated member with expression [Measures].[Unit Sales] +
     *     1 is drillable</li>
     * <li>Calculated member with expression
     *     ([Measures].[Unit Sales], [Time].PrevMember) is not drillable</li>
     * </ul>
     */
    private static class DrillThroughVisitor extends MdxVisitorImpl {
        static final RuntimeException bomb = new RuntimeException();
        RolapCube cube = null;

        DrillThroughVisitor() {
        }

        public Object visit(MemberExpr memberExpr) {
            handleMember(memberExpr.getMember());
            return null;
        }

        public Object visit(ResolvedFunCall call) {
            final FunDef def = call.getFunDef();
            final Exp[] args = call.getArgs();
            final String name = def.getName();
            if (name.equals("+")
                || name.equals("-")
                || name.equals("/")
                || name.equals("*")
                || name.equals("CoalesceEmpty")
                // Allow parentheses but don't allow tuple
                || name.equals("()") && args.length == 1)
            {
                return null;
            }
            throw bomb;
        }

        public void handleMember(Member member) {
            if (member instanceof RolapStoredMeasure) {
                // If this member is in a different cube that previous members
                // we've seen, we cannot drill through.
                final RolapCube cube = ((RolapStoredMeasure) member).getCube();
                if (this.cube == null) {
                    this.cube = cube;
                } else if (this.cube != cube) {
                    // this measure lives in a different cube than previous
                    // measures we have seen
                    throw bomb;
                }
            } else if (member instanceof RolapCubeMember) {
                handleMember(((RolapCubeMember) member).rolapMember);
            } else if (member
                instanceof RolapHierarchy.RolapCalculatedMeasure)
            {
                RolapHierarchy.RolapCalculatedMeasure measure =
                    (RolapHierarchy.RolapCalculatedMeasure) member;
                measure.getFormula().getExpression().accept(this);
            } else if (member instanceof RolapMember) {
                // regular RolapMember - fine
            } else {
                // don't know what this is!
                throw bomb;
            }
        }

        public Object visit(NamedSetExpr namedSetExpr) {
            throw Util.newInternal("not valid here: " + namedSetExpr);
        }

        public Object visit(Literal literal) {
            return null; // literals are drillable
        }

        public Object visit(Query query) {
            throw Util.newInternal("not valid here: " + query);
        }

        public Object visit(QueryAxis queryAxis) {
            throw Util.newInternal("not valid here: " + queryAxis);
        }

        public Object visit(Formula formula) {
            throw Util.newInternal("not valid here: " + formula);
        }

        public Object visit(UnresolvedFunCall call) {
            throw Util.newInternal("expected resolved expression");
        }

        public Object visit(Id id) {
            throw Util.newInternal("expected resolved expression");
        }

        public Object visit(ParameterExpr parameterExpr) {
            // Not valid in general; might contain complex expression
            throw bomb;
        }

        public Object visit(DimensionExpr dimensionExpr) {
            // Not valid in general; might be part of complex expression
            throw bomb;
        }

        public Object visit(HierarchyExpr hierarchyExpr) {
            // Not valid in general; might be part of complex expression
            throw bomb;
        }

        public Object visit(LevelExpr levelExpr) {
            // Not valid in general; might be part of complex expression
            throw bomb;
        }
    }
}

// End RolapCell.java
