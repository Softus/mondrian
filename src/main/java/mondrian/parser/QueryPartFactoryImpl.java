package mondrian.parser;

import mondrian.olap.*;

import java.util.*;

class QueryPartFactoryImpl
    implements MdxParserValidator.QueryPartFactory
{
    public Query makeQuery(
        Connection mdxConnection,
        Formula[] formulae,
        QueryAxis[] axes,
        String cube,
        Exp slicer,
        QueryPart[] cellProps,
        boolean strictValidation)
    {
        final QueryAxis slicerAxis =
            slicer == null
                ? null
                : new QueryAxis(
                    false, slicer, AxisOrdinal.StandardAxisOrdinal.SLICER,
                    QueryAxis.SubtotalVisibility.Undefined, new Id[0]);
        return new Query(
            mdxConnection, formulae, axes, cube, slicerAxis, cellProps, false,
            strictValidation);
    }

    public DrillThrough makeDrillThrough(
        Query query,
        int maxRowCount,
        int firstRowOrdinal,
        List<Exp> returnList)
    {
        return new DrillThrough(
            query, maxRowCount, firstRowOrdinal, returnList);
    }

    /**
     * Creates an {@link Explain} object.
     */
    public Explain makeExplain(
        QueryPart query)
    {
        return new Explain(query);
    }
}
