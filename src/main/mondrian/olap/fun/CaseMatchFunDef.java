/*
// $Id: //open/mondrian/src/main/mondrian/olap/fun/CaseMatchFunDef.java#8 $
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.BooleanType;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.olap.type.TypeUtil;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ResultStyle;
import mondrian.calc.impl.ConstantCalc;
import mondrian.calc.impl.GenericCalc;
import mondrian.calc.impl.GenericIterCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;
import java.util.ArrayList;

/**
 * Definition of the matched <code>CASE</code> MDX operator.
 *
 * Syntax is:
 * <blockquote><pre><code>Case &lt;Expression&gt;
 * When &lt;Expression&gt; Then &lt;Expression&gt;
 * [...]
 * [Else &lt;Expression&gt;]
 * End</code></blockquote>.
 *
 * @see CaseTestFunDef
 * @author jhyde
 * @version $Id: //open/mondrian/src/main/mondrian/olap/fun/CaseMatchFunDef.java#8 $
 * @since Mar 23, 2006
 */
class CaseMatchFunDef extends FunDefBase {
    static final ResolverImpl Resolver = new ResolverImpl();

    private CaseMatchFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        switch (returnCategory) {
        case Category.Numeric:
            return new NumericType();
        case Category.String:
            return new StringType();
        case Category.Logical:
            return new BooleanType();
        default:
            final int typeCount = args.length / 2;
            final Type[] resultTypes = new Type[typeCount];
            int i = 0;

            for (int j = 2; j < args.length; j += 2) {
                resultTypes[i++] = args[j].getType();
            }
            if (i < typeCount) {
                resultTypes[i] = args[args.length - 1].getType();
            }
            return TypeUtil.computeCommonType(true, resultTypes);
        }
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp[] args = call.getArgs();
        final List<Calc> calcList = new ArrayList<Calc>();
        final Calc valueCalc =
                compiler.compileScalar(args[0], true);
        calcList.add(valueCalc);
        final int matchCount = (args.length - 1) / 2;
        final Calc[] matchCalcs = new Calc[matchCount];
        final Calc[] exprCalcs = new Calc[matchCount];
        for (int i = 0, j = 1; i < exprCalcs.length; i++) {
            matchCalcs[i] = compiler.compileScalar(args[j++], true);
            calcList.add(matchCalcs[i]);
            exprCalcs[i] = compiler.compile(args[j++]);
            calcList.add(exprCalcs[i]);
        }
        final Calc defaultCalc =
                args.length % 2 == 0 ?
                compiler.compile(args[args.length - 1]) :
                ConstantCalc.constantNull(call.getType());
        calcList.add(defaultCalc);
        final Calc[] calcs = calcList.toArray(new Calc[calcList.size()]);

        if (call.getType() instanceof SetType) {
            ResultStyle rs = ResultStyle.MUTABLE_LIST;
            for (int i = 0; i <= exprCalcs.length; i++) {
                final Calc resultCalc = i < exprCalcs.length ? exprCalcs[i] : defaultCalc;
                final ResultStyle calcRs = resultCalc.getResultStyle();

                if (calcRs == ResultStyle.ITERABLE) {
                    rs = ResultStyle.ITERABLE;
                } else if (calcRs == ResultStyle.LIST) {
                    if (rs == ResultStyle.MUTABLE_LIST) {
                        rs = ResultStyle.LIST;
                    }
                }
            }

            final ResultStyle resultStyle = rs;
            return new GenericIterCalc(call) {
                public Object evaluate(Evaluator evaluator) {
                    Object value = valueCalc.evaluate(evaluator);
                    for (int i = 0; i < matchCalcs.length; i++) {
                        Object match = matchCalcs[i].evaluate(evaluator);
                        if (match.equals(value)) {
                            return exprCalcs[i].evaluate(evaluator);
                        }
                    }
                    return defaultCalc.evaluate(evaluator);
                }

                public Calc[] getCalcs() {
                    return calcs;
                }

                public ResultStyle getResultStyle() {
                    return resultStyle;
                }
            };
        } else {
            return new GenericCalc(call) {
                public Object evaluate(Evaluator evaluator) {
                    Object value = valueCalc.evaluate(evaluator);
                    for (int i = 0; i < matchCalcs.length; i++) {
                        Object match = matchCalcs[i].evaluate(evaluator);
                        if (match.equals(value)) {
                            return exprCalcs[i].evaluate(evaluator);
                        }
                    }
                    return defaultCalc.evaluate(evaluator);
                }

                public Calc[] getCalcs() {
                    return calcs;
                }
            };
        }
    }

    private static class ResolverImpl extends ResolverBase {
        private ResolverImpl() {
            super(
                    "_CaseMatch",
                    "Case <Expression> When <Expression> Then <Expression> [...] [Else <Expression>] End",
                    "Evaluates various expressions, and returns the corresponding expression for the first which matches a particular value.",
                    Syntax.Case);
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            if (args.length < 3) {
                return null;
            }
            int valueType = args[0].getCategory();
            int returnType = args[2].getCategory();

            if (Category.Null == returnType) {
                // Try find first not Null return expression
                for (int i = 4; Category.Null == returnType && i <= args.length; i += 2) {
                    returnType =  args[i < args.length ? i : args.length - 1].getCategory();
                }
            }

            int j = 0;
            int clauseCount = (args.length - 1) / 2;
            int mismatchingArgs = 0;
            if (!validator.canConvert(args[j++], valueType, conversions)) {
                mismatchingArgs++;
            }
            for (int i = 0; i < clauseCount; i++) {
                if (!validator.canConvert(args[j++], valueType, conversions)) {
                    mismatchingArgs++;
                }
                if (!validator.canConvert(args[j++], returnType, conversions)) {
                    mismatchingArgs++;
                }
            }
            if (j < args.length) {
                if (!validator.canConvert(args[j++], returnType, conversions)) {
                    mismatchingArgs++;
                }
            }
            Util.assertTrue(j == args.length);
            if (mismatchingArgs != 0) {
                return null;
            }

            FunDef dummy = createDummyFunDef(this, returnType, args);
            return new CaseMatchFunDef(dummy);
        }

        public boolean requiresExpression(int k) {
            return true;
        }
    }
}

// End CaseMatchFunDef.java
