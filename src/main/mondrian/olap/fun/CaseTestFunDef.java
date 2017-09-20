/*
// $Id: //open/mondrian-release/3.1/src/main/mondrian/olap/fun/CaseTestFunDef.java#3 $
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
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
import mondrian.calc.BooleanCalc;
import mondrian.calc.ResultStyle;
import mondrian.calc.impl.ConstantCalc;
import mondrian.calc.impl.GenericCalc;
import mondrian.calc.impl.GenericIterCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;
import java.util.ArrayList;

/**
 * Definition of the tested <code>CASE</code> MDX operator.
 *
 * Syntax is:
 * <blockquote><pre><code>Case
 * When &lt;Logical Expression&gt; Then &lt;Expression&gt;
 * [...]
 * [Else &lt;Expression&gt;]
 * End</code></blockquote>.
 *
 * @see CaseMatchFunDef
 *
 * @author jhyde
 * @version $Id: //open/mondrian-release/3.1/src/main/mondrian/olap/fun/CaseTestFunDef.java#3 $
 * @since Mar 23, 2006
 */
class CaseTestFunDef extends FunDefBase {
    static final ResolverImpl Resolver = new ResolverImpl();

    public CaseTestFunDef(FunDef dummyFunDef) {
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

            for (int j = 1; j < args.length; j += 2) {
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
        final BooleanCalc[] conditionCalcs =
                new BooleanCalc[args.length / 2];
        final Calc[] exprCalcs =
                new Calc[args.length / 2];
        final List<Calc> calcList = new ArrayList<Calc>();
        for (int i = 0, j = 0; i < exprCalcs.length; i++) {
            conditionCalcs[i] =
                    compiler.compileBoolean(args[j++]);
            calcList.add(conditionCalcs[i]);
            exprCalcs[i] = compiler.compile(args[j++]);
            calcList.add(exprCalcs[i]);
        }
        final Calc defaultCalc =
            args.length % 2 == 1
            ? compiler.compile(args[args.length - 1])
            : ConstantCalc.constantNull(call.getType());
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
                for (int i = 0; i < conditionCalcs.length; i++) {
                    if (conditionCalcs[i].evaluateBoolean(evaluator)) {
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
                    for (int i = 0; i < conditionCalcs.length; i++) {
                        if (conditionCalcs[i].evaluateBoolean(evaluator)) {
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
        public ResolverImpl() {
            super(
                    "_CaseTest",
                    "Case When <Logical Expression> Then <Expression> [...] [Else <Expression>] End",
                    "Evaluates various conditions, and returns the corresponding expression for the first which evaluates to true.",
                    Syntax.Case);
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            if (args.length < 1) {
                return null;
            }
            int j = 0;
            int clauseCount = args.length / 2;
            int mismatchingArgs = 0;
            int returnType = args[1].getCategory();

            if (Category.Null == returnType) {
                // Try find first not Null return expression
                for (int i = 3; Category.Null == returnType && i <= args.length; i += 2) {
                    returnType =  args[i < args.length ? i : args.length - 1].getCategory();
                }
            }

            for (int i = 0; i < clauseCount; i++) {
                if (!validator.canConvert(
                    j, args[j++], Category.Logical, conversions))
                {
                    mismatchingArgs++;
                }
                if (!validator.canConvert(
                    j, args[j++], returnType, conversions))
                {
                    mismatchingArgs++;
                }
            }
            if (j < args.length) {
                if (!validator.canConvert(
                    j, args[j++], returnType, conversions))
                {
                    mismatchingArgs++;
                }
            }
            Util.assertTrue(j == args.length);
            if (mismatchingArgs != 0) {
                return null;
            }
            FunDef dummy = createDummyFunDef(this, returnType, args);
            return new CaseTestFunDef(dummy);
        }

        public boolean requiresExpression(int k) {
            return true;
        }
    }
}

// End CaseTestFunDef.java
