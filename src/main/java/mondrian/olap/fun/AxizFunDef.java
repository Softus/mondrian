package mondrian.olap.fun;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.IntegerCalc;
import mondrian.calc.ListCalc;
import mondrian.calc.ResultStyle;

import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.GenericIterCalc;
import mondrian.mdx.ResolvedFunCall;

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.ExpCacheDescriptor;
import mondrian.olap.FunDef;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.QueryAxis;
import mondrian.olap.Util;
import mondrian.olap.Validator;
import mondrian.olap.type.Type;

/**
 * return set from a axis. using: with set [test] as 'Axis(1)' select
 * [Measures].[A] on columns, {A,B,C} on rows from [cubeA]
 * 
 * [test] contain set:{A,B,C}
 * 
 */
public class AxizFunDef extends FunDefBase {
	static final ReflectiveMultiResolver Resolver =
		new ReflectiveMultiResolver
			( "Axiz", "Axiz([<count>])", "return axis set"
			, new String[] { "fx", "fxn" }
			, AxizFunDef.class
			);

	public AxizFunDef(FunDef dummyFunDef) {
		super(dummyFunDef);
	}
	
	public Type getResultType(Validator validator, Exp[] args) {
		int axis = args.length > 0 && (args[0] instanceof Literal) ? ((Literal)args[0]).getIntValue() : 0;
		QueryAxis[] cs = validator.getQuery().axes;
		
		if (cs.length < axis) {
			throw Util.newInternal("current query only:" + cs.length + " axis, you canot get " + axis + " axis set");
		}
		return cs[axis].getSet().getType();
	}

	public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
		final IntegerCalc nc = call.getArgs().length > 0 ? compiler.compileInteger(call.getArg(0)) : null;

		final ListCalc listCalc =
			new AbstractListCalc(call, new Calc[] {nc}) {
				public List<?> evaluateList(Evaluator evaluator) {
					// which axis set y want?default 0
					final int axis = (nc == null) ? 0 : nc.evaluateInteger(evaluator);
					final List<Position> positions = evaluator.getQuery().axisPositions[axis].getPositions();
					final int size = positions.size();

					if (size > 0) {
						final int arity = positions.get(0).size();
						
						if (arity > 1) {
							List<Member[]> result = new ArrayList<Member[]>(size);
							
							for (final List<Member> position : positions) {
								final Member[] tuple = new Member[arity];
								
								result.add(tuple);
								for (int i = 0; i < arity; i++) {
									tuple[i] = position.get(i);
								}
							}
							return result;
						} else {
							List<Member> result = new ArrayList<Member>(size);
							
							for (final List<Member> position : positions) {
								result.add(position.get(0));
							}
							return result;
						}
					} else {
						return Collections.EMPTY_LIST;
					}
				}
			};

		if (MondrianProperties.instance().EnableExpCache.get()) {
			final ExpCacheDescriptor cacheDescriptor = new ExpCacheDescriptor(call, listCalc, compiler.getEvaluator());

			return new GenericIterCalc(call) {
				public Object evaluate(Evaluator evaluator) {
					return evaluator.getCachedResult(cacheDescriptor);
				}

				public Calc[] getCalcs() {
					return new Calc[] {cacheDescriptor.getCalc()};
				}

				public ResultStyle getResultStyle() {
					// cached lists are immutable
					return ResultStyle.LIST;
				}
			};
			
		} else {
			return listCalc;
		}
	}
}

/////////////////////////////////////////////////////////////////////////////
// End of AxisFunDef.java