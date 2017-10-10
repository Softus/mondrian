//	Copyright (C) 2008, IDC
//
//	Project: 
//	File:    
//	Author:  Alexander Korsukov (akorsukov@gmail.com)
//	Date:
//
//	Description:
//		NONE
//
//	Update History:
//		NONE
//
/////////////////////////////////////////////////////////////////////////////

package mondrian.olap.fun;

/////////////////////////////////////////////////////////////////////////////
//Imports

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.MemberCalc;
import mondrian.calc.TupleCalc;

import mondrian.calc.impl.AbstractListCalc;

import mondrian.mdx.ResolvedFunCall;

import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Member;

import mondrian.olap.type.EmptyType;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;

/////////////////////////////////////////////////////////////////////////////
//

public class OpeningSetFunDef extends FunDefBase {
	static final ReflectiveMultiResolver Resolver =
		new ReflectiveMultiResolver
			( "OpeningSet"
			, "OpeningSet(<Set>[, <Tuple> | <Empty> [, BYSET | BYHIER]])"
			, "Ordered opening set of axis set"
			, new String[] {"fxx", "fxxt", "fxxm", "fxxty", "fxxmy", "fxxey", "fxxey"}
			, OpeningSetFunDef.class
			);
	
	private enum Flag { BYHIER, BYSET }

	public OpeningSetFunDef(FunDef dummyFunDef) {
		super(dummyFunDef);
	}

	public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
		final Exp set = call.getArg(0);
		final ListCalc listCalc = compiler.compileList(set);

		final Exp tuple =
			call.getArgCount() > 1
				&& !(call.getArg(1).getType() instanceof EmptyType)
					? call.getArg(1) : null;
		final Flag flag =
			call.getArgCount() > 2
				? getLiteralArg(call, 2, Flag.BYSET, Flag.class) : Flag.BYSET;
				
		final MemberCalc memberCalc =
			(null != tuple && tuple.getType() instanceof MemberType)
				? compiler.compileMember(tuple) : null;
		final TupleCalc tupleCalc =
			(null != tuple && tuple.getType() instanceof TupleType)
				? compiler.compileTuple(tuple) : null;
				
		final Calc[] calcs =
			(null != memberCalc || null != tupleCalc)
				? new Calc[] {listCalc, (null != memberCalc ? memberCalc : tupleCalc)}
				: new Calc[] {listCalc};

		if (((SetType)set.getType()).getArity() == 1) {
			return new AbstractListCalc(call, calcs) {
				@SuppressWarnings("unchecked")
                public List<?> evaluateList(Evaluator evaluator) {
					final List<Member> set = (List<Member>)listCalc.evaluateList(evaluator);

					if (set.size() > 0) {
						final Member[] specifiedTuple =
							(null != memberCalc)
								? new Member[] {memberCalc.evaluateMember(evaluator)}
								: (null != tupleCalc)
									? tupleCalc.evaluateTuple(evaluator)
									: null;
						
						Member currentItem = null;
						final Dimension dimension = set.get(0).getDimension();
						
						if (null != specifiedTuple) {
							final String dimUniqueName = dimension.getUniqueName(); 
								
							for (int j = 0; j < specifiedTuple.length; j++) {
								if (specifiedTuple[j].getDimension().getUniqueName().equals(dimUniqueName)) {
									currentItem = specifiedTuple[j];
									break;
								}
							}
						}

						if (null == currentItem)
							currentItem = evaluator.getContext(dimension);
						
						for (int i = 0; i < set.size(); i++) {
							Member item = set.get(i);
							
							if (currentItem.getUniqueName().equals(item.getUniqueName())) {
								final List<Member> result = new ArrayList<Member>(i + 1);
								
								result.add(item);
								for (int j = i; j > 0; j--) {
									item = set.get(j - 1);
									
									if (currentItem.getDepth() == item.getDepth()) {
										if (Flag.BYHIER == flag) {
											final Member currentParent = currentItem.getParentMember();
											final Member parent = item.getParentMember();
											
											if (currentParent != parent && !currentParent.getUniqueName().equals(parent.getUniqueName())) {
												break;
											}
										}
										result.add(item);
									} else if (currentItem.getDepth() > item.getDepth()) {
										break;
									}
								}
								
								Collections.reverse(result);
								return result;
							}
						}
					}
					return Collections.EMPTY_LIST; 
				}
			};
		} else {
			return new AbstractListCalc(call, calcs) {
				@SuppressWarnings("unchecked")
                public List<?> evaluateList(Evaluator evaluator) {
					final List<Member[]> set = (List<Member[]>)listCalc.evaluateList(evaluator);

					if (set.size() > 0) {
						final Member[] setTuple = set.get(0);
						final Member[] currentTuple = new Member[setTuple.length];
						final Member[] specifiedTuple =
							(null != memberCalc)
								? new Member[] {memberCalc.evaluateMember(evaluator)}
								: (null != tupleCalc)
									? tupleCalc.evaluateTuple(evaluator)
									: null;

						for (int i = 0; i < currentTuple.length; i++) {
							final Dimension dimension = setTuple[i].getDimension();
							
							if (null != specifiedTuple) {
								final String dimUniqueName = dimension.getUniqueName(); 
									
								for (int j = 0; j < specifiedTuple.length; j++) {
									if (specifiedTuple[j].getDimension().getUniqueName().equals(dimUniqueName)) {
										currentTuple[i] = specifiedTuple[j];
										break;
									}
								}
							}

							if (null == currentTuple[i])
								currentTuple[i] = evaluator.getContext(dimension);
						}
						
						final int length = currentTuple.length;
	
						for (int i = 0; i < set.size(); i++) {
							Member[] tuple = set.get(i);
							
							if (equalsTuple(currentTuple, tuple, length)) {
								final List<Member[]> result = new ArrayList<Member[]>(i + 1);
								final Member currentItem = currentTuple[length - 1];
								
								result.add(tuple);
								for (int j = i; j > 0; j--) {
									tuple = set.get(j - 1);
									
									if (!equalsTuple(currentTuple, tuple, length - 1)) {
										break;
									}
									
									final Member item = tuple[length - 1];
									
									if (currentItem.getDepth() == item.getDepth()) {
										if (Flag.BYHIER == flag) {
											final Member currentParent = currentItem.getParentMember();
											final Member parent = item.getParentMember();
											
											if (currentParent != parent
												&& !currentParent.getUniqueName().equals(parent.getUniqueName())) {
												break;
											}
										}
										result.add(tuple);
									} else if (currentItem.getDepth() > item.getDepth()) {
										break;
									}
								}
								
								Collections.reverse(result);
								return result;
							}
						}
					}
					return Collections.EMPTY_LIST; 
				}
				
				private boolean equalsTuple(final Member[] x, final Member[] y, final int length)
				{
					for (int i = length - 1; i >= 0; i--) {
						if (!x[i].getUniqueName().equals(y[i].getUniqueName())) {
							return false;
						}
					}
					return true;
				}
			};
		}
	}
}

/////////////////////////////////////////////////////////////////////////////
// End of OpeningSetFunDef.java
