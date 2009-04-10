package mondrian.olap.fun;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.MemberCalc;
import mondrian.calc.TupleCalc;

import mondrian.calc.impl.AbstractListCalc;

import mondrian.mdx.HierarchyExpr;
import mondrian.mdx.ResolvedFunCall;

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.Syntax;

import mondrian.olap.Validator;

import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;

public class OpeningSetFunDef extends FunDefBase {
	static final ReflectiveMultiResolver resolver =
		new ReflectiveMultiResolver
			( "OpeningSet"
			, "OpeningSet(<Set>[, <Tuple>][, BYSET | BYHIER])"
			, "Ordered opening set of axis set"
			, new String[] {"fxx", "fxxt", "fxxm", "fxxy", "fxxty", "fxxmy"}
			, OpeningSetFunDef.class
			);
	
	private enum Flag { BYHIER, BYSET }

	public OpeningSetFunDef(FunDef dummyFunDef) {
		super(dummyFunDef);
	}

	public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
		final Exp set = call.getArg(0);
		final SetType setType = (SetType)set.getType();
		final Type setElementType = setType.getElementType();
		final ListCalc listCalc = compiler.compileList(set);

		Exp tuple = call.getArgCount() > 1 ? call.getArg(1) : null;
		final Flag flag;
		
		if (null != tuple) {
			if (tuple instanceof Literal) {
				tuple = null;
				flag = getLiteralArg(call, 1, Flag.BYSET, Flag.class);
			} else {
				flag = getLiteralArg(call, 2, Flag.BYSET, Flag.class);
			}
		} else {
			flag = Flag.BYSET;
		}

		if (setType.getArity() == 1) {
			final MemberType setMemberType = 
				(MemberType)
					(setElementType instanceof TupleType
						? ((TupleType)setElementType).elementTypes[0]
						: setElementType
					);
			Exp memberExp = null;
				
			if (null != tuple) {
				final Type tupleType = tuple.getType(); 
				final String hierarchyUniqueName = setMemberType.getHierarchy().getUniqueName();
					
				if (tupleType instanceof TupleType) {
					final Type[] tupleElementTypes = ((TupleType)tupleType).elementTypes;
					
					for (int i = 0; i < tupleElementTypes.length; i++) {
						if (((MemberType)tupleElementTypes[i]).getHierarchy().getUniqueName().equals(hierarchyUniqueName)) {
							memberExp = ((ResolvedFunCall)tuple).getArgs()[i];
							break;
						}
					}
				} else {
					if (((MemberType)tupleType).getHierarchy().getUniqueName().equals(hierarchyUniqueName)) {
						memberExp = tuple;
					}
				}
			}
			
			if (null == memberExp) { 
				memberExp =
					new ResolvedFunCall
						( HierarchyCurrentMemberFunDef.instance
						, new Exp[] {new HierarchyExpr(setMemberType.getHierarchy())}
						, setMemberType
						);
			}
			
			final MemberCalc currentMemberCalc = compiler.compileMember(memberExp);
			
			return new AbstractListCalc(call, new Calc[] {listCalc, currentMemberCalc}) {
				@SuppressWarnings("unchecked")
                public List<?> evaluateList(Evaluator evaluator) {
					final List<Member> set = (List<Member>)listCalc.evaluateList(evaluator); 
					final Member currentItem = currentMemberCalc.evaluateMember(evaluator);
					Member item;
					
					for (int i = 0; i < set.size(); i++) {
						item = set.get(i);
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
					return Collections.EMPTY_LIST; 
				}
			};
		} else {
			final Type[] setMemberTypes = ((TupleType)setElementType).elementTypes;
			final Exp[] args = new Exp[setMemberTypes.length];
			
			if (null != tuple) {
				final Type tupleType = tuple.getType(); 
					
				if (tupleType instanceof TupleType) {
					final Type[] tupleElementTypes = ((TupleType)tupleType).elementTypes;
					
					for (int i = 0; i < tupleElementTypes.length; i++) {
						final String hierarchyUniqueName = ((MemberType)tupleElementTypes[i]).getHierarchy().getUniqueName();
						
						for (int j = 0; j < setMemberTypes.length; j++) {
							final MemberType memberType = (MemberType)setMemberTypes[j];
							
							if (hierarchyUniqueName.equals(memberType.getHierarchy().getUniqueName())) {
								args[j] = ((ResolvedFunCall)tuple).getArgs()[i];
								break;
							}
						}
					}
				} else {
					final String hierarchyUniqueName = ((MemberType)tupleType).getHierarchy().getUniqueName(); 
					
					for (int i = 0; i < setMemberTypes.length; i++) {
						final MemberType memberType = (MemberType)setMemberTypes[i];
						
						if (hierarchyUniqueName.equals(memberType.getHierarchy().getUniqueName())) {
							args[i] = tuple;
							break;
						}
					}
				}
			}			
			
			for (int i = 0; i < setMemberTypes.length; i++) {
				if (null == args[i]) {
					final MemberType memberType = (MemberType)setMemberTypes[i];
					
					args[i] =
						new ResolvedFunCall
							( HierarchyCurrentMemberFunDef.instance
							, new Exp[] {new HierarchyExpr(memberType.getHierarchy())}
							, memberType
							);
				}
			}
			
			final Validator validator = compiler.getValidator();
			final ResolvedFunCall currentTuple =
				new ResolvedFunCall
					( validator.getFunTable().getDef(args, validator, "()", Syntax.Parentheses)
					, args
					, setElementType
					);
			final TupleCalc currentTupleCalc = compiler.compileTuple(currentTuple);
			
			return new AbstractListCalc(call, new Calc[] {listCalc, currentTupleCalc}) {
				@SuppressWarnings("unchecked")
                public List<?> evaluateList(Evaluator evaluator) {
					final List<Member[]> set = (List<Member[]>)listCalc.evaluateList(evaluator);
					final Member[] currentTuple = currentTupleCalc.evaluateTuple(evaluator);
					final int length = currentTuple.length;
					Member[] tuple;

					for (int i = 0; i < set.size(); i++) {
						tuple = set.get(i);
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
										
										if (currentParent != parent && !currentParent.getUniqueName().equals(parent.getUniqueName())) {
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
// End of AxisOrderedHeadFunDef.java
