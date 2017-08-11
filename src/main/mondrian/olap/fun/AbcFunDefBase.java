//	Copyright (C) 2008, IDC
//
//	Project: 
//	File:	
//	Author:  Alexander Korsukov (akorsukov@gmail.com)
//	Date:
//
//	Description:
//		ABC test function
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

import mondrian.calc.impl.GenericCalc;

import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.UnresolvedFunCall;

import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.ResultCacheDescriptor;
import mondrian.olap.Syntax;
import mondrian.olap.Util;
import mondrian.olap.Validator;

import mondrian.olap.type.MemberType;
import mondrian.olap.type.Type;

/////////////////////////////////////////////////////////////////////////////
//

public class AbcFunDefBase extends FunDefBase
{

	protected enum AbcResultType
	{
		Percent,
		Value,
		Category
	}
	
	protected enum AbcCategory
	{
		None,
		A,
		B,
		C
	}

	protected static class AbcResult implements Comparable<AbcResult>
	{
		public AbcResult(double pct)
		{
			_pct = pct;
			_category = AbcCategory.None;
		}

		final public double pct()
		{
			return _pct;
		}

		final public void set(double value, AbcCategory category)
		{
			_value = value;
			_category = category;
		}

		final public Double getPercent()
		{
			return (_category != AbcCategory.None) ? _pct : null;
		}

		final public Double getValue()
		{
			return (_category != AbcCategory.None) ? _value : null;
		}

		final public String getCategory()
		{
			return (_category != AbcCategory.None) ? _category.toString() : null;
		}

		public int compareTo(AbcResult o)
		{
			if (null != o)
			{
				return
					(_pct < o._pct) ? 1
					: (_pct > o._pct) ? -1
					: 0;
			}
			return -1;
		}

		public String toString()
		{
			return 
				(_pct <= 0) ? "[]"
				: (_category == AbcCategory.None) ? String.format("[%.2f%%]", _pct * 100)
				: String.format("[%.2f%%, %.2f%%, %s]", _pct * 100, _value * 100, _category);
		}

		private double _pct;
		private double _value;
		private AbcCategory _category;
	}

	public AbcFunDefBase(FunDef dummyFunDef)
	{
		super(dummyFunDef);
	}
	
	protected Exp validateArg(Validator validator, Exp[] args, int i, int category)
	{
		// If expression cache is enabled, wrap third expression (the set)
		// in a function which will use the expression cache.
		return
			(i == 2 && MondrianProperties.instance().EnableExpCache.get())
				? validator.validate
					( new UnresolvedFunCall(CacheFunDef.NAME, Syntax.Function, new Exp[] {args[i]})
					, false
					)
				: super.validateArg(validator, args, i, category);
	}

	protected Calc compileCall(ResolvedFunCall call, ExpCompiler compiler, AbcResultType resultType)
	{
		final Exp measure = call.getArg(0);
		final Exp item = call.getArg(1);
		final Exp set = call.getArg(2);
		final Exp total = call.getArgCount() > 3 ? call.getArg(3) : null;

		final Type measureType = measure.getType();
		final Calc measureCalc =
			(measureType instanceof MemberType)
				? compiler.compileMember(measure)
				: compiler.compileTuple(measure);
		
		
		final Type itemType = item.getType();
		final Calc itemCalc =
			(itemType instanceof MemberType)
				? compiler.compileMember(item)
				: compiler.compileTuple(item);

		final ListCalc setCalc = compiler.compileList(set);

		final Type totalType = null != total ? total.getType() : null;
		final Calc totalCalc =
			(null == totalType)
				? null
				: (totalType instanceof MemberType)
					? compiler.compileMember(total)
					: compiler.compileTuple(total);

		return
			new AbcCalc
				( compiler.getEvaluator()
				, resultType
				, call
				, measureCalc, itemCalc, setCalc, totalCalc
				);
	}
	
	private static class AbcCalc extends GenericCalc
	{
		protected AbcCalc
			( Evaluator evaluator
			, AbcResultType resultType
			, Exp exp
			, Calc measureCalc, Calc itemCalc, ListCalc setCalc, Calc totalCalc
			)
		{
			super(exp);
			
			_resultType = resultType;

			_measureCalc = measureCalc;
			_itemCalc = itemCalc;
			_setCalc = setCalc;
			_totalCalc = totalCalc;
			_calcs =
				null != totalCalc
					? new Calc[] {measureCalc, itemCalc, setCalc, totalCalc}
					: new Calc[] {measureCalc, itemCalc, setCalc};
					
			_cacheDescriptor = new ResultCacheDescriptor(this, evaluator);
		}

		public Calc[] getCalcs()
		{
			return _calcs;
		}
		
		public boolean dependsOn(Dimension dimension)
		{
			if (_measureCalc.getType().usesDimension(dimension, true))
				return false;
			
			if (_itemCalc.getType().usesDimension(dimension, true))
				return false;
			
			if (_setCalc.getType().usesDimension(dimension, true))
			{
				if (null != _totalCalc || _totalCalc.getType().usesDimension(dimension, true))
					return false;
			}
			
			return true;
		}

		public Object evaluate(final Evaluator evaluator)
		{
			if (_evaluating)
				return performAbcTest(evaluator);
			
			final Object item = _itemCalc.evaluate(evaluator);
			
			if (null != item)
			{
				_set = _setCalc.evaluateList(evaluator);
				
				final int itemIndex = findItem(evaluator, _set, item);

				if (itemIndex >= 0)
				{
					_measure = _measureCalc.evaluate(evaluator);
					_total = (null != _totalCalc) ? _totalCalc.evaluate(evaluator) : null;
					
					final List<AbcResult> result = lookUpFromCache(evaluator);
					
					if (null != result)
					{
						final AbcResult abc = result.get(itemIndex);
						
						switch (_resultType)
						{
						case Percent:
							return abc.getPercent();
							
						case Value:
							return abc.getValue();
							
						case Category:
							return abc.getCategory();
						}
					}
				}
			}
			return null;
		}
		
		@SuppressWarnings("unchecked")
		private List<AbcResult> lookUpFromCache(final Evaluator evaluator)
		{
			if (MondrianProperties.instance().EnableExpCache.get())
			{
				try
				{
					_evaluating = true;
					if (null != _total)
						_cacheDescriptor.setArgs(_measure, _set, _total);
					else
						_cacheDescriptor.setArgs(_measure, _set);
					
					return (List<AbcResult>)evaluator.getCachedResult(_cacheDescriptor);
				}
				finally
				{
					_evaluating = false;
				}
			}
			else
				return performAbcTest(evaluator);
		}

		protected List<AbcResult> performAbcTest(Evaluator evaluator)
		{
			final List<AbcResult> abcSet = calcDistribution(evaluator);
			
			if (null != abcSet)
			{
				final List<AbcResult> sortedAbc = new ArrayList<AbcResult>(abcSet);

				Collections.sort(sortedAbc);
				double cum = 0;
				AbcCategory category = AbcCategory.None;

				for (AbcResult abc : sortedAbc)
				{
					if (abc.pct() > 0)
					{
						cum += abc.pct();
						category =
							(cum <= A_THRESHOLD || category == AbcCategory.None) ? AbcCategory.A
							: (cum <= B_THRESHOLD || category == AbcCategory.A) ? AbcCategory.B
							: AbcCategory.C;

						abc.set(cum, category);
					}
					else
						break;
				}
			}
			return abcSet;
		}

		protected List<AbcResult> calcDistribution(Evaluator evaluator)
		{
			final List<Double> vs = evaluateSet(evaluator.push(), _set, _measure);
			final Double t = (null != _total) ? (Double)evaluateDouble(evaluator, _total, _measure) : sum(vs);
			
			if (isValidDouble(t))
			{
				final List<AbcResult> abcResults = new ArrayList<AbcResult>(vs.size());

				for (Double v : vs)
					abcResults.add(new AbcResult(isValidDouble(v) ? v / t : 0));

				return abcResults;
			}
			return null;
		}

		protected static boolean isValidDouble(Double d)
		{
			return null != d && !d.isNaN() && d > 0;
		}

		protected static List<Double> evaluateSet
			( Evaluator evaluator
			, List<?> members
			, Object measure
			)
		{
			final List<Double> l = new ArrayList<Double>(members.size());
			final boolean tuples = (members.get(0) instanceof Member[]);  
			
			for (Object m : members)
			{
				if (tuples)
					evaluator.setContext((Member[])m);
				else
					evaluator.setContext((Member)m);

				l.add(evaluateDouble(evaluator, measure));
			}
			return l;
		}
		
		protected static Double evaluateDouble(Evaluator evaluator, Object total, Object measure)
		{
			final Member[] members =
				(total instanceof Member[])
					? (Member[])total
					: new Member[] {(Member)total};
			final Member[] savedMembers = new Member[members.length];
			
			for (int i = 0; i < members.length; i++)
			{
				if (null == members[i] || members[i].isNull())
				{
					for (int j = 0; j < i; j++)
						evaluator.setContext(savedMembers[j]);
					return null;
				}
				savedMembers[i] = evaluator.setContext(members[i]);
			}

			final Double d = evaluateDouble(evaluator, measure);

			evaluator.setContext(savedMembers);
			return d;
		}
		
		protected static Double evaluateDouble(Evaluator evaluator, Object measure)
		{
			final Member[] members =
				(measure instanceof Member[])
					? (Member[])measure
					: new Member[] {(Member)measure};
			final Member[] savedMembers = new Member[members.length];

			for (int i = 0; i < members.length; i++)
			{
				if (null == members[i] || members[i].isNull())
				{
					for (int j = 0; j < i; j++)
						evaluator.setContext(savedMembers[j]);
					return null;
				}
				savedMembers[i] = evaluator.setContext(members[i]);
			}

			if (evaluator.needToReturnNullForUnrelatedDimension(members))
			{
				evaluator.setContext(savedMembers);
				return null;
			}

			final Object o = evaluator.evaluateCurrent();

			evaluator.setContext(savedMembers);

			if (o == null || o == Util.nullValue)
				return null;
			else if (o instanceof Throwable)
				return null;
			else if (o instanceof Double)
				return (Double)o;
			else if (o instanceof Number)
				return ((Number)o).doubleValue();
			else
				return null;
		}

		protected static Double sum(List<Double> l)
		{
			Double s = null;

			for (Double d : l)
			{
				if (null != d)
					s = (null == s ? d : s + d);
			}
			return s;
		}
		
		protected static int findItem(final Evaluator evaluator, final List<?> set, final Object item)
		{
			if (set.size() > 0)
			{
				final Object firstItem = set.get(0);

				if (firstItem instanceof Member[])
				{
					final Member[] firstMembers = (Member[])firstItem;
					final Member[] itemMembers = new Member[firstMembers.length];
					
					for (int i = 0; i < firstMembers.length; i++)
					{
						if (item instanceof Member[])
						{
							final Member[] members = (Member[])item;
							
							for (int j = 0; j < members.length; j++)
							{
								if (firstMembers[i].getDimension().equals(members[j].getDimension()))
								{
									itemMembers[i] = members[j];
									break;
								}
							}
						}
						else
						{
							final Member member = (Member)item;

							if (firstMembers[i].getDimension().equals(member.getDimension()))
								itemMembers[i] = member;
						}

						if (null == itemMembers[i]) 
							itemMembers[i] = evaluator.getContext(firstMembers[i].getDimension());
					}

					for (int i = 0; i < set.size(); i++)
					{
						final Member[] members = (Member[])set.get(i);
						
						for (int j = 0; /*true*/; j++)
						{
							if (j == members.length)
								return i;
							
							if (!members[j].equals(itemMembers[j]))
								break;
						}
					}
				}
				else
				{
					final Member firstMember = (Member)firstItem;
					Member itemMember = null;
					
					if (item instanceof Member[])
					{
						final Member[] members = (Member[])item;
						
						for (int j = 0; j < members.length; j++)
						{
							if (firstMember.getDimension().equals(members[j].getDimension()))
							{
								itemMember = members[j];
								break;
							}
						}
					}
					else
					{
						final Member member = (Member)item;

						if (firstMember.getDimension().equals(member.getDimension()))
							itemMember = member;
					}

					if (null == itemMember) 
						itemMember = evaluator.getContext(firstMember.getDimension());
					
					for (int i = 0; i < set.size(); i++)
					{
						final Member member = (Member)set.get(i);
						
						if (member.equals(itemMember))
							return i;
					}
				}
			}
			return -1;
		}

		private static final double A_THRESHOLD = 0.5d;
		private static final double B_THRESHOLD = 0.8d;

		private final AbcResultType _resultType;

		private final Calc _measureCalc;
		private final Calc _itemCalc;
		private final ListCalc _setCalc;
		private final Calc _totalCalc;	

		private final ResultCacheDescriptor _cacheDescriptor;
		private boolean _evaluating;
		
		private List<?> _set;
		private Object _measure;
		private Object _total;	

		private final Calc[] _calcs;
	}
}

/////////////////////////////////////////////////////////////////////////////
// End of AbcFunDefBase.java
