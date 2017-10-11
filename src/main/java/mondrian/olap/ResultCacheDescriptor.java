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

package mondrian.olap;

/////////////////////////////////////////////////////////////////////////////
//Imports

import java.util.Arrays;

import mondrian.calc.Calc;
import mondrian.calc.DummyExp;
import mondrian.olap.type.NullType;

/////////////////////////////////////////////////////////////////////////////
//

public class ResultCacheDescriptor extends ExpCacheDescriptor
{
	private static class ArgCashExp extends DummyExp
	{
		public ArgCashExp()
		{
			super(new NullType());
		}
		
		public void setArgs(Object[] args)
		{
			_args = args;
		}
		
		public boolean equals(Object obj)
        {
			return (obj instanceof ArgCashExp) && Arrays.deepEquals(_args, ((ArgCashExp)obj)._args);
        }

		public int hashCode()
        {
			return Arrays.deepHashCode(_args);
        }
		
		private Object[] _args;
	}

	public ResultCacheDescriptor(Calc calc, Evaluator evaluator)
	{
		super(new ArgCashExp(), calc, evaluator);
	}
	
	public void setArgs(Object... args)
	{
		((ArgCashExp)getExp()).setArgs(args);
	}
}

/////////////////////////////////////////////////////////////////////////////
// End of ResultCacheDescriptor.java
