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

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;

import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.FunDef;

/////////////////////////////////////////////////////////////////////////////
//

public class AbcValueFunDef extends AbcFunDefBase {
	static final ReflectiveMultiResolver Resolver =
		new ReflectiveMultiResolver
			( "AbcValue"
			, "AbcValue(<Member> | <Tuple>, <Member> | <Tuple>, <Set>[, <Member> | <Tuple>])"
			, "ABC test value"
			, new String[] {"fnttx", "fnttxt"}
			, AbcValueFunDef.class
			);

	public AbcValueFunDef(FunDef dummyFunDef) {
		super(dummyFunDef);
	}

	public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
		return compileCall(call, compiler, AbcResultType.Value);
	}
}

/////////////////////////////////////////////////////////////////////////////
// End of AbcValueFunDef.java
