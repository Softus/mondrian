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

public class AbcPercentFunDef extends AbcFunDefBase {
	static final ReflectiveMultiResolver Resolver =
		new ReflectiveMultiResolver
			( "AbcPercent"
			, "AbcPercent(<Member> | <Tuple>, <Member> | <Tuple>, <Set>[, <Member> | <Tuple>])"
			, "ABC percent"
			, new String[] {"fnttx", "fnttxt"}
			, AbcPercentFunDef.class
			);

	public AbcPercentFunDef(FunDef dummyFunDef) {
		super(dummyFunDef);
	}

	public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
		return compileCall(call, compiler, AbcResultType.Percent);
	}
}

/////////////////////////////////////////////////////////////////////////////
// End of AbcFunDef.java
