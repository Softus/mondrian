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

public class AbcFunDef extends AbcFunDefBase {
	static final ReflectiveMultiResolver Resolver =
		new ReflectiveMultiResolver
			( "Abc"
			, "Abc(<Member> | <Tuple>, <Member> | <Tuple>, <Set>[, <Member> | <Tuple>])"
			, "ABC test category"
			, new String[] {"fSttx", "fSttxt"}
			, AbcFunDef.class
			);

	public AbcFunDef(FunDef dummyFunDef) {
		super(dummyFunDef);
	}

	public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
		return compileCall(call, compiler, AbcResultType.Category);
	}
}

/////////////////////////////////////////////////////////////////////////////
// End of AbcFunDef.java
