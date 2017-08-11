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
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Syntax;

/////////////////////////////////////////////////////////////////////////////
//

public class VarCoeffFunDef extends FunDefBase
{
	static final ReflectiveMultiResolver Resolver =
		new ReflectiveMultiResolver
			( "VarCoeff"
			, "VarCoeff(<Set>[, <Member> | <Tuple>])"
			, "Coefficient of variation"
			, new String[] {"fnx", "fnxn"}
			, VarCoeffFunDef.class
			);

	public VarCoeffFunDef(FunDef dummyFunDef)
	{
		super(dummyFunDef);
	}

	public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler)
	{
		final Exp[] args = call.getArgs();
		final Exp varCoeffExp =
			compiler.getValidator().validate
				( new UnresolvedFunCall
					( "/"
					, Syntax.Infix
					, new Exp[]
						{ new UnresolvedFunCall
							( StdevFunDef.StdevResolver.getName()
							, StdevFunDef.StdevResolver.getSyntax()
							, args
							)
						, new UnresolvedFunCall
							( AvgFunDef.Resolver.getName()
							, AvgFunDef.Resolver.getSyntax()
							, args
							)
						}
					)
				, true
				);

		return compiler.compileScalar(varCoeffExp, true);
	}
}

/////////////////////////////////////////////////////////////////////////////
// End of VarCoeff.java
