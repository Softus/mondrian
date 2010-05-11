/*
// $Id: //open/mondrian-release/3.1/src/main/mondrian/calc/CalcWriter.java#2 $
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import java.io.PrintWriter;

/**
 * Visitor which serializes an expression to text.
 *
 * @author jhyde
 * @version $Id: //open/mondrian-release/3.1/src/main/mondrian/calc/CalcWriter.java#2 $
 * @since Dec 23, 2005
 */
public class CalcWriter {
    private final PrintWriter writer;

    public CalcWriter(PrintWriter writer) {
        this.writer = writer;
    }

    public PrintWriter getWriter() {
        return writer;
    }
}

// End CalcWriter.java
