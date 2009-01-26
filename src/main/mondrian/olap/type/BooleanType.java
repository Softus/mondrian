/*
// $Id: //open/mondrian-release/3.0/src/main/mondrian/olap/type/BooleanType.java#2 $
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

/**
 * The type of a boolean expression.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id: //open/mondrian-release/3.0/src/main/mondrian/olap/type/BooleanType.java#2 $
 */
public class BooleanType extends ScalarType {
    public BooleanType() {
        super("BOOLEAN");
    }

    public boolean equals(Object obj) {
        return obj instanceof BooleanType;
    }
}

// End BooleanType.java
