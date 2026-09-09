package org.apache.flink.sql.examples.join;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

//@FunctionHint(output = new DataTypeHint("Row"))
public class ColumnToRowFunction extends TableFunction<Row> {
    private static final long serialVersionUID = 538060868919905992L;

    @DataTypeHint("Row<param String>")
    public void eval(String[] arr) {
        for (String s : arr) {
            System.out.println("output: " + s);
            collect(Row.of(s));
        }
    }
}
