package com.centric.dqm.data;

import java.sql.SQLException;

import javax.sql.RowSet;
import javax.sql.rowset.Predicate;

public class RowsetStringFilter implements Predicate {

    private String[] values;
    private String columnName = null;
    private int columnNumber = -1;
    
    public RowsetStringFilter(String columnName, String[] values) {
        this.values = values;
        this.columnNumber = -1;
        this.columnName = columnName;
    }
    
    public RowsetStringFilter(int columnNumber, String[] values) {
        this.values = values;
        this.columnNumber = columnNumber;
        this.columnName = null;
    }

    @Override
    public boolean evaluate(RowSet rs) {
    	
    	 if (rs == null) return false;

         try {
             for (int i = 0; i < this.values.length; i++) {

                 String cityName = null;

                 if (this.columnNumber > 0) {
                     cityName = (String)rs.getObject(this.columnNumber);
                 } else if (this.columnName != null) {
                     cityName = (String)rs.getObject(this.columnName);
                 } else {
                     return false;
                 }

                 if (cityName.equalsIgnoreCase(values[i])) {
                     return true;
                 }
             }
         } catch (SQLException e) {
             return false;
         }
         
         return false;
         
    }

    @Override
    public boolean evaluate(Object valueArg, int colNumberArg) throws SQLException {

        if (colNumberArg == this.columnNumber) {
            for (int i = 0; i < this.values.length; i++) {
                if (this.values[i].equalsIgnoreCase((String)valueArg)) {
                    return true;
                }
            }
        }
        return false;
    	
    }

    @Override
    public boolean evaluate(Object valueArg, String colNameArg) throws SQLException {
    	
        if (colNameArg.equalsIgnoreCase(this.columnName)) {
            for (int i = 0; i < this.values.length; i++) {
                if (this.values[i].equalsIgnoreCase((String) valueArg)) {
                    return true;
                }
            }
        }
        return false;
    }

}