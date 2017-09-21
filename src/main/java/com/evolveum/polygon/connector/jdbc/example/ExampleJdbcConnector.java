/**
 * Copyright (c) 2010-2017 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolveum.polygon.connector.jdbc.example;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Lukas Skublik
 *
 */

import java.util.Set;

import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationalAttributeInfos;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.AndFilter;
import org.identityconnectors.framework.common.objects.filter.CompositeFilter;
import org.identityconnectors.framework.common.objects.filter.ContainsFilter;
import org.identityconnectors.framework.common.objects.filter.EndsWithFilter;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.common.objects.filter.NotFilter;
import org.identityconnectors.framework.common.objects.filter.OrFilter;
import org.identityconnectors.framework.common.objects.filter.StartsWithFilter;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.TestOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;

import com.evolveum.polygon.connector.jdbc.AbstractJdbcConnector;
import com.evolveum.polygon.connector.jdbc.DeleteBuilder;
import com.evolveum.polygon.connector.jdbc.SQLRequest;
import com.evolveum.polygon.connector.jdbc.SelectSQLBuilder;
import com.evolveum.polygon.connector.jdbc.InsertSQLBuilder;
import com.evolveum.polygon.connector.jdbc.InsertOrUpdateSQL;
import com.evolveum.polygon.connector.jdbc.SQLParameterBuilder;
import com.evolveum.polygon.connector.jdbc.UpdateSQLBuilder;

import org.identityconnectors.framework.spi.operations.DeleteOp;

@ConnectorClass(displayNameKey = "connector.gitlab.rest.display", configurationClass = ExampleJdbcConfiguration.class)
public class ExampleJdbcConnector extends AbstractJdbcConnector<ExampleJdbcConfiguration> implements TestOp, SchemaOp, CreateOp, DeleteOp, UpdateOp, 
		SearchOp<Filter> {

	private static final Log LOGGER = Log.getLog(ExampleJdbcConnector.class);

	private final static String TABLE_OF_ACCOUNTS = "TestUsers";
	private final static String KEY_OF_ACCOUNTS_TABLE = "ID";
	private final static String COLUMN_WITH_PASSWORD = "PASSWORD";
	private final static String COLUMN_WITH_NAME = "USERNAME";
	private final static String LIKE = " LIKE ";
	private final static String NOT_LIKE = " NOT LIKE ";
	private final static String AND = " AND ";
	private final static String OR = " OR ";
	
	@Override
	public FilterTranslator<Filter> createFilterTranslator(ObjectClass objectClass, OperationOptions option) {
		return new FilterTranslator<Filter>() {
			@Override
			public List<Filter> translate(Filter filter) {
				return CollectionUtil.newList(filter);
			}
		};
	}

	@Override
	public void executeQuery(ObjectClass objectClass, Filter query, ResultsHandler handler, OperationOptions options) {
		if (objectClass == null) {
			LOGGER.error("Attribute of type ObjectClass not provided.");
			throw new InvalidAttributeValueException("Attribute of type ObjectClass is not provided.");
		}

		if (handler == null) {
			LOGGER.error("Attribute of type ResultsHandler not provided.");
			throw new InvalidAttributeValueException("Attribute of type ResultsHandler is not provided.");
		}
		
		if (options == null) {
			LOGGER.error("Attribute of type OperationOptions not provided.");
			throw new InvalidAttributeValueException("Attribute of type OperationOptions is not provided.");
		}
		
		SelectSQLBuilder selectSqlB = new SelectSQLBuilder();
		String whereClause = cretedWhereClauseFromFilter(query);
		String[] names = options.getAttributesToGet();
		
		if(names != null){
			String[] quotedName = new String[names.length];;
			for(int i =0; i < names.length; i++){
				quotedName[i] = quoteName(getConfiguration().getQuoting(), names[i]);
				i++;
			}
			selectSqlB.setAllNamesOfColumns(quotedName);
		}
		selectSqlB.addNameOfTable(TABLE_OF_ACCOUNTS);
		selectSqlB.setWhereClause(whereClause);
		String sqlRequest = selectSqlB.build();
		
		List<List<Attribute>> rowsOfTable = executeQueryOnTable(sqlRequest);
		
		
		convertListOfRowsToConnectorObject(rowsOfTable, handler);

		LOGGER.info("executeQuery on {0}, filter: {1}, options: {2}", objectClass, query, options);
	}
	
	private void convertListOfRowsToConnectorObject(List<List<Attribute>> rowsOfTable, ResultsHandler handler){
		
		for (List<Attribute> row : rowsOfTable){
			ConnectorObjectBuilder connObjB = new ConnectorObjectBuilder();
			connObjB.setObjectClass(ObjectClass.ACCOUNT);
			for(Attribute attr : row){
				if(attr.getName().equalsIgnoreCase(KEY_OF_ACCOUNTS_TABLE)){
					connObjB.addAttribute(new Uid(String.valueOf(attr.getValue().get(0))));
				} else if(attr.getName().equalsIgnoreCase(COLUMN_WITH_NAME)){
					connObjB.addAttribute(new Name(String.valueOf(attr.getValue().get(0))));
				} else if(attr.getName().equalsIgnoreCase(COLUMN_WITH_PASSWORD)){
					if(!getConfiguration().getSuppressPassword()){
						connObjB.addAttribute(AttributeBuilder.build(OperationalAttributes.PASSWORD_NAME,new GuardedString(String.valueOf(attr.getValue().get(0)).toCharArray())));
					}
				} else {
					connObjB.addAttribute(AttributeBuilder.build(attr.getName(),attr.getValue()));
				}
			}
			handler.handle(connObjB.build());
		}
	}
	
	@Override
	public void delete(ObjectClass objectClass, Uid uid, OperationOptions option) {
		if (objectClass == null) {
			LOGGER.error("Attribute of type ObjectClass not provided.");
			throw new InvalidAttributeValueException("Attribute of type ObjectClass not provided.");
		}
		if (option == null) {
			LOGGER.error("Attribute of type OperationOptions not provided.");
			throw new InvalidAttributeValueException("Attribute of type OperationOptions not provided.");
		}
		
		if (uid == null) {
			LOGGER.error("Attribute of type uid not provided.");
			throw new InvalidAttributeValueException("Attribute of type uid not provided.");
		}
		
		if (objectClass.is(ObjectClass.ACCOUNT_NAME)) { // __ACCOUNT__
			
			DeleteBuilder deleteBuilder = new DeleteBuilder();
			executeUpdateOnTable(deleteBuilder.build(TABLE_OF_ACCOUNTS, uid, quoteName(getConfiguration().getQuoting(), KEY_OF_ACCOUNTS_TABLE)).toLowerCase());
			
		}  else {
			LOGGER.error("Attribute of type ObjectClass is not supported.");
			throw new UnsupportedOperationException("Attribute of type ObjectClass is not supported.");
		}
		
	}

	@Override
	public Uid create(ObjectClass objectClass, Set<Attribute> attributes, OperationOptions option) {
		if (objectClass == null) {
			LOGGER.error("Attribute of type ObjectClass not provided.");
			throw new InvalidAttributeValueException("Attribute of type ObjectClass not provided.");
		}
		if (attributes == null) {
			LOGGER.error("Attribute of type Set<Attribute> not provided.");
			throw new InvalidAttributeValueException("Attribute of type Set<Attribute> not provided.");
		}
		if (option == null) {
			LOGGER.error("Attribute of type OperationOptions not provided.");
			throw new InvalidAttributeValueException("Attribute of type OperationOptions not provided.");
		}
		
		if (objectClass.is(ObjectClass.ACCOUNT_NAME)) { // __ACCOUNT__
			
			List<String> excludedNames = new ArrayList<String>();
			excludedNames.add(COLUMN_WITH_PASSWORD);
			excludedNames.add(COLUMN_WITH_NAME);
			buildAttributeInfosFromTable(TABLE_OF_ACCOUNTS, quoteName(getConfiguration().getQuoting(), KEY_OF_ACCOUNTS_TABLE), excludedNames);
			List<String> namesOfRequiredColumns = getNamesOfRequiredColumns();
			System.out.println("XXXXXXXXXXX "+namesOfRequiredColumns);
			Name name = AttributeUtil.getNameFromAttributes(attributes);
			
			if(getConfiguration().isEnableEmptyStr()) {
				LOGGER.info("Requested attributes which name will be not find in input attribute set  should be empty.");
				for(String nameOfReqColumn : namesOfRequiredColumns) {
					if(AttributeUtil.find(nameOfReqColumn, attributes) == null){
						attributes.add(AttributeBuilder.build(nameOfReqColumn, ""));
					}
				}
				if(name == null){
					attributes.add(new Name(""));
				}
			} else {
				
				if(name == null){
					throw new InvalidAttributeValueException("Input attributes do not contains Name attribute.");
				}
				
				for(String nameOfReqColumn : namesOfRequiredColumns) {
					if(AttributeUtil.find(nameOfReqColumn, attributes) == null){
						StringBuilder sb = new StringBuilder();
						sb.append("Input attributes do not contains required attribute ").append(nameOfReqColumn).append(".");
						throw new InvalidAttributeValueException(sb.toString());
					}
				}
			}
			
			GuardedString password = AttributeUtil.getPasswordValue(attributes);
			if(password == null){
				throw new InvalidAttributeValueException("Input attributes do not contains password attribute.");
			}
			
			
			SQLRequest insertSQL = buildInsertOrUpdateSql(attributes, true, null);
			executeUpdateOnTable(insertSQL.getSql(), insertSQL.getParameters());
			return returnUidFromName(name.getNameValue());
			
		}  else {
			LOGGER.error("Attribute of type ObjectClass is not supported.");
			throw new UnsupportedOperationException("Attribute of type ObjectClass is not supported.");
		}
	}
	

	@Override
	public Schema schema() {
		
		SchemaBuilder schemaBuilder = new SchemaBuilder(ExampleJdbcConnector.class);
		List<String> excludedNames = new ArrayList<String>();
		excludedNames.add(COLUMN_WITH_PASSWORD);
		Set<AttributeInfo> attributesFromUsersTable = buildAttributeInfosFromTable(TABLE_OF_ACCOUNTS, quoteName(getConfiguration().getQuoting(), KEY_OF_ACCOUNTS_TABLE), excludedNames);
		ObjectClassInfoBuilder accountObjClassBuilder = new ObjectClassInfoBuilder();
		
		accountObjClassBuilder.addAllAttributeInfo(attributesFromUsersTable);
		
		accountObjClassBuilder.addAttributeInfo(OperationalAttributeInfos.PASSWORD);
		
		schemaBuilder.defineObjectClass(accountObjClassBuilder.build());
		return schemaBuilder.build();
	}
	
	private SQLRequest buildInsertOrUpdateSql(Set<Attribute> attributes, Boolean insert, Uid uid){
		
		InsertOrUpdateSQL requestBuilder;
		
		if(insert){
			requestBuilder = new InsertSQLBuilder();
		}else {
			requestBuilder = new UpdateSQLBuilder();
			((UpdateSQLBuilder)requestBuilder).setWhereClause(uid, KEY_OF_ACCOUNTS_TABLE.toLowerCase());
		}
		
		requestBuilder.setNameOfTable(quoteName(getConfiguration().getQuoting(), TABLE_OF_ACCOUNTS));
		for(Attribute attr : attributes){
			if(attr.getName().equalsIgnoreCase(OperationalAttributes.PASSWORD_NAME)){
				final StringBuilder sbPass = new StringBuilder();
				((GuardedString)attr.getValue().get(0)).access(new GuardedString.Accessor() {
					@Override
					public void access(char[] chars) {
						sbPass.append(new String(chars));
					}
				});
				requestBuilder.setNameAndValueOfColumn(new SQLParameterBuilder().build(Types.VARCHAR, sbPass.toString(), quoteName(getConfiguration().getQuoting(), COLUMN_WITH_PASSWORD).toLowerCase()));
			}else if(attr.getName().equalsIgnoreCase(Name.NAME)){
				requestBuilder.setNameAndValueOfColumn(new SQLParameterBuilder().build(Types.VARCHAR, ((Name)attr).getNameValue(), quoteName(getConfiguration().getQuoting(), COLUMN_WITH_NAME).toLowerCase()));
			}else{
				requestBuilder.setNameAndValueOfColumn(new SQLParameterBuilder().build(getSqlTypes().get(attr.getName()), attr.getValue().get(0), quoteName(getConfiguration().getQuoting(), attr.getName())));
			}
		}
		
		return requestBuilder.build();
	} 
	
	@Override
	public void test() {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT * FROM ").append(TABLE_OF_ACCOUNTS).append(" WHERE ").append(KEY_OF_ACCOUNTS_TABLE).append(" IS NULL");
		String sql = sb.toString();
		
		Statement stmt = null;
		try {
			stmt = getConnection().createStatement();
			stmt.executeQuery(sql);
		} catch (SQLException ex) {
			LOGGER.error(ex.getMessage());
			if(rethrowSQLException(ex.getErrorCode())){
				throw new ConnectorException(ex.getMessage(), ex);
			}
		}
	}

	@Override
	public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> attributes, OperationOptions option) {
		if (objectClass == null) {
			LOGGER.error("Attribute of type ObjectClass not provided.");
			throw new InvalidAttributeValueException("Attribute of type ObjectClass not provided.");
		}
		if (attributes == null) {
			LOGGER.error("Attribute of type Set<Attribute> not provided.");
			throw new InvalidAttributeValueException("Attribute of type Set<Attribute> not provided.");
		}
		if (option == null) {
			LOGGER.error("Attribute of type OperationOptions not provided.");
			throw new InvalidAttributeValueException("Attribute of type OperationOptions not provided.");
		}
		
		if (uid == null) {
			LOGGER.error("Attribute of type uid not provided.");
			throw new InvalidAttributeValueException("Attribute of type uid not provided.");
		}
		
		if (objectClass.is(ObjectClass.ACCOUNT_NAME)) { // __ACCOUNT__
			
			SQLRequest updateSQL = buildInsertOrUpdateSql(attributes, false, uid);
			executeUpdateOnTable(updateSQL.getSql(), updateSQL.getParameters());
			Uid newUid = AttributeUtil.getUidAttribute(attributes);
			if(newUid == null){
				return uid;
			} else {
				return newUid;
			}
			
		}  else {
			LOGGER.error("Attribute of type ObjectClass is not supported.");
			throw new UnsupportedOperationException("Attribute of type ObjectClass is not supported.");
		}
	}
	
	private static String quoteName(String quoting, String value) {
        StringBuilder sb = new StringBuilder();
        if ("NONE".equalsIgnoreCase(quoting) || StringUtil.isBlank(quoting)) {
        	return value;
        }
        switch(quoting.toLowerCase()){
        	case "single": {
        		sb.append("'").append(value).append("'");
        		break;
        	}
        	case "double":{
        		sb.append("\"").append(value).append("\"");
        		break;
        	}
        	case "back":{
        		sb.append("`").append(value).append("`");
        		break;
        	}
        	case "brackets":{
        		sb.append("[").append(value).append("]");
        		break;
        	}
        	default:{
        		throw new IllegalArgumentException("Configuration attribute 'quoting' have wrong format.");
        	}
        }
        return sb.toString();
    }
	
	private String cretedWhereClauseFromFilter(Filter query){
		StringBuilder sb = new StringBuilder();
		sb.append("WHERE ").append(cretedBodyWhereClauseFromFilter(query, " LIKE "));
		return sb.toString();
	}
	
	private String cretedBodyWhereClauseFromFilter(Filter query, String introduction){
		String ret = "";
		if(query instanceof EqualsFilter){
			Attribute attr = ((EqualsFilter)query).getAttribute();
			ret = buildBodyWhereClause(introduction, "'", "'", attr);
		} else if(query instanceof ContainsFilter){
			Attribute attr = ((ContainsFilter)query).getAttribute();
			ret = buildBodyWhereClause(introduction, "'%", "%'", attr);
		} else if(query instanceof StartsWithFilter){
			Attribute attr = ((StartsWithFilter)query).getAttribute();
			ret = buildBodyWhereClause(introduction, "'", "%'", attr);
		} else if(query instanceof EndsWithFilter){
			Attribute attr = ((EndsWithFilter)query).getAttribute();
			ret = buildBodyWhereClause(introduction, "'%", "'", attr);
		} else if(query instanceof OrFilter || query instanceof AndFilter){
			StringBuilder sb = new StringBuilder();
			Collection<Filter> filters = ((CompositeFilter)query).getFilters();
			for(Filter filter : filters){
				if(sb.length() != 0){
					if(introduction.equalsIgnoreCase(NOT_LIKE)){
						if(query instanceof AndFilter){
							sb.append(OR);
						} else if(query instanceof OrFilter){
							sb.append(AND);
						}
					}else {
						if(query instanceof OrFilter){
							sb.append(OR);
						} else if(query instanceof AndFilter){
							sb.append(AND);
						}
					}
				}
				if((filter instanceof OrFilter || filter instanceof AndFilter) && introduction.equalsIgnoreCase(LIKE)){
					sb.append("(").append(cretedBodyWhereClauseFromFilter(filter, introduction)).append(")");
				} else if(filter instanceof OrFilter && introduction.equalsIgnoreCase(NOT_LIKE)){
					AndFilter and = new AndFilter(FilterBuilder.not(((OrFilter)filter).getLeft()), FilterBuilder.not(((OrFilter)filter).getRight()));
					sb.append("(").append(cretedBodyWhereClauseFromFilter(and, LIKE)).append(")");
				} else if(filter instanceof AndFilter && introduction.equalsIgnoreCase(NOT_LIKE)){
					OrFilter or = new OrFilter(FilterBuilder.not(((AndFilter)filter).getLeft()), FilterBuilder.not(((AndFilter)filter).getRight()));
					sb.append("(").append(cretedBodyWhereClauseFromFilter(or, LIKE)).append(")");
				} else {
					sb.append(cretedBodyWhereClauseFromFilter(filter, introduction));
				}
			}
			ret = sb.toString();
		} else if(query instanceof NotFilter){
			Filter filter = ((NotFilter)query).getFilter();
			if(introduction.equalsIgnoreCase(NOT_LIKE)){
				ret = cretedBodyWhereClauseFromFilter(filter, LIKE);
			} else {
				ret = cretedBodyWhereClauseFromFilter(filter, NOT_LIKE);
			}
		} else if(query == null){
			return ret;
		} else {
			StringBuilder sb = new StringBuilder();
			sb.append("Unexpected filter ").append(query.getClass());
			LOGGER.error(sb.toString());
			throw new ConnectorIOException(sb.toString());
		}
		return ret;
	}
		
	private String buildBodyWhereClause(String introduction, String start, String end ,Attribute attr){
		StringBuilder sb = new StringBuilder();
		if(attr instanceof Uid){
			sb.append(KEY_OF_ACCOUNTS_TABLE.toLowerCase()).append(" ").append(introduction).append(start).append(((Uid)attr).getUidValue()).append(end);
		} else if(attr instanceof Name){
			sb.append(COLUMN_WITH_NAME.toLowerCase()).append(" ").append(introduction).append(start).append(((Name)attr).getValue().get(0)).append(end);
		} else if(attr.getName().equalsIgnoreCase(OperationalAttributes.PASSWORD_NAME)){
			StringBuilder sbMessage = new StringBuilder();
			sbMessage.append("Illegal search with attribute ").append(attr.getName()).append(".");
			LOGGER.error(sbMessage.toString());
			throw new InvalidAttributeValueException(sbMessage.toString());
		} else{
			sb.append(attr.getName().toLowerCase()).append(introduction).append(start).append(attr.getValue().get(0)).append(end);
		}
		return sb.toString();
	}
	
	private Uid returnUidFromName(String name){
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ").append(quoteName(getConfiguration().getQuoting(), KEY_OF_ACCOUNTS_TABLE)).append(" FROM ").append(TABLE_OF_ACCOUNTS).append(" WHERE ").append(COLUMN_WITH_NAME).append(" = '").append(name).append("'");
		String sql = sb.toString();
		Uid ret = null;
		ResultSet result = null;
		Statement stmt = null;
		try {
			stmt = getConnection().createStatement();

			result = stmt.executeQuery(sql);
			result.next();
			int uid = result.getInt(1);
			ret = new Uid(String.valueOf(uid));
		} catch (SQLException ex) {
			LOGGER.error(ex.getMessage());
			if(rethrowSQLException(ex.getErrorCode())){
				throw new ConnectorException(ex.getMessage(), ex);
			}
		}
		
		return ret;
	}
	
}
