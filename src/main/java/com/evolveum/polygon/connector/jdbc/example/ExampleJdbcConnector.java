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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
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
import org.identityconnectors.framework.common.objects.filter.ContainsFilter;
import org.identityconnectors.framework.common.objects.filter.EndsWithFilter;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.Filter;
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
import com.evolveum.polygon.connector.jdbc.InsertSQLBuilder;
import com.evolveum.polygon.connector.jdbc.JdbcUtil;
import com.evolveum.polygon.connector.jdbc.InsertOrUpdateSQL;
import com.evolveum.polygon.connector.jdbc.SQLParameter;
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
	private final static String COLUMN_WITH_STATUS = "STATUS";
	private final static String ATTR_STATUS = "status";
	
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

		LOGGER.info("executeQuery on {0}, filter: {1}, options: {2}", objectClass, query, options);

		
	}
	
	@Override
	public void delete(ObjectClass objectClass, Uid uid, OperationOptions option) {
		if (objectClass == null) {
			LOGGER.error("Attribute of type ObjectClass not provided.");
			throw new InvalidAttributeValueException("Attribute of type ObjectClass not provided.");
		}
//		if (option == null) {
//			LOGGER.error("Attribute of type OperationOptions not provided.");
//			throw new InvalidAttributeValueException("Attribute of type OperationOptions not provided.");
//		}
		
		if (uid == null) {
			LOGGER.error("Attribute of type uid not provided.");
			throw new InvalidAttributeValueException("Attribute of type uid not provided.");
		}
		
		if (objectClass.is(ObjectClass.ACCOUNT_NAME)) { // __ACCOUNT__
			
			DeleteBuilder deleteBuilder = new DeleteBuilder();
			executeUpdate(deleteBuilder.build(TABLE_OF_ACCOUNTS, uid, KEY_OF_ACCOUNTS_TABLE).toLowerCase());
			
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
			
			Name name = AttributeUtil.getNameFromAttributes(attributes);
			if(name == null){
				throw new InvalidAttributeValueException("Input attributes do not contains Name attribute.");
			}
			GuardedString password = AttributeUtil.getPasswordValue(attributes);
			if(password == null){
				throw new InvalidAttributeValueException("Input attributes do not contains password attribute.");
			}
			SQLRequest insertSQL = buildInsertOrUpdateSql(attributes, true, null);
			executeUpdate(insertSQL.getSql(), insertSQL.getParameters());
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
		excludedNames.add(COLUMN_WITH_STATUS);
		Set<AttributeInfo> attributesFromUsersTable = buildAttributeInfosFromTable(TABLE_OF_ACCOUNTS, KEY_OF_ACCOUNTS_TABLE, excludedNames);
		ObjectClassInfoBuilder accountObjClassBuilder = new ObjectClassInfoBuilder();
		
		AttributeInfoBuilder attrStatusBuilder = new AttributeInfoBuilder(ATTR_STATUS.toLowerCase());
		attrStatusBuilder.setType(Boolean.class);
		accountObjClassBuilder.addAttributeInfo(attrStatusBuilder.build());

		
		accountObjClassBuilder.addAllAttributeInfo(attributesFromUsersTable);
		
		accountObjClassBuilder.addAttributeInfo(OperationalAttributeInfos.PASSWORD);
		
		schemaBuilder.defineObjectClass(accountObjClassBuilder.build());
		return schemaBuilder.build();
	}
	
	public SQLRequest buildInsertOrUpdateSql(Set<Attribute> attributes, Boolean insert, Uid uid){
		
		InsertOrUpdateSQL requestBuilder;
		
		if(insert){
			requestBuilder = new InsertSQLBuilder();
		}else {
			requestBuilder = new UpdateSQLBuilder();
			((UpdateSQLBuilder)requestBuilder).setWhereClause(uid, KEY_OF_ACCOUNTS_TABLE.toLowerCase());
		}
		
		requestBuilder.setNameOfTable(TABLE_OF_ACCOUNTS);
		buildAttributeInfosFromTable(TABLE_OF_ACCOUNTS, KEY_OF_ACCOUNTS_TABLE, null);
		for(Attribute attr : attributes){
			if(attr.getName().equalsIgnoreCase(OperationalAttributes.PASSWORD_NAME)){
				final StringBuilder sbPass = new StringBuilder();
				((GuardedString)attr.getValue().get(0)).access(new GuardedString.Accessor() {
					@Override
					public void access(char[] chars) {
						sbPass.append(new String(chars));
					}
				});
				requestBuilder.setNameAndValueOfColumn(new SQLParameterBuilder().build(Types.VARCHAR, sbPass.toString(), COLUMN_WITH_PASSWORD.toLowerCase()));
			}else if(attr.getName().equalsIgnoreCase(Name.NAME)){
				requestBuilder.setNameAndValueOfColumn(new SQLParameterBuilder().build(Types.VARCHAR, ((Name)attr).getNameValue(), COLUMN_WITH_NAME.toLowerCase()));
			}else if(attr.getName().equalsIgnoreCase(ATTR_STATUS)){
				if((Boolean)attr.getValue().get(0)){
					requestBuilder.setNameAndValueOfColumn(new SQLParameterBuilder().build(Types.BOOLEAN, 1, attr.getName()));
				} else {
					requestBuilder.setNameAndValueOfColumn(new SQLParameterBuilder().build(Types.BOOLEAN, 0, attr.getName()));
				}
			}else{
				requestBuilder.setNameAndValueOfColumn(new SQLParameterBuilder().build(getSqlTypes().get(attr.getName()), attr.getValue().get(0), attr.getName()));
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
			throw new ConnectorException(ex.getMessage(), ex);
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
			executeUpdate(updateSQL.getSql(), updateSQL.getParameters());
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
			Collection<Filter> filters = ((OrFilter)query).getFilters();
			for(Filter filter : filters){
				if(sb.length() != 0){
					if(query instanceof OrFilter){
						sb.append(" OR ");
					} else if(query instanceof AndFilter){
						sb.append(" AND ");
					}
				}
				if((filter instanceof OrFilter || query instanceof AndFilter) && introduction.equalsIgnoreCase(" LIKE ")){
					sb.append("(").append(cretedBodyWhereClauseFromFilter(filter, introduction)).append(")");
				} else if(filter instanceof OrFilter && introduction.equalsIgnoreCase(" NOT LIKE ")){
					AndFilter and = new AndFilter(((OrFilter)filter).getLeft(), ((OrFilter)filter).getRight());
					sb.append("(").append(cretedBodyWhereClauseFromFilter(and, " NOT LIKE ")).append(")");
				} else if(query instanceof AndFilter && introduction.equalsIgnoreCase(" NOT LIKE ")){
					OrFilter and = new OrFilter(((AndFilter)filter).getLeft(), ((AndFilter)filter).getRight());
					sb.append("(").append(cretedBodyWhereClauseFromFilter(and, " NOT LIKE ")).append(")");
				} else {
					sb.append(cretedBodyWhereClauseFromFilter(filter, introduction));
				}
			}
			ret = sb.toString();
		} else if(query instanceof NotFilter){
			Filter filter = ((NotFilter)query).getFilter();
			ret = cretedBodyWhereClauseFromFilter(filter, " NOT LIKE ");
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
			sb.append(KEY_OF_ACCOUNTS_TABLE.toLowerCase()).append(" ").append(attr.getName()).append(introduction).append(start).append(((Uid)attr).getUidValue()).append(end);
		} else if(attr instanceof Name){
			sb.append(COLUMN_WITH_NAME.toLowerCase()).append(" ").append(attr.getName()).append(introduction).append(start).append(((Name)attr).getValue().get(0)).append(end);
		} else {
			sb.append(COLUMN_WITH_NAME.toLowerCase()).append(" ").append(attr.getName()).append(introduction).append(start).append(attr.getValue().get(0)).append(end);
		}
		return sb.toString();
	}
	
	private Uid returnUidFromName(String name){
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ").append(KEY_OF_ACCOUNTS_TABLE).append(" FROM ").append(TABLE_OF_ACCOUNTS).append(" WHERE ").append(COLUMN_WITH_NAME).append(" = '").append(name).append("'");
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
			throw new ConnectorException(ex.getMessage(), ex);
		}
		
		return ret;
	}
	
}
