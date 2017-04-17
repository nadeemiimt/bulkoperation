using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace BulkOperation
{
    public class Operation
    {
        Database _database;
        public Operation(Database database)
        {
            _database = database;
        }
        /// <summary>
        /// Insert parent child(if exist) in database using SQL bulk copy 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="parentTableName"></param>
        public void BulkInsert<T>(IEnumerable<T> list, string parentTableName) where T : class
        {
            using (SqlConnection conn = new SqlConnection(_database.Connection.ConnectionString))
            {
                conn.Open();
                using (SqlTransaction tran = conn.BeginTransaction())
                {
                    using (var bulkCopy = new SqlBulkCopy(_database.Connection.ConnectionString, SqlBulkCopyOptions.Default) { DestinationTableName = parentTableName })
                    {
                        using (var childBulkCopy = new SqlBulkCopy(_database.Connection.ConnectionString, SqlBulkCopyOptions.Default))
                        {
                            var table = new DataTable();
                            var childTable = new DataTable();
                            var columnNames = new List<string>();
                            var childColumnNames = new List<string>();
                            var items = list.ToList();
                            PropertyInfo[] props = typeof(T).GetProperties();
                            foreach (PropertyInfo prop in props)
                            {
                                object[] attributes = prop.GetCustomAttributes(true);
                                var attrs = attributes.Where(x => x.GetType() == typeof(ColumnAttribute)).ToList();
                                ChildAttribute childAttrs = attributes.FirstOrDefault(x => x.GetType() == typeof(ChildAttribute)) as ChildAttribute;
                                foreach (ColumnAttribute attr in attrs)
                                {
                                    if (attr != null)
                                    {
                                        Type propertyType;
                                        //// check if nullable type if yes then select type 
                                        if (prop.PropertyType.IsGenericType &&
                                            prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                                        {
                                            propertyType = prop.PropertyType.GetGenericArguments()[0];
                                        }
                                        else
                                        {
                                            propertyType = prop.PropertyType;
                                        }
                                        table.Columns.Add(new DataColumn(attr.Name, propertyType));
                                        bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(attr.Name, attr.Name));
                                        columnNames.Add(prop.Name);
                                    }
                                }
                                if (childAttrs != null)
                                {
                                    childBulkCopy.DestinationTableName = childAttrs.Name;
                                    var childObject = prop.PropertyType;
                                    Type childObjectType;
                                    //// check if child type is single object or list of object
                                    if (childObject.Name.Contains("ICollection"))
                                    {
                                        childObjectType = childObject.GetGenericArguments().FirstOrDefault();
                                    }
                                    else
                                    {
                                        childObjectType = childObject;
                                    }
                                    if (childObjectType != null)
                                    {
                                        string parentName = string.Empty;
                                        PropertyInfo[] childProps = childObjectType.GetProperties();
                                        foreach (PropertyInfo cProp in childProps)
                                        {
                                            object[] childAttributes = cProp.GetCustomAttributes(true);
                                            var childsAttrs = childAttributes.Where(x => x.GetType() == typeof(ColumnAttribute)).ToList();
                                            ForeignKeyAttribute parentAttrsName = childAttributes.FirstOrDefault(x => x.GetType() == typeof(ForeignKeyAttribute)) as ForeignKeyAttribute;
                                            if (parentAttrsName != null)
                                                parentName = parentAttrsName.Name;
                                            foreach (ColumnAttribute cAttr in childsAttrs)
                                            {
                                                if (cAttr != null)
                                                {
                                                    Type propertyType;
                                                    if (cProp.PropertyType.IsGenericType &&
                                                        cProp.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                                                    {
                                                        propertyType = cProp.PropertyType.GetGenericArguments()[0];
                                                    }
                                                    else
                                                    {
                                                        propertyType = cProp.PropertyType;
                                                    }
                                                    childTable.Columns.Add(new DataColumn(cAttr.Name, propertyType));
                                                    childBulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(cAttr.Name, cAttr.Name));
                                                    childColumnNames.Add(cProp.Name);
                                                }
                                            }
                                        }
                                        for (int itemIndex = 0; itemIndex < items.Count; itemIndex++)
                                        {
                                            dynamic childItems = items[itemIndex].GetType().GetProperty(prop.Name).GetValue(items[itemIndex], null);
                                            var isEnumerable = (childItems as System.Collections.IEnumerable) != null;
                                            if (isEnumerable)
                                            {
                                                for (int i = 0; i < childItems.Count; i++)
                                                {
                                                    var dataItems = new List<object>();
                                                    for (int j = 0; j < childColumnNames.Count; j++)
                                                    {
                                                        object value = null;
                                                        if (parentName == childColumnNames[j])
                                                        {
                                                            PropertyInfo[] parentProps = items[itemIndex].GetType().GetProperties();
                                                            foreach (PropertyInfo pProp in parentProps)
                                                            {
                                                                object[] parentAttributes = pProp.GetCustomAttributes(true);
                                                                var parentAttrs = parentAttributes.FirstOrDefault(x => x.GetType() == typeof(KeyAttribute));
                                                                if (parentAttrs != null)
                                                                {
                                                                    value = items[itemIndex].GetType().GetProperty(pProp.Name).GetValue(items[itemIndex], null);
                                                                }
                                                            }
                                                        }
                                                        else
                                                        {
                                                            value = childItems[i].GetType().GetProperty(childColumnNames[j]).GetValue(childItems[i], null);
                                                        }

                                                        if (value != null)
                                                        {
                                                            ////assign the value
                                                            dataItems.Add(value);
                                                        }
                                                        else
                                                        {
                                                            dataItems.Add(DBNull.Value);
                                                        }
                                                    }
                                                    childTable.Rows.Add(dataItems.ToArray());
                                                }
                                            }
                                            else
                                            {
                                                var dataItems = new List<object>();
                                                for (int j = 0; j < childColumnNames.Count; j++)
                                                {
                                                    object value = childItems.GetType().GetProperty(columnNames[j]).GetValue(childItems, null);
                                                    if (value != null)
                                                    {
                                                        ////assign the value
                                                        dataItems.Add(value);
                                                    }
                                                    else
                                                    {
                                                        dataItems.Add(DBNull.Value);
                                                    }
                                                }
                                                childTable.Rows.Add(dataItems.ToArray());
                                            }
                                        }
                                    }
                                }
                            }
                            for (int i = 0; i < items.Count; i++)
                            {
                                var dataItems = new List<object>();
                                for (int j = 0; j < columnNames.Count; j++)
                                {
                                    object value = items[i].GetType().GetProperty(columnNames[j]).GetValue(items[i], null);
                                    if (value != null)
                                    {
                                        ////assign the value
                                        dataItems.Add(value);
                                    }
                                    else
                                    {
                                        dataItems.Add(DBNull.Value);
                                    }
                                }
                                table.Rows.Add(dataItems.ToArray());
                            }
                            bulkCopy.WriteToServer(table);
                            if (childTable.Rows.Count > 0)
                                childBulkCopy.WriteToServer(childTable);
                            tran.Commit();
                        }
                    }
                }
            }
        }
        /// <summary>
        /// Insert parent child(if exist) in database using SQL bulk copy with batch parameters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="parentTableName"></param>
        /// <param name="batchSize"></param>
        public void BulkInsert<T>(IEnumerable<T> list, string parentTableName, int batchSize) where T : class
        {
            using (SqlConnection conn = new SqlConnection(_database.Connection.ConnectionString))
            {
                conn.Open();
                using (SqlTransaction tran = conn.BeginTransaction())
                {
                    using (var bulkCopy = new SqlBulkCopy(_database.Connection.ConnectionString, SqlBulkCopyOptions.Default) { DestinationTableName = parentTableName })
                    {
                        bulkCopy.BatchSize = batchSize;
                        using (var childBulkCopy = new SqlBulkCopy(_database.Connection.ConnectionString, SqlBulkCopyOptions.Default))
                        {
                            childBulkCopy.BatchSize = batchSize;
                            var table = new DataTable();
                            var childTable = new DataTable();
                            var columnNames = new List<string>();
                            var childColumnNames = new List<string>();
                            var items = list.ToList();
                            PropertyInfo[] props = typeof(T).GetProperties();
                            foreach (PropertyInfo prop in props)
                            {
                                object[] attributes = prop.GetCustomAttributes(true);
                                var attrs = attributes.Where(x => x.GetType() == typeof(ColumnAttribute)).ToList();
                                ChildAttribute childAttrs = attributes.FirstOrDefault(x => x.GetType() == typeof(ChildAttribute)) as ChildAttribute;
                                foreach (ColumnAttribute attr in attrs)
                                {
                                    if (attr != null)
                                    {
                                        Type propertyType;
                                        //// check if nullable type if yes then select type 
                                        if (prop.PropertyType.IsGenericType &&
                                            prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                                        {
                                            propertyType = prop.PropertyType.GetGenericArguments()[0];
                                        }
                                        else
                                        {
                                            propertyType = prop.PropertyType;
                                        }
                                        table.Columns.Add(new DataColumn(attr.Name, propertyType));
                                        bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(attr.Name, attr.Name));
                                        columnNames.Add(prop.Name);
                                    }
                                }
                                if (childAttrs != null)
                                {
                                    childBulkCopy.DestinationTableName = childAttrs.Name;
                                    var childObject = prop.PropertyType;
                                    Type childObjectType;
                                    //// check if child type is single object or list of object
                                    if (childObject.Name.Contains("ICollection"))
                                    {
                                        childObjectType = childObject.GetGenericArguments().FirstOrDefault();
                                    }
                                    else
                                    {
                                        childObjectType = childObject;
                                    }
                                    if (childObjectType != null)
                                    {
                                        string parentName = string.Empty;
                                        PropertyInfo[] childProps = childObjectType.GetProperties();
                                        foreach (PropertyInfo cProp in childProps)
                                        {
                                            object[] childAttributes = cProp.GetCustomAttributes(true);
                                            var childsAttrs = childAttributes.Where(x => x.GetType() == typeof(ColumnAttribute)).ToList();
                                            ForeignKeyAttribute parentAttrsName = childAttributes.FirstOrDefault(x => x.GetType() == typeof(ForeignKeyAttribute)) as ForeignKeyAttribute;
                                            if (parentAttrsName != null)
                                                parentName = parentAttrsName.Name;
                                            foreach (ColumnAttribute cAttr in childsAttrs)
                                            {
                                                if (cAttr != null)
                                                {
                                                    Type propertyType;
                                                    if (cProp.PropertyType.IsGenericType &&
                                                        cProp.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                                                    {
                                                        propertyType = cProp.PropertyType.GetGenericArguments()[0];
                                                    }
                                                    else
                                                    {
                                                        propertyType = cProp.PropertyType;
                                                    }
                                                    childTable.Columns.Add(new DataColumn(cAttr.Name, propertyType));
                                                    childBulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(cAttr.Name, cAttr.Name));
                                                    childColumnNames.Add(cProp.Name);
                                                }
                                            }
                                        }
                                        for (int itemIndex = 0; itemIndex < items.Count; itemIndex++)
                                        {
                                            dynamic childItems = items[itemIndex].GetType().GetProperty(prop.Name).GetValue(items[itemIndex], null);
                                            var isEnumerable = (childItems as System.Collections.IEnumerable) != null;
                                            if (isEnumerable)
                                            {
                                                for (int i = 0; i < childItems.Count; i++)
                                                {
                                                    var dataItems = new List<object>();
                                                    for (int j = 0; j < childColumnNames.Count; j++)
                                                    {
                                                        object value = null;
                                                        if (parentName == childColumnNames[j])
                                                        {
                                                            PropertyInfo[] parentProps = items[itemIndex].GetType().GetProperties();
                                                            foreach (PropertyInfo pProp in parentProps)
                                                            {
                                                                object[] parentAttributes = pProp.GetCustomAttributes(true);
                                                                var parentAttrs = parentAttributes.FirstOrDefault(x => x.GetType() == typeof(KeyAttribute));
                                                                if (parentAttrs != null)
                                                                {
                                                                    value = items[itemIndex].GetType().GetProperty(pProp.Name).GetValue(items[itemIndex], null);
                                                                }
                                                            }
                                                        }
                                                        else
                                                        {
                                                            value = childItems[i].GetType().GetProperty(childColumnNames[j]).GetValue(childItems[i], null);
                                                        }

                                                        if (value != null)
                                                        {
                                                            ////assign the value
                                                            dataItems.Add(value);
                                                        }
                                                        else
                                                        {
                                                            dataItems.Add(DBNull.Value);
                                                        }
                                                    }
                                                    childTable.Rows.Add(dataItems.ToArray());
                                                }
                                            }
                                            else
                                            {
                                                var dataItems = new List<object>();
                                                for (int j = 0; j < childColumnNames.Count; j++)
                                                {
                                                    object value = childItems.GetType().GetProperty(columnNames[j]).GetValue(childItems, null);
                                                    if (value != null)
                                                    {
                                                        ////assign the value
                                                        dataItems.Add(value);
                                                    }
                                                    else
                                                    {
                                                        dataItems.Add(DBNull.Value);
                                                    }
                                                }
                                                childTable.Rows.Add(dataItems.ToArray());
                                            }
                                        }
                                    }
                                }
                            }
                            for (int i = 0; i < items.Count; i++)
                            {
                                var dataItems = new List<object>();
                                for (int j = 0; j < columnNames.Count; j++)
                                {
                                    object value = items[i].GetType().GetProperty(columnNames[j]).GetValue(items[i], null);
                                    if (value != null)
                                    {
                                        ////assign the value
                                        dataItems.Add(value);
                                    }
                                    else
                                    {
                                        dataItems.Add(DBNull.Value);
                                    }
                                }
                                table.Rows.Add(dataItems.ToArray());
                            }
                            bulkCopy.WriteToServer(table);
                            if (childTable.Rows.Count > 0)
                                childBulkCopy.WriteToServer(childTable);
                            tran.Commit();
                        }
                    }
                }
            }
        }
        /// <summary>
        /// deletes data for given table data
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataList"></param>
        /// <param name="tableName"></param>
        public void DeleteEntity<T>(IEnumerable<T> dataList, string tableName) where T : class
        {
            PropertyInfo[] props = typeof(T).GetProperties();
            Type propertyType = null;
            string primaryKeyName = "";
            string keyName = "";
            foreach (PropertyInfo prop in props)
            {
                object[] attributes = prop.GetCustomAttributes(true);
                var attrs = attributes.Where(x => x.GetType() == typeof(KeyAttribute)).ToList();
                var columnAttrs = attributes.FirstOrDefault(x => x.GetType() == typeof(ColumnAttribute)) as ColumnAttribute;
                foreach (KeyAttribute attr in attrs)
                {
                    if (attr != null && columnAttrs != null)
                    {
                        if (prop.PropertyType.IsGenericType &&
                            prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                        {
                            propertyType = prop.PropertyType.GetGenericArguments()[0];
                        }
                        else
                        {
                            propertyType = prop.PropertyType;
                        }
                        primaryKeyName = columnAttrs.Name;
                        keyName = prop.Name;
                        break;
                    }
                }
            }
            var dataItems = dataList.ToList();
            var idList = new List<object>();
            for (int i = 0; i < dataItems.Count; i++)
            {
                object value = dataItems[i].GetType().GetProperty(keyName).GetValue(dataItems[i], null);
                if (value != null)
                {
                    idList.Add(value);
                }
            }
            var ids = "";

            if (propertyType == typeof(int) || propertyType == typeof(decimal) || propertyType == typeof(float) || propertyType == typeof(double))
            {
                ids = string.Join(",", Array.ConvertAll(idList.ToArray(), i => i.ToString()));
            }
            else
            {
                ids = string.Join(",", Array.ConvertAll(idList.ToArray(), i => "'" + i.ToString() + "'"));
            }

            _database.ExecuteSqlCommand($"DELETE FROM {tableName} WHERE {primaryKeyName} in ({ids})");
        }
    }
}
