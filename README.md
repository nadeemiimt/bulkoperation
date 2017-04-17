User manual:-

1- This project uses ColumnAttribute and KeyAttribute of EF Entities. To recognize child the library has custom ChildAttribute which need to be attached to child.
To use this library all POCO class should have Column and Key attributes and child attributes like as below:-

public class ParentEntity
{
[Key, Column("parentId")]
public int Id{get;set;}

[Column("parentName")]
public string Name{get;set;}

[Child("childTable")]   // childTable is the name of childtable in database
public ICollection<ChildEntity> Childs{get;set;}
}

public class ChildEntity
{
[Key, Column("childId")]
public int Id{get;set;}

[Column("childName")]   // childName is the name of column in database
public string Name{get;set;}

[Column("parentId")]
public int ParentId{get;set;}

public ParentEntity Parent {get;set;}
}

The bulk library has three methods :-
public void BulkInsert<T>(IEnumerable<T> list, string parentTableName) where T : class
public void BulkInsert<T>(IEnumerable<T> list, string parentTableName, int batchSize) where T : class
public void DeleteEntity<T>(IEnumerable<T> dataList, string tableName) where T : class

which resides in Operation class.

You need to initialize the Operation class by passing "dbContext.Database" instance in constructor.
example :- 

using BulkOperation;
namespace Test
{
 internal class TestContext : DbContext
 {

 private Operation _bulkOperations;
 
 public TestContext() : base("DefaultConnectionString")
 {
  _bulkOperations = new Operation(Database);      
 }
 
 public void Insert(List<ParentEntity> parentData)
 {
 _bulkOperation.BulkInsert(parentData, "parentEntity");
 }
 
 public void Insert(List<ParentEntity> parentData, int batchSize)
 {
 _bulkOperation.BulkInsert(parentData, "parentEntity", batchSize);
 }
 
 public void Delete(List<ParentEntity> parentData)
 {
 _bulkOperation.BulkDelete(new List<ParentEntity>(), "parentEntity");
 }
 
 public void Delete(List<ChildEntity> childData)
 {
 _bulkOperation.BulkDelete(childData, "childTable");
 }
 
 }
 
}

Insert has two method based on if you want batch insert or default insert and delete has only one method.

Note:- 

Insert support parent and child insertion but only one condition it supports only one child attribute child can be single object or list of object.
Delete supports on single entity either parent or child it doesn't support both if they are passed in list.

