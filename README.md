# calcite-avro-sample

## Schema

```
{
  "type" : "record",
  "name" : "test",
  "fields" : [ {
    "name" : "emp_id",
    "type" : [ "null", "long" ],
    "doc" : "Type inferred from '1'",
    "default" : null
  }, {
    "name" : "dept_id",
    "type" : [ "null", "long" ],
    "doc" : "Type inferred from '1'",
    "default" : null
  }, {
    "name" : "name",
    "type" : [ "null", "string" ],
    "doc" : "Type inferred from 'test1'",
    "default" : null
  }, {
    "name" : "created_at",
    "type" : [ "null", "string" ],
    "doc" : "Type inferred from '2019-02-17 10:00:00'",
    "default" : null
  }, {
    "name" : "updated_at",
    "type" : [ "null", "string" ],
    "doc" : "Type inferred from '2019-02-17 12:00:00'",
    "default" : null
  } ]
}
```

## Records

```
{"emp_id": 1, "dept_id": 1, "name": "test1", "created_at": "2019-02-17 10:00:00", "updated_at": "2019-02-17 12:00:00"}
{"emp_id": 2, "dept_id": 2, "name": "test2", "created_at": "2019-02-17 10:00:00", "updated_at": "2019-02-17 12:00:00"}
```