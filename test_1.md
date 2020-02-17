# test case 1
unions then join against all

|    |    |
|----|----|
| t1 | t4 |
| t2 | t4 |

```
{
  "base_table": {
    "name": "table_1",
    "operations": [{
      "type": "union",
      "name": "table_2"
    }]
  },
  "operations": [{
      "type": "join",
      "name": "table_4"
    }]
}
```

```
select ...
from (
  select ...
  from "table_1"
  union
  select ...
  from "table_2"
) t1
join table_4
  on t1.field = table_4.field
```
