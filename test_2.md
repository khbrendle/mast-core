# test case 2
join tables together then union

|    |    |
|----|----|
| t1 | t2 |
| t3 | t2 |

```
{
  "base_table": {
    "source": {"name": "table_1"},
    "operations": [{
      "type": "join",
      "source": {"name": "table_2"}
    }]
  },
  "operations": [{
      "type": "union",
      "source": {"name": "table_3"}
      operations: [{
        "type": "join",
        "source": {"name": "table_2"}
      ]
    }]
}
```

```
select ...
from t1
join t2
  on t1.field = t2.field
union
select ...
from t3
join t2
  on t3.field = t2.field
```
