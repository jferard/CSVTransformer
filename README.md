# CSV Transformer - A CSV Transformer library in Python

Copyright (C) 2022 J. Férard <https://github.com/jferard>

License: GPLv3

This is a POC. See tests to understand the goal of the library.

## Format of the transformation dict
All entries are optional.

### The entity filter
Format :
```
"entity_filter" : <an expression using columns ids>
```

This filter lines before aggregating them.

### The default column transformation
Format :
```
"default_col" : {
    "normalize": <bool: default False>
    "visible": <bool: default True>
}
```

* `"normalize"` means that the column names are normalized by default. This can be overrided by col `"rename"` attribute.
* `"visible"` means that columns are visible by default. This can be overrided by col `"visible"` attribute.

### The columns transformations
Format :
```
"cols" : {
    "<col name>": <col transformation>,
    ...
}
```

#### A column transformation
Format :
```
"<col name>": {
    "visible": <bool: default True>
    "id"
    "type": <int|float_iso|float|float_iso|date|date_iso|<expression>>
    "filter"
    "map"
    "rename"
    "agg"
}
```

* `"visible"`: means that the column is visible.
* `"type"`: the name of a type, or an expression

### The new columns (TODO)
Format :
```
"new_cols" : [
    <new col>,
    ...
]
```

### A new column (TODO)
Format :
```
"<col name>": {
    "visible": <bool: default True>
    "id"
    "filter"
    "formula"
    "rename"
    "agg"
}
```


### The aggregate filter (TODO)
Format :
```
"agg_filter" : <an expression using columns ids>
```

Filters after aggregation.

## Expressions
### Data Types
Simple data types

### Operators

### Functions


## Test
```
python3 -m pytest --cov-report term-missing --cov=csv_transformer && python3 -m pytest --cov-report term-missing  --ignore=test --cov-append --doctest-modules --cov=csv_transformer
```

