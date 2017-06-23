# msi 
 
**m**ap[**s**tring]**i**nterface{} to make orm-less queries.
Attempt to achieve the similar interface as mongodb does.

## Goals
  This is not an ORM but a query builder using map[string]interface{}. 
  This will not require generated code, but load the schema from databases. model first instead of code first
  Focus on per table based queries. Joins should still be handled manually.
## Usage
```
func TestParseGroupBY(t *testing.T) {
	where := map[string]interface{}{
		`id`: map[string]interface{}{`$in`: []int{123, 456, 789}},
	}
	meta := map[string]interface{}{
		msi.LIMIT: 5, msi.OFFSET: 101,
		msi.GROUPBY: []string{`flowcell_barcode`, `RecipePath`},
		msi.ORDERBY: []string{`flowcell_barcode desc`},
		msi.FIELDS:  []string{`flowcell_barcode`},
	}

	query, err := flowcell.Find(where, meta)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(query)

}
```