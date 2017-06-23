# msi 
 
**m**ap[**s**tring]**i**nterface{} to make orm-less queries.
Attempt to achieve the similar interface as mongodb does.

## Goals
  This is not an ORM but a query builder using map[string]interface{}. 
  This will not require generated code, but load the schema from databases. model first instead of code first
  Focus on per table based queries. Joins should still be handled manually.
## Usage
Load Table definitions and CRUD functions
```
	schema, err := msi.NewMsi(`mysql`, `username:password@(localhost:3306)/databasename`, `databasename`, ``)
	if err != nil {
		t.Fatal()
	}
	t.Log(`Total Number of Tables: `, len(schema.Tables))
	t.Log(`##############`)
	crit := map[string]interface{}{`id`: 123}
	updates := map[string]interface{}{`id`: `updated values`}
	for _, table := range schema.Tables {
		t.Log(`###`)
		t.Log(` Table:`, table.TableName, `,`, `number of fields:`, len(table.Fields))
		table.SelectAll()
		query, err := table.FindId(123)
		if err != nil {
			t.Fatal(err.Error())
		}

		table.UnSelectAllFields()
		t.Log(query)
		query, err = table.Find(crit)
		if err != nil {
			t.Fatal(err.Error())
		}

		t.Log(query)
		query, err = table.UpdateId(1, updates)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.Log(query)
		query, err = table.RemoveId(123)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.Log(query)
		query, err = table.Insert(updates)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.Log(query)

	}
```

Build a query from Mongodb flavor
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

Shorten the writtings with redefined msi
```
func TestNameAlias(t *testing.T) {
	query, err := flowcell.Find(M{`id`: 123})
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(query)
}
```
