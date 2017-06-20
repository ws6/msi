package msi

import (
	"fmt"
)

func (t *Table) GetId(id int) string {

	return fmt.Sprintf(`SELECT * FROM %s WHERE id = %d ;`, t.TableName, id)
}
