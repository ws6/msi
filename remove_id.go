package msi

import (
	"fmt"
)

func (t *Table) RemoveId(id int) string {

	return fmt.Sprintf(`DELETE FROM %s WHERE id = %d ;`, t.TableName, id)
}
