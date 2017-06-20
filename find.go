package msi

//crit can be nested crit
//if crit.key is a field, using =
//if crit.key is any $and, $or,$ne, $nin, $in, $gt, $gte, $lt, $lte, then translate it to sub query
//only try one level

func FlatQuery(crit map[string]interface{}) []string {
	return []string{}
}

func (t *Table) Find(crit map[string]interface{}) string {
	return ""
}
