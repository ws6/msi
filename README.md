# msi 
 
**m**ap[**s**tring]**i**nterface{} to make orm-less queries.
Attempt to achieve the similar interface as mongodb does.

## Goals
  This is not an ORM but a query builder using map[string]interface{}. 
  This will not require generated code, but load the schema from databases. model first instead of code first
  Focus on per table based queries. Joins should still be handled manually.
## Usage
Please check example/example.go as standablone library

example/beegoapp is a simple example with CRUD out of the box using beego framework.

TODO barefoot http app using msi lib
 
