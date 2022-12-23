package querybuilder

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"
)

type TestSet struct {
	Type string
	Text string
	Pass bool
}

func TestBuildOneParam(t *testing.T) {
	tests := []TestSet{
		TestSet{
			``, `val1234`, true,
		},
		TestSet{
			``, `$ne:12354`, true,
		},
		TestSet{
			``, `$exists:true`, true,
		},
		TestSet{
			``, `$in:true1,2,3,4,5,a,b,c,d`, true,
		},
		TestSet{
			``, `$nin:true1,2,3,4,5,a,b,c,d`, true,
		},
		TestSet{
			``, `$eq:`, false,
		},
		TestSet{
			``, ` `, false,
		},
		TestSet{
			``, `$regex:value1,value2`, true,
		},
		TestSet{
			``, `or:$regex:value3,value4`, true,
		},
		TestSet{
			`Time`, `or:$lte:value5`, false,
		},
		TestSet{
			`Time`, `or:$lte:2012-11-26`, true,
		},
		TestSet{
			``, `other:$lte:notAllowed`, true,
		},
		//`okay`:        `$exists:true`,
		TestSet{
			``, `$exists:true`, true,
		},
		TestSet{
			``, `$null`, true,
		},
	}
	for _, test := range tests {
		and := make(map[string]interface{})
		or := make(map[string]interface{})
		err := BuildOneParam(test.Type, test.Text, and, or)
		if err != nil {
			if test.Pass {
				t.Fatal(err.Error())
			}
			t.Log(`Expected Not Pass:  wrong format `+test.Text, err.Error())
		}

		for k, v := range and {
			t.Log(`and`, k, `:`, v)
		}

		for k, v := range or {
			t.Log(`or`, k, `:`, v)
		}

	}

}

func TestBuild(t *testing.T) {
	params := map[string]string{
		`country`:      `China`,
		`okay`:         `$exists:true`,
		`name`:         `$regex:LP60001.*`,
		`createdat`:    `or:$lte:2012-11-26`,
		`ProjectName`:  `$nin:abc,def,gha_123`,
		`ProjectName2`: `$in:abc,def,gha_123`,
		`project_id64`: `$null`,
		`project_id`:   `$null`,
		`project_id2`:  `$exists`,
		`_populates`:   `project_id|sample_id`,
		`_outcountby`:  `sample__project_id__project_id|library__project_id__project_id`,
	}
	fm := map[string]string{
		`okay`:         `string`,
		`name`:         `string`,
		`country`:      `string`,
		`createdat`:    `Time`,
		`projectname`:  `string`,
		`projectname2`: `string`,
		`project_id`:   `int`,
		`project_id2`:  `int`,
		`project_id64`: `int64`,
	}
	m, err := BuildAllParams(params, fm)
	if err != nil {
		t.Fatal(err.Error())
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Logf("%s", string(b))
}

type UrlParser struct {
	//	Path string
	u *url.URL
}

func NewUrlParser(path string) (*UrlParser, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	return &UrlParser{u: u}, nil
}

func (self *UrlParser) Get(name string) string {
	m, _ := url.ParseQuery(self.u.RawQuery)
	return strings.Join(m[name], ",")
}

func TestBuildURL(t *testing.T) {
	urls := []string{
		`http://localhost/?field1=$in:abc&_limit=7&_sortby=field1&_skip=5&_groupby=field1&_fields=field1,field2`,
	}
	filemap := map[string]string{
		`field1`: `string`,
		`field2`: `string`,
	}

	for _, u := range urls {
		q, err := NewUrlParser(u)
		if err != nil {
			t.Fatal(err.Error())
		}
		qb, err := Build(q, filemap)
		if err != nil {
			t.Fatal(err.Error())
		}

		t.Logf("%+v", qb)
	}
}
