package models

import (
	"fmt"

	"github.com/ws6/msi"
)

//add trigger after analysis_run created to analysis.last_run_id

func GetRunTable() *msi.Table {
	db := GetSchema()
	runTable := db.GetTable(`analysis_run`)
	if runTable == nil {
		return fmt.Errorf(`no analysis_run table found`)
	}
	return runTable, nil
}

//unique key by  analysisId + attempt
func GetAnalysisRun(runTable *msi.Table, analysisId, attempt int) (map[string]interface{}, error) {

	founds, err := runTable.Find(msi.M{`analysis_id`: analysisId, `attempt`: attempt}).Map()
	if err != nil {
		return nil, err
	}

	if len(founds) == 0 {
		return nil, fmt.Errorf(`not found`)
	}

	return founds[0], nil
}

//BeforeAnalysisRunCreated auto increament attempt
func GetAttempt(updates msi.M) error {
	analysisId, ok := updates[`analysis_id`]
	if !ok {
		return fmt.Errorf(`analysis_id is missing when create analysis_run`)
	}

	return nil
}

//AfterAnalysisRunCreated auto increament attempt
func UpdateLastBuildId(updates msi.M) error {
	return nil
}

func UpdateAnalysisLastRunId() error {
	db := GetSchema()
	runTable := db.GetTable(`analysis_run`)
	if runTable == nil {
		return fmt.Errorf(`no analysis_run table found`)
	}

	runTable.BeforeCreates = append(runTable.BeforeCreates, GetAttempt)

	runTable.AfterCreates = append(runTable.AfterCreates, UpdateLastBuildId)

	//	runTable.AfterCreates
	return nil
}
