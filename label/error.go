package label

import (
	"github.com/influxdata/influxdb/v2"
)

var (
	// NotUniqueIDError occurs when attempting to create a Label with an ID that already belongs to another one
	NotUniqueIDError = &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  "ID already exists",
	}

	// ErrFailureGeneratingID occurs ony when the random number generator
	// cannot generate an ID in MaxIDGenerationN times.
	ErrFailureGeneratingID = &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "unable to generate valid id",
	}
)
