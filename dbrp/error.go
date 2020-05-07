package dbrp

import (
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

var (
	// ErrInvalidDBRPID is used when the ID of the DBRP cannot be encoded.
	ErrInvalidDBRPID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "DBRPorization ID is invalid",
	}

	// ErrDBRPNotFound is used when the specified DBRP cannot be found.
	ErrDBRPNotFound = &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  "unable to find DBRP",
	}

	// NotUniqueIDError is used when the ID of the DBRP is not unique.
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

func ErrUnauthorized(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnauthorized,
		Msg:  "unauthorized",
		Err:  err,
	}
}

// ErrInvalidDBRPError is used when a service was provided an invalid DBRP.
func ErrInvalidDBRPError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "DBRP provided is invalid",
		Err:  err,
	}
}

// ErrInternalServiceError is used when the error comes from an internal system.
func ErrInternalServiceError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Err:  err,
	}
}

// UnexpectedDBRPIndexError is used when the error comes from an internal system.
func UnexpectedDBRPIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving DBRP index; Err: %v", err),
	}
}

// ErrDBRPAlreadyExist is used when there is a conflict in creating a new DBRP.
func ErrDBRPAlreadyExist(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Err:  fmt.Errorf("dbrp already exist for this particular ID. If you are trying an update use the right function .Update"),
	}
}

// ErrInternalServiceError is used when the error comes from an internal system.
func ErrInternalDBRPError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Err:  err,
	}
}
