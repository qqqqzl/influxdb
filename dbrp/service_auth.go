package dbrp

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
)

var _ influxdb.DBRPMappingServiceV2 = (*DBRPMappingAuthorizedService)(nil)

type DBRPMappingAuthorizedService struct {
	influxdb.DBRPMappingServiceV2
}

func (svc DBRPMappingAuthorizedService) FindByID(ctx context.Context, orgID influxdb.ID, id influxdb.ID) (*influxdb.DBRPMapping, error) {
	if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.DBRPTResourceype, id, orgID); err != nil {
		return nil, ErrUnauthorized(err)
	}

	return svc.DBRPMappingServiceV2.FindByID(ctx, orgID, id)
}

func (svc DBRPMappingAuthorizedService) FindMany(ctx context.Context, filter influxdb.DBRPMappingFilter, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMapping, int, error) {
	if _, _, err := authorizer.AuthorizeOrgReadResource(ctx, influxdb.DBRPTResourceype, *filter.OrgID); err != nil {
		return nil, 0, ErrUnauthorized(err)
	}

	return svc.DBRPMappingServiceV2.FindMany(ctx, filter, opts...)
}

func (svc DBRPMappingAuthorizedService) Create(ctx context.Context, t *influxdb.DBRPMapping) error {
	if _, _, err := authorizer.AuthorizeCreate(ctx, influxdb.DBRPTResourceype, t.OrganizationID); err != nil {
		return ErrUnauthorized(err)
	}
	return svc.DBRPMappingServiceV2.Create(ctx, t)
}

func (svc DBRPMappingAuthorizedService) Update(ctx context.Context, u *influxdb.DBRPMapping) error {
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.DBRPTResourceype, u.ID, u.OrganizationID); err != nil {
		return ErrUnauthorized(err)
	}
	return svc.Update(ctx, u)
}

func (svc DBRPMappingAuthorizedService) Delete(ctx context.Context, orgID influxdb.ID, id influxdb.ID) error {
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.DBRPTResourceype, id, orgID); err != nil {
		return ErrUnauthorized(err)
	}
	return svc.Delete(ctx, orgID, id)
}
