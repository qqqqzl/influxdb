package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/pkg/errors"
)

// TODO:
//  - Update (all)
//  - Create - bucket does not exist

var dbrpMappingCmpOptionsV2 = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.DBRPMappingV2) []*influxdb.DBRPMappingV2 {
		out := make([]*influxdb.DBRPMappingV2, len(in))
		copy(out, in) // Copy input slice to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			if out[i].Database != out[j].Database {
				return out[i].Database < out[j].Database
			}
			return out[i].RetentionPolicy < out[j].RetentionPolicy
		})
		return out
	}),
}

type DBRPMappingFieldsV2 struct {
	BucketSvc      influxdb.BucketService
	DBRPMappingsV2 []*influxdb.DBRPMappingV2
}

// Populate creates all entities in DBRPMappingFieldsV2.
func (f DBRPMappingFieldsV2) Populate(ctx context.Context, s influxdb.DBRPMappingServiceV2) error {
	for _, m := range f.DBRPMappingsV2 {
		if err := s.Create(ctx, m); err != nil {
			return errors.Wrap(err, "failed to populate dbrp mappings")
		}
	}
	return nil
}

// CleanupDBRPMappingsV2 finds and removes all dbrp mappings.
func CleanupDBRPMappingsV2(ctx context.Context, s influxdb.DBRPMappingServiceV2) error {
	mappings, _, err := s.FindMany(ctx, influxdb.DBRPMappingFilterV2{})
	if err != nil {
		return errors.Wrap(err, "failed to retrieve all dbrp mappings")
	}

	for _, m := range mappings {
		if err := s.Delete(ctx, m.ID); err != nil {
			return errors.Wrapf(err, "failed to remove dbrp mapping %v", m.ID)
		}
	}
	return nil
}

func CreateDBRPMappingV2(
	init func(DBRPMappingFieldsV2, *testing.T) (influxdb.DBRPMappingServiceV2, func()),
	t *testing.T,
) {
	type args struct {
		dbrpMapping *influxdb.DBRPMappingV2
	}
	type wants struct {
		err          error
		dbrpMappings []*influxdb.DBRPMappingV2
	}

	tests := []struct {
		name   string
		fields DBRPMappingFieldsV2
		args   args
		wants  wants
	}{
		{
			name: "basic create dbrpMapping",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{},
			},
			args: args{
				dbrpMapping: &influxdb.DBRPMappingV2{
					ID:              100,
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         false,
					OrganizationID:  MustIDBase16(dbrpOrg1ID),
					BucketID:        MustIDBase16(dbrpBucket1ID),
				},
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{{
					ID:              100,
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         false,
					OrganizationID:  MustIDBase16(dbrpOrg1ID),
					BucketID:        MustIDBase16(dbrpBucket1ID),
				}},
			},
		},
		{
			name: "error on create existing dbrpMapping with same ID",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{{
					ID:              100,
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         false,
					OrganizationID:  MustIDBase16(dbrpOrg1ID),
					BucketID:        MustIDBase16(dbrpBucket1ID),
				}},
			},
			args: args{
				dbrpMapping: &influxdb.DBRPMappingV2{
					ID:              100,
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         true,
					OrganizationID:  MustIDBase16(dbrpOrg1ID),
					BucketID:        MustIDBase16(dbrpBucket1ID),
				},
			},
			wants: wants{
				err: dbrp.ErrDBRPAlreadyExist(nil),
				dbrpMappings: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg1ID),
						BucketID:        MustIDBase16(dbrpBucket1ID),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.Create(ctx, tt.args.dbrpMapping)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			dbrpMappings, _, err := s.FindMany(ctx, influxdb.DBRPMappingFilterV2{})
			if err != nil {
				t.Fatalf("failed to retrieve dbrpMappings: %v", err)
			}
			if diff := cmp.Diff(dbrpMappings, tt.wants.dbrpMappings, dbrpMappingCmpOptionsV2...); diff != "" {
				t.Errorf("dbrpMappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func FindManyDBRPMappingsV2(
	init func(DBRPMappingFieldsV2, *testing.T) (influxdb.DBRPMappingServiceV2, func()),
	t *testing.T,
) {
	type args struct {
		filter influxdb.DBRPMappingFilterV2
	}

	type wants struct {
		dbrpMappings []*influxdb.DBRPMappingV2
		err          error
	}
	tests := []struct {
		name   string
		fields DBRPMappingFieldsV2
		args   args
		wants  wants
	}{
		{
			name: "find all dbrpMappings",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg1ID),
						BucketID:        MustIDBase16(dbrpBucket1ID),
					},
					{
						ID:              200,
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg2ID),
						BucketID:        MustIDBase16(dbrpBucket2ID),
					},
				},
			},
			args: args{
				filter: influxdb.DBRPMappingFilterV2{},
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg1ID),
						BucketID:        MustIDBase16(dbrpBucket1ID),
					},
					{
						ID:              200,
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg2ID),
						BucketID:        MustIDBase16(dbrpBucket2ID),
					},
				},
			},
		},
		{
			name: "find by ID",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              MustIDBase16("1111111111111111"),
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              MustIDBase16("2222222222222222"),
						Database:        "database2",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
			args: args{
				filter: influxdb.DBRPMappingFilterV2{
					ID: MustIDBase16Ptr("1111111111111111"),
				},
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{
					{
						ID:              MustIDBase16("1111111111111111"),
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
				},
			},
		},
		{
			name: "find by bucket ID",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              200,
						Database:        "database2",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
			args: args{
				filter: influxdb.DBRPMappingFilterV2{
					BucketID: MustIDBase16Ptr(dbrpBucketBID),
				},
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{
					{
						ID:              200,
						Database:        "database2",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
		},
		{
			name: "find by orgID",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg2ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              200,
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
			args: args{
				filter: influxdb.DBRPMappingFilterV2{
					OrgID: MustIDBase16Ptr(dbrpOrg3ID),
				},
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{
					{
						ID:              200,
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
		},
		{
			name: "find by db",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              200,
						Database:        "database2",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
			args: args{
				filter: influxdb.DBRPMappingFilterV2{
					Database: stringPtr("database1"),
				},
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
				},
			},
		},
		{
			name: "find by rp",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              200,
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
			args: args{
				filter: influxdb.DBRPMappingFilterV2{
					RetentionPolicy: stringPtr("retention_policyB"),
				},
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{
					{
						ID:              200,
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
		},
		{
			name: "find by default",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              200,
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
			args: args{
				filter: influxdb.DBRPMappingFilterV2{
					Default: boolPtr(true),
				},
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{
					{
						ID:              200,
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
		},
		{
			name: "mixed",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              200,
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              300,
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg2ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              400,
						Database:        "database2",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
			args: args{
				filter: influxdb.DBRPMappingFilterV2{
					RetentionPolicy: stringPtr("retention_policyA"),
					Default:         boolPtr(true),
					OrgID:           MustIDBase16Ptr(dbrpOrg3ID),
				},
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              200,
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
				},
			},
		},
		{
			name: "not found",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						Database:        "database1",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg2ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						Database:        "database2",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketBID),
					},
				},
			},
			args: args{
				filter: influxdb.DBRPMappingFilterV2{
					Database:        stringPtr("database1"),
					RetentionPolicy: stringPtr("retention_policyB"),
				},
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			dbrpMappings, _, err := s.FindMany(ctx, tt.args.filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(dbrpMappings, tt.wants.dbrpMappings, dbrpMappingCmpOptionsV2...); diff != "" {
				t.Errorf("dbrpMappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func FindDBRPMappingByIDV2(
	init func(DBRPMappingFieldsV2, *testing.T) (influxdb.DBRPMappingServiceV2, func()),
	t *testing.T,
) {
	type args struct {
		ID influxdb.ID
	}

	type wants struct {
		dbrpMapping *influxdb.DBRPMappingV2
		err         error
	}

	tests := []struct {
		name   string
		fields DBRPMappingFieldsV2
		args   args
		wants  wants
	}{
		{
			name: "find existing dbrpMapping",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              200,
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
					{
						ID:              300,
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
				},
			},
			args: args{
				ID: 200,
			},
			wants: wants{
				dbrpMapping: &influxdb.DBRPMappingV2{
					ID:              200,
					Database:        "database",
					RetentionPolicy: "retention_policyA",
					Default:         false,
					OrganizationID:  MustIDBase16(dbrpOrg3ID),
					BucketID:        MustIDBase16(dbrpBucketAID),
				},
			},
		},
		{
			name: "find non existing dbrpMapping",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg3ID),
						BucketID:        MustIDBase16(dbrpBucketAID),
					},
				},
			},
			args: args{
				ID: 200,
			},
			wants: wants{
				err: dbrp.ErrDBRPNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			dbrpMapping, err := s.FindByID(ctx, tt.args.ID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(dbrpMapping, tt.wants.dbrpMapping, dbrpMappingCmpOptionsV2...); diff != "" {
				t.Errorf("dbrpMappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func DeleteDBRPMappingV2(
	init func(DBRPMappingFieldsV2, *testing.T) (influxdb.DBRPMappingServiceV2, func()),
	t *testing.T,
) {
	type args struct {
		ID influxdb.ID
	}
	type wants struct {
		err          error
		dbrpMappings []*influxdb.DBRPMappingV2
	}

	tests := []struct {
		name   string
		fields DBRPMappingFieldsV2
		args   args
		wants  wants
	}{
		{
			name: "delete existing dbrpMapping",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg1ID),
						BucketID:        MustIDBase16(dbrpBucket1ID),
					},
					{
						ID:              200,
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg2ID),
						BucketID:        MustIDBase16(dbrpBucket2ID),
					},
				},
			},
			args: args{
				ID: 100,
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{{
					ID:              200,
					Database:        "database2",
					RetentionPolicy: "retention_policy2",
					Default:         true,
					OrganizationID:  MustIDBase16(dbrpOrg2ID),
					BucketID:        MustIDBase16(dbrpBucket2ID),
				}},
			},
		},
		{
			name: "delete non-existing dbrpMapping",
			fields: DBRPMappingFieldsV2{
				DBRPMappingsV2: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg1ID),
						BucketID:        MustIDBase16(dbrpBucket1ID),
					},
					{
						ID:              200,
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg2ID),
						BucketID:        MustIDBase16(dbrpBucket2ID),
					},
				},
			},
			args: args{
				ID: 300,
			},
			wants: wants{
				dbrpMappings: []*influxdb.DBRPMappingV2{
					{
						ID:              100,
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  MustIDBase16(dbrpOrg1ID),
						BucketID:        MustIDBase16(dbrpBucket1ID),
					},
					{
						ID:              200,
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  MustIDBase16(dbrpOrg2ID),
						BucketID:        MustIDBase16(dbrpBucket2ID),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.Delete(ctx, tt.args.ID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			filter := influxdb.DBRPMappingFilterV2{}
			dbrpMappings, _, err := s.FindMany(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve dbrpMappings: %v", err)
			}
			if diff := cmp.Diff(dbrpMappings, tt.wants.dbrpMappings, dbrpMappingCmpOptionsV2...); diff != "" {
				t.Errorf("dbrpMappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
