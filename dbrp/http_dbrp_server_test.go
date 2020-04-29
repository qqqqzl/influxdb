package dbrp_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/mock"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func initBucketHttpService(t *testing.T) (influxdb.DBRPMappingServiceV2, *httptest.Server, func()) {
	t.Helper()
	ctx := context.Background()
	bucketSvc := mock.NewBucketService()

	s := inmem.NewKVStore()
	svc, err := dbrp.NewService(ctx, bucketSvc, s)
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(dbrp.NewHTTPDBRPHandler(zaptest.NewLogger(t), svc))
	return svc, server, func() {
		server.Close()
	}
}

func Test_handlePostDBRP(t *testing.T) {
	table := []struct {
		Name         string
		ExpectedErr  *influxdb.Error
		ExpectedDBRP *influxdb.DBRPMapping
		Input        io.Reader
	}{
		{
			Name: "Create valid dbrp",
			Input: strings.NewReader(`{
	"bucket_id": "5555f7ed2a035555",
	"organization_id": "059af7ed2a034000",
	"database": "mydb",
	"retention_policy": "autogen",
	"default": false
}`),
			ExpectedDBRP: &influxdb.DBRPMapping{
				OrganizationID: platformtesting.MustIDBase16("059af7ed2a034000"),
			},
		},
		{
			Name: "Create with invalid orgID",
			Input: strings.NewReader(`{
	"bucket_id": "5555f7ed2a035555",
	"organization_id": "invalid",
	"database": "mydb",
	"retention_policy": "autogen",
	"default": false
}`),
			ExpectedErr: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "invalid json structure",
				Err:  influxdb.ErrInvalidID.Err,
			},
		},
	}

	for _, s := range table {
		t.Run(s.Name, func(t *testing.T) {
			if s.ExpectedErr != nil && s.ExpectedDBRP != nil {
				t.Error("one of those has to be set")
			}
			_, server, shutdown := initBucketHttpService(t)
			defer shutdown()
			client := server.Client()

			resp, err := client.Post(server.URL+"/", "application/json", s.Input)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if s.ExpectedErr != nil {
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}

				if !strings.Contains(string(b), s.ExpectedErr.Error()) {
					t.Fatal(string(b))
				}
				return
			}
			dbrp := &influxdb.DBRPMapping{}
			if err := json.NewDecoder(resp.Body).Decode(&dbrp); err != nil {
				t.Fatal(err)
			}

			if !dbrp.ID.Valid() {
				t.Fatalf("expected invalid id, got an invalid one %s", dbrp.ID.String())
			}

			if dbrp.OrganizationID != s.ExpectedDBRP.OrganizationID {
				t.Fatalf("expected orgid %s got %s", s.ExpectedDBRP.OrganizationID, dbrp.OrganizationID)
			}

		})
	}
}

func Test_handleGetDBRPs(t *testing.T) {
	table := []struct {
		Name          string
		QueryParams   string
		ExpectedErr   *influxdb.Error
		ExpectedDBRPs []influxdb.DBRPMapping
	}{
		{
			Name:        "Create with invalid orgID",
			QueryParams: "orgID=059af7ed2a034000",
			ExpectedDBRPs: []influxdb.DBRPMapping{
				{
					ID:              platformtesting.MustIDBase16("1111111111111111"),
					BucketID:        platformtesting.MustIDBase16("5555f7ed2a035555"),
					OrganizationID:  platformtesting.MustIDBase16("059af7ed2a034000"),
					Database:        "mydb",
					RetentionPolicy: "autogen",
					Default:         true,
				},
			},
		},
	}

	ctx := context.Background()
	for _, s := range table {
		t.Run(s.Name, func(t *testing.T) {
			if s.ExpectedErr != nil && len(s.ExpectedDBRPs) != 0 {
				t.Error("one of those has to be set")
			}
			svc, server, shutdown := initBucketHttpService(t)
			defer shutdown()

			if svc, ok := svc.(*dbrp.Service); ok {
				svc.IDGen = mock.NewIDGenerator("1111111111111111", t)
			}
			dbrp := &influxdb.DBRPMapping{
				BucketID:        platformtesting.MustIDBase16("5555f7ed2a035555"),
				OrganizationID:  platformtesting.MustIDBase16("059af7ed2a034000"),
				Database:        "mydb",
				RetentionPolicy: "autogen",
				Default:         true,
			}
			if err := svc.Create(ctx, dbrp); err != nil {
				t.Fatal(err)
			}

			client := server.Client()
			resp, err := client.Get(server.URL + "?" + s.QueryParams)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if s.ExpectedErr != nil {
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}

				if !strings.Contains(string(b), s.ExpectedErr.Error()) {
					t.Fatal(string(b))
				}
				return
			}
			dbrps := struct {
				Content []influxdb.DBRPMapping `json:"content"`
			}{}
			if err := json.NewDecoder(resp.Body).Decode(&dbrps); err != nil {
				t.Fatal(err)
			}

			if len(dbrps.Content) != len(s.ExpectedDBRPs) {
				t.Fatalf("expected %d dbrps got %d", len(s.ExpectedDBRPs), len(dbrps.Content))
			}

			if !cmp.Equal(s.ExpectedDBRPs, dbrps.Content) {
				t.Fatalf(cmp.Diff(s.ExpectedDBRPs, dbrps.Content))
			}

		})
	}
}

func Test_handlPatchDBRP(t *testing.T) {
	table := []struct {
		Name         string
		ExpectedErr  *influxdb.Error
		ExpectedDBRP *influxdb.DBRPMapping
		URLSuffix    string
		Input        io.Reader
	}{
		{
			Name:      "happy path update",
			URLSuffix: "/1111111111111111?orgID=059af7ed2a034000",
			Input: strings.NewReader(`{
	"database": "updatedb"
}`),
			ExpectedDBRP: &influxdb.DBRPMapping{
				ID:              platformtesting.MustIDBase16("1111111111111111"),
				BucketID:        platformtesting.MustIDBase16("5555f7ed2a035555"),
				OrganizationID:  platformtesting.MustIDBase16("059af7ed2a034000"),
				Database:        "updatedb",
				RetentionPolicy: "autogen",
				Default:         true,
			},
		},
	}

	ctx := context.Background()

	for _, s := range table {
		t.Run(s.Name, func(t *testing.T) {
			if s.ExpectedErr != nil && s.ExpectedDBRP != nil {
				t.Error("one of those has to be set")
			}
			svc, server, shutdown := initBucketHttpService(t)
			defer shutdown()
			client := server.Client()

			if svc, ok := svc.(*dbrp.Service); ok {
				svc.IDGen = mock.NewIDGenerator("1111111111111111", t)
			}

			dbrp := &influxdb.DBRPMapping{
				BucketID:        platformtesting.MustIDBase16("5555f7ed2a035555"),
				OrganizationID:  platformtesting.MustIDBase16("059af7ed2a034000"),
				Database:        "mydb",
				RetentionPolicy: "autogen",
				Default:         true,
			}
			if err := svc.Create(ctx, dbrp); err != nil {
				t.Fatal(err)
			}

			req, _ := http.NewRequest(http.MethodPatch, server.URL+s.URLSuffix, s.Input)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if s.ExpectedErr != nil {
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}

				if !strings.Contains(string(b), s.ExpectedErr.Error()) {
					t.Fatal(string(b))
				}
				return
			}
			dbrpResponse := struct {
				Content *influxdb.DBRPMapping `json:"content"`
			}{}

			if err := json.NewDecoder(resp.Body).Decode(&dbrpResponse); err != nil {
				t.Fatal(err)
			}

			if !cmp.Equal(s.ExpectedDBRP, dbrpResponse.Content) {
				t.Fatalf(cmp.Diff(s.ExpectedDBRP, dbrpResponse.Content))
			}

		})
	}
}

func Test_handlDeleteDBRP(t *testing.T) {
	table := []struct {
		Name         string
		ExpectedErr  *influxdb.Error
		ExpectedDBRP *influxdb.DBRPMapping
		URLSuffix    string
		Input        io.Reader
	}{
		{
			Name:      "delete",
			URLSuffix: "/1111111111111111?orgID=059af7ed2a034000",
		},
	}

	ctx := context.Background()

	for _, s := range table {
		t.Run(s.Name, func(t *testing.T) {
			if s.ExpectedErr != nil && s.ExpectedDBRP != nil {
				t.Error("one of those has to be set")
			}
			svc, server, shutdown := initBucketHttpService(t)
			defer shutdown()
			client := server.Client()

			if svc, ok := svc.(*dbrp.Service); ok {
				svc.IDGen = mock.NewIDGenerator("1111111111111111", t)
			}

			d := &influxdb.DBRPMapping{
				BucketID:        platformtesting.MustIDBase16("5555f7ed2a035555"),
				OrganizationID:  platformtesting.MustIDBase16("059af7ed2a034000"),
				Database:        "mydb",
				RetentionPolicy: "autogen",
				Default:         true,
			}
			if err := svc.Create(ctx, d); err != nil {
				t.Fatal(err)
			}

			req, _ := http.NewRequest(http.MethodDelete, server.URL+s.URLSuffix, nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if s.ExpectedErr != nil {
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}

				if !strings.Contains(string(b), s.ExpectedErr.Error()) {
					t.Fatal(string(b))
				}
				return
			}

			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("expected status code %d, got %d", http.StatusNoContent, resp.StatusCode)
			}

			if _, err := svc.FindByID(ctx, platformtesting.MustIDBase16("1111111111111111"), platformtesting.MustIDBase16("5555f7ed2a035555")); !errors.Is(err, dbrp.ErrDBRPNotFound) {
				t.Fatalf("expected err dbrp not found, got %s", err)
			}

		})
	}
}
