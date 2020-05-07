package label

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

type Service struct {
	store *Store
}

func NewService(st *Store) influxdb.LabelService {
	return &Service{
		store: st,
	}
}

// CreateLabel creates a new label.
func (s *Service) CreateLabel(ctx context.Context, l *influxdb.Label) error {
	// err := s.kv.Update(ctx, func(tx kv.Tx) error {
	// 	if err := l.Validate(); err != nil {
	// 		return &influxdb.Error{
	// 			Code: influxdb.EInvalid,
	// 			Err:  err,
	// 		}
	// 	}

	// 	l.Name = strings.TrimSpace(l.Name)

	// 	if err := s.uniqueLabelName(ctx, tx, l); err != nil {
	// 		return err
	// 	}

	// 	l.ID = s.IDGenerator.ID()

	// 	if err := s.putLabel(ctx, tx, l); err != nil {
	// 		return err
	// 	}

	// 	if err := s.createUserResourceMappingForOrg(ctx, tx, l.OrgID, l.ID, influxdb.LabelsResourceType); err != nil {
	// 		return err
	// 	}

	// 	return nil
	// })

	// if err != nil {
	// 	return &influxdb.Error{
	// 		Err: err,
	// 	}
	// }
	return nil
}

// FindLabelByID finds a label by its ID
func (s *Service) FindLabelByID(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
	var l *influxdb.Label

	// err := s.kv.View(ctx, func(tx kv.Tx) error {
	// 	label, pe := s.findLabelByID(ctx, tx, id)
	// 	if pe != nil {
	// 		return pe
	// 	}
	// 	l = label
	// 	return nil
	// })

	// if err != nil {
	// 	return nil, &influxdb.Error{
	// 		Err: err,
	// 	}
	// }

	return l, nil
}

// FindLabels returns a list of labels that match a filter.
func (s *Service) FindLabels(ctx context.Context, filter influxdb.LabelFilter, opt ...influxdb.FindOptions) ([]*influxdb.Label, error) {
	ls := []*influxdb.Label{}
	// err := s.kv.View(ctx, func(tx kv.Tx) error {
	// 	labels, err := s.findLabels(ctx, tx, filter)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	ls = labels
	// 	return nil
	// })

	// if err != nil {
	// 	return nil, err
	// }

	return ls, nil
}

func (s *Service) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
	ls := []*influxdb.Label{}
	// if err := s.kv.View(ctx, func(tx Tx) error {
	// 	return s.findResourceLabels(ctx, tx, filter, &ls)
	// }); err != nil {
	// 	return nil, err
	// }

	return ls, nil
}

// UpdateLabel updates a label.
func (s *Service) UpdateLabel(ctx context.Context, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	var label *influxdb.Label
	// err := s.kv.Update(ctx, func(tx Tx) error {
	// 	labelResponse, pe := s.updateLabel(ctx, tx, id, upd)
	// 	if pe != nil {
	// 		return &influxdb.Error{
	// 			Err: pe,
	// 		}
	// 	}
	// 	label = labelResponse
	// 	return nil
	// })

	return label, nil // todo (al): should be err
}

// DeleteLabel deletes a label.
func (s *Service) DeleteLabel(ctx context.Context, id influxdb.ID) error {
	// err := s.kv.Update(ctx, func(tx Tx) error {
	// 	return s.deleteLabel(ctx, tx, id)
	// })
	// if err != nil {
	// 	return &influxdb.Error{
	// 		Err: err,
	// 	}
	// }
	return nil
}

// LabelMappings

// CreateLabelMapping creates a new mapping between a resource and a label.
func (s *Service) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	// return s.kv.Update(ctx, func(tx kv.Tx) error {
	// 	return s.createLabelMapping(ctx, tx, m)
	// })

	return nil
}

// DeleteLabelMapping deletes a label mapping.
func (s *Service) DeleteLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	// err := s.kv.Update(ctx, func(tx Tx) error {
	// 	return s.deleteLabelMapping(ctx, tx, m)
	// })
	// if err != nil {
	// 	return &influxdb.Error{
	// 		Err: err,
	// 	}
	// }
	return nil
}
