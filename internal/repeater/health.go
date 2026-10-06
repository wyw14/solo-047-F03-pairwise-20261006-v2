package repeater

import (
	"context"
	"sync"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type IncidentSink interface {
	OpenRepeaterIncident(repeaterID string, generation uint64) error
	ResolveRepeaterIncident(repeaterID string, generation uint64) error
	Active(repeaterID string) (uint64, bool)
	LastResolution(repeaterID string) uint64
}

// RampInvalidator is notified when a repeater trips or its trip generation
// changes, so a ramp bound to the old generation is invalidated eagerly.
type RampInvalidator interface {
	InvalidateOnFault(repeaterID string, generation uint64)
}

type HealthProjector struct {
	mu          sync.RWMutex
	repeaters   map[string]*Repeater
	incidents   IncidentSink
	invalidator RampInvalidator
	tripAtC     float64
}

func NewHealthProjector(incidents IncidentSink, tripAtC float64) *HealthProjector {
	return &HealthProjector{repeaters: make(map[string]*Repeater), incidents: incidents, tripAtC: tripAtC}
}

func (p *HealthProjector) SetInvalidator(invalidator RampInvalidator) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.invalidator = invalidator
}

func (p *HealthProjector) Register(repeater *Repeater) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.repeaters[repeater.Snapshot().ID] = repeater
}

func (p *HealthProjector) ApplySample(_ context.Context, sample telemetry.Sample) error {
	if sample.Kind != telemetry.KindTemperature {
		return nil
	}
	p.mu.RLock()
	repeater := p.repeaters[sample.AssetID]
	p.mu.RUnlock()
	if repeater == nil {
		return nil
	}
	before := repeater.Snapshot()
	if sample.Sequence <= before.LastSequence || sample.Generation < before.TripGeneration {
		return nil
	}
	after := repeater.update(func(current *Snapshot) {
		current.LastSequence = sample.Sequence
		current.TemperatureC = sample.Value
		current.UpdatedAt = sample.ObservedAt.UTC()
		if sample.Value >= p.tripAtC {
			current.Health = HealthTripped
			current.TripGeneration = sample.Generation
		} else if current.Health != HealthTripped {
			current.Health = HealthNormal
		}
	})
	if before.Health != HealthTripped && after.Health == HealthTripped {
		p.mu.RLock()
		invalidator := p.invalidator
		p.mu.RUnlock()
		if invalidator != nil {
			invalidator.InvalidateOnFault(after.ID, after.TripGeneration)
		}
		if p.incidents != nil {
			return p.incidents.OpenRepeaterIncident(after.ID, after.TripGeneration)
		}
	}
	return nil
}

func (p *HealthProjector) Reset(repeaterID string, generation uint64) error {
	p.mu.RLock()
	repeater := p.repeaters[repeaterID]
	p.mu.RUnlock()
	if repeater == nil {
		return nil
	}
	if active, ok := p.incidents.Active(repeaterID); ok && generation < active {
		return nil
	}
	repeater.update(func(current *Snapshot) {
		if generation >= current.TripGeneration {
			current.Health = HealthNormal
			current.TripGeneration = generation
		}
	})
	p.mu.RLock()
	invalidator := p.invalidator
	p.mu.RUnlock()
	if invalidator != nil {
		invalidator.InvalidateOnFault(repeaterID, generation)
	}
	if p.incidents != nil {
		if err := p.incidents.ResolveRepeaterIncident(repeaterID, generation); err != nil {
			return err
		}
		_ = p.incidents.LastResolution(repeaterID)
	}
	return nil
}
