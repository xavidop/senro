package kubeapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// A coordination.k8s.io/v1 Lease: Kubernetes' own answer to "one holder at
// a time", used for exactly one thing here: excluding two runs from a
// PersistentVolumeClaim-backed workspace. A claim-backed workspace may be
// reached by coordinators on two machines, so the file lock
// internal/persist uses for local workspaces excludes nothing.
//
// What a Lease gives: resourceVersion makes "create, or take over if
// expired" atomic, so two racing coordinators produce one winner. What it
// does not: a fencing token, so a holder partitioned from the apiserver can
// keep writing. That is the standard limitation, stated rather than hidden;
// the exclusion is as strong as Kubernetes' own leader election.

// Lease is the subset of coordination.k8s.io/v1 Lease this client uses.
type Lease struct {
	APIVersion string     `json:"apiVersion,omitempty"`
	Kind       string     `json:"kind,omitempty"`
	Metadata   ObjectMeta `json:"metadata,omitempty"`
	Spec       LeaseSpec  `json:"spec,omitempty"`
}

// LeaseSpec is who holds it and for how long.
type LeaseSpec struct {
	// HolderIdentity names the current holder. senro puts the run ID here, so
	// a refusal can say which run is holding the workspace.
	HolderIdentity *string `json:"holderIdentity,omitempty"`
	// LeaseDurationSeconds is how long the lease is good for after
	// RenewTime. A lease whose holder died is taken over once this has
	// elapsed, which is what stops a killed coordinator locking a workspace
	// out forever.
	LeaseDurationSeconds *int32 `json:"leaseDurationSeconds,omitempty"`
	// AcquireTime is when the current holder first took it, and RenewTime is
	// when it last said it was still alive. Both are MicroTime, not time.Time;
	// see MicroTime for why that distinction is load-bearing.
	AcquireTime *MicroTime `json:"acquireTime,omitempty"`
	RenewTime   *MicroTime `json:"renewTime,omitempty"`
}

// MicroTime is a timestamp in the format Kubernetes' metav1.MicroTime
// demands. The apiserver parses these fields with the layout
// "2006-01-02T15:04:05.000000Z07:00", which requires EXACTLY six decimal
// places; Go's RFC3339Nano removes trailing zeros, so a timestamp whose
// microseconds end in one marshals as ".00087Z" and the apiserver rejects
// the object with a 400. That failure depends on the clock's value at the
// moment of the call, so it presents as an unreproducible flake, not a bug.
type MicroTime struct{ time.Time }

// microTimeLayout is the apiserver's, exactly.
const microTimeLayout = "2006-01-02T15:04:05.000000Z07:00"

// NewMicroTime is the constructor, which truncates to microseconds because
// that is all the wire format carries: keeping nanoseconds here would mean a
// value that does not survive a round trip.
func NewMicroTime(t time.Time) *MicroTime {
	m := MicroTime{Time: t.UTC().Truncate(time.Microsecond)}
	return &m
}

func (m MicroTime) MarshalJSON() ([]byte, error) {
	return []byte(`"` + m.Time.UTC().Format(microTimeLayout) + `"`), nil
}

func (m *MicroTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	// Parsed with RFC3339 rather than the strict layout: this is reading what
	// the apiserver SENT, and being liberal about a format that is only
	// strict on the way in costs nothing and avoids failing on a field some
	// other writer produced.
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return fmt.Errorf("kubeapi: parsing MicroTime %q: %w", s, err)
	}
	m.Time = t
	return nil
}

// Expired reports whether this lease may be taken over, as of now.
//
// A lease with no renew time or no duration has never been properly held and
// is free: that is the shape left behind by a coordinator killed between
// creating the object and filling it in.
func (l Lease) Expired(now time.Time) bool {
	if l.Spec.RenewTime == nil || l.Spec.LeaseDurationSeconds == nil {
		return true
	}
	return now.After(l.Spec.RenewTime.Add(time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second))
}

// Holder is who holds it, or "" when nobody has recorded themselves.
func (l Lease) Holder() string {
	if l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

// GetLease reads one lease. A lease that does not exist is reported through
// IsNotFound rather than as an empty value, because "absent" and "present but
// unheld" are different states and the caller acts differently on each.
func (c *Client) GetLease(ctx context.Context, ns, name string) (Lease, error) {
	var l Lease
	err := c.jsonRequest(ctx, http.MethodGet, leasePath(ns, name), "", nil, &l)
	return l, err
}

// CreateLease creates one. A name already taken comes back through
// IsConflict, which is the signal that somebody else got there first.
func (c *Client) CreateLease(ctx context.Context, ns string, l Lease) (Lease, error) {
	l.APIVersion, l.Kind = "coordination.k8s.io/v1", "Lease"
	b, err := json.Marshal(l)
	if err != nil {
		return Lease{}, fmt.Errorf("kubeapi: encoding lease: %w", err)
	}
	var out Lease
	err = c.jsonRequest(ctx, http.MethodPost, leasesPath(ns), "application/json", b, &out)
	return out, err
}

// UpdateLease replaces one, and it is the operation the whole exclusion rests
// on.
//
// The lease MUST carry the resourceVersion it was read at. The apiserver
// rejects an update whose resourceVersion is stale with a 409, so two
// coordinators that both read an expired lease and both try to take it over
// produce exactly one winner: the loser's update carries the version it read,
// which the winner has already superseded. Dropping the resourceVersion would
// turn this into last-write-wins and the exclusion into decoration.
func (c *Client) UpdateLease(ctx context.Context, ns string, l Lease) (Lease, error) {
	if l.Metadata.ResourceVersion == "" {
		return Lease{}, fmt.Errorf(
			"kubeapi: refusing to update lease %s/%s with no resourceVersion: the update would be "+
				"last-write-wins and two holders could take the same lease", ns, l.Metadata.Name)
	}
	l.APIVersion, l.Kind = "coordination.k8s.io/v1", "Lease"
	b, err := json.Marshal(l)
	if err != nil {
		return Lease{}, fmt.Errorf("kubeapi: encoding lease: %w", err)
	}
	var out Lease
	err = c.jsonRequest(ctx, http.MethodPut, leasePath(ns, l.Metadata.Name), "application/json", b, &out)
	return out, err
}

// DeleteLease removes one. A lease that is already gone is not an error, for
// the reason DeletePod gives: release runs on paths where something else may
// have removed it first.
func (c *Client) DeleteLease(ctx context.Context, ns, name string) error {
	err := c.jsonRequest(ctx, http.MethodDelete, leasePath(ns, name), "", nil, nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

func leasesPath(ns string) string {
	return "/apis/coordination.k8s.io/v1/namespaces/" + url.PathEscape(ns) + "/leases"
}

func leasePath(ns, name string) string {
	return leasesPath(ns) + "/" + url.PathEscape(name)
}
