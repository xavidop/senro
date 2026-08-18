package api_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/api/schema"
)

// The schema is a published artifact, so it must at minimum parse and expose
// every envelope field by name. That is all this test checks: it says nothing
// about the types, the required list, or the type enum, and a schema can drift
// badly while still passing it. TestSchemasMatchGoTypes below is what actually
// holds the two in sync; this one is the cheap smoke test in front of it.
func TestEventSchemaIsValidJSON(t *testing.T) {
	b, err := schema.Files.ReadFile("event.schema.json")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("event.schema.json is not valid JSON: %v", err)
	}
	props, ok := doc["properties"].(map[string]any)
	if !ok {
		t.Fatal("schema has no properties")
	}
	for _, field := range []string{"v", "seq", "ts", "type", "run", "step", "attempt", "group", "trace_id", "payload"} {
		if _, ok := props[field]; !ok {
			t.Errorf("schema is missing envelope field %q", field)
		}
	}
}

// TestEventSchemaTypeExamplesMatchDeclaredTypes checks event.schema.json's
// properties.type.examples against api.DeclaredTypes(). Symmetric on
// purpose: every declared type must appear in the schema's examples, and
// every example must name a real declared type, so drift in either
// direction fails.
func TestEventSchemaTypeExamplesMatchDeclaredTypes(t *testing.T) {
	b, err := schema.Files.ReadFile("event.schema.json")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var doc struct {
		Properties struct {
			Type struct {
				Examples []string `json:"examples"`
			} `json:"type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	want := make(map[string]bool)
	for _, ty := range api.DeclaredTypes() {
		want[string(ty)] = true
	}
	got := make(map[string]bool, len(doc.Properties.Type.Examples))
	for _, s := range doc.Properties.Type.Examples {
		got[s] = true
	}

	for ty := range want {
		if !got[ty] {
			t.Errorf("event.schema.json properties.type.examples is missing %q: an event type exists in code but not in the published schema", ty)
		}
	}
	for ex := range got {
		if !want[ex] {
			t.Errorf("event.schema.json properties.type.examples lists %q, which api.DeclaredTypes() does not recognise: stale or renamed entry", ex)
		}
	}
}

func TestFrameSchemaIsValidJSON(t *testing.T) {
	b, err := schema.Files.ReadFile("frame.schema.json")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("frame.schema.json is not valid JSON: %v", err)
	}
}

// TestSchemasMatchGoTypes guards the direction that actually breaks: a schema
// that drifts from the code it documents is worse than no schema, because a
// third-party client trusts it. frame.schema.json sets additionalProperties
// false, so a Go field with no schema property makes real frames fail their own
// published schema; event.schema.json is deliberately open, but a missing
// property there still means an undocumented field on the persisted ledger.
func TestSchemasMatchGoTypes(t *testing.T) {
	cases := []struct {
		name string
		file string
		typ  reflect.Type
		reqd []string
		// open records the envelope-vs-frame ruling: events are persisted to
		// events.jsonl and re-read by arbitrary later readers, so their
		// envelope must tolerate fields a newer engine added. Frames are
		// point-to-point after a version handshake, so theirs stays closed.
		open bool
	}{
		{"event", "event.schema.json", reflect.TypeOf(api.Event{}), []string{"v", "seq", "ts", "type"}, true},
		{"frame", "frame.schema.json", reflect.TypeOf(api.Frame{}), []string{"v", "kind"}, false},
		{
			"stream_end", "stream_end.schema.json", reflect.TypeOf(api.StreamEndMarker{}),
			[]string{"stream_end", "last_seq", "overflowed", "reason", "hint"}, false,
		},
		{"overflow", "overflow.schema.json", reflect.TypeOf(api.OverflowBody{}), []string{"error", "hint"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := schema.Files.ReadFile(tc.file)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			var doc struct {
				Required             []string                   `json:"required"`
				Properties           map[string]json.RawMessage `json:"properties"`
				AdditionalProperties *bool                      `json:"additionalProperties"`
			}
			if err := json.Unmarshal(b, &doc); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}

			// Every Go field must have a schema property, or a real value fails
			// validation against its own published schema.
			var goFields, goRequired []string
			for i := range tc.typ.NumField() {
				tag := tc.typ.Field(i).Tag.Get("json")
				name, opts, _ := strings.Cut(tag, ",")
				if name == "" || name == "-" {
					continue
				}
				goFields = append(goFields, name)
				if !strings.Contains(opts, "omitempty") && !strings.Contains(opts, "omitzero") {
					goRequired = append(goRequired, name)
				}
				if _, ok := doc.Properties[name]; !ok {
					t.Errorf("Go field %q has no property in %s", name, tc.file)
				}
			}
			// And no schema property may lack a Go field.
			for name := range doc.Properties {
				if !slices.Contains(goFields, name) {
					t.Errorf("%s declares property %q with no Go field", tc.file, name)
				}
			}
			// required must be exactly the fields lacking omitempty/omitzero:
			// checked both against the pinned list (which must never shrink,
			// this being published API) and against the Go tags themselves, so
			// adding omitempty on one side alone is a failure.
			slices.Sort(doc.Required)
			want := slices.Clone(tc.reqd)
			slices.Sort(want)
			if !slices.Equal(doc.Required, want) {
				t.Errorf("%s required = %v, want %v", tc.file, doc.Required, want)
			}
			slices.Sort(goRequired)
			if !slices.Equal(goRequired, want) {
				t.Errorf("%s: Go fields without omitempty/omitzero = %v, want %v",
					tc.typ, goRequired, want)
			}
			// additionalProperties must be stated, and must match the ruling.
			if doc.AdditionalProperties == nil {
				t.Fatalf("%s does not state additionalProperties", tc.file)
			}
			if *doc.AdditionalProperties != tc.open {
				t.Errorf("%s additionalProperties = %v, want %v", tc.file, *doc.AdditionalProperties, tc.open)
			}
		})
	}
}
