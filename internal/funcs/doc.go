// Package funcs is senro's registry of Go functions that are steps.
//
// An arbitrary closure has no stable identity, so it cannot be cache-keyed,
// named in plan.json, or addressed by `senro rerun --step`. Registration
// solves all three, and the explicit serializable parameters it imposes are
// worth having anyway.
//
// The registered NAME is stable API: changing it invalidates every cache
// entry for the step and breaks every recorded plan naming it.
//
// It is not in the root package because the engine invokes these functions
// and cannot import the root (the root imports the engine), while senro.Ctx
// must be nameable by a pipeline author. The type lives here and the root
// aliases it, as senro.Plan already does with internal/plan.
package funcs
