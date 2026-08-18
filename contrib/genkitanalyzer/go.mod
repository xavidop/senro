module github.com/xavidop/senro/contrib/genkitanalyzer

go 1.26.0

require (
	github.com/firebase/genkit/go v1.11.0
	github.com/xavidop/senro v0.0.0
)

require (
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/buger/jsonparser v1.1.2 // indirect
	github.com/coder/websocket v1.8.14 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/goccy/go-yaml v1.17.1 // indirect
	github.com/google/dotprompt/go v0.0.0-20251014011017-8d056e027254 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/invopop/jsonschema v0.13.0 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mbleigh/raymond v0.0.0-20250414171441-6b3a58ab9e0a // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.8 // indirect
	github.com/xavidop/mamori v1.12.1 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel v1.36.0 // indirect
	go.opentelemetry.io/otel/metric v1.36.0 // indirect
	go.opentelemetry.io/otel/sdk v1.36.0 // indirect
	go.opentelemetry.io/otel/trace v1.36.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// senro and this module are developed and tagged together, so the edge points
// at the tree rather than at whatever the proxy last served: a change to
// senro.Analyzer or api.Failure has to break this module in the same CI run
// that made it, not one release later. The require above is the placeholder
// this resolves; consumers ignore a dependency's replace directives and pick
// up the required version instead, which is why both lines exist.
replace github.com/xavidop/senro => ../..
