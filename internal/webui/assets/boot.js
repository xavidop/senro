// Bootstrap for the senro browser UI.
//
// A file rather than an inline <script>, because the page's Content-Security-
// Policy has no 'unsafe-inline' and is not going to grow one for four lines.
//
// Everything this file does is load the WebAssembly module and hand it the
// page. It deliberately contains no knowledge of the run, of events, or of
// what any of them mean: that is api.RunState.Apply's job, it is compiled
// into the module, and a JavaScript file that started interpreting events
// would be the second implementation this whole design exists to avoid.

(function () {
  "use strict";

  function fail(message) {
    var sub = document.getElementById("run-sub");
    if (sub) {
      sub.textContent = message;
      sub.className = "sub sub-error";
    }
  }

  if (typeof WebAssembly !== "object") {
    fail("this browser has no WebAssembly support");
    return;
  }

  var go = new Go();
  // instantiateStreaming compiles while the bytes are still arriving, which
  // is worth having for a module this size. It requires the response to be
  // served as application/wasm; the server sets that explicitly rather than
  // letting anything sniff it.
  var source = fetch("/_ui/senro-ui.wasm", { credentials: "same-origin" });

  var instantiate = WebAssembly.instantiateStreaming
    ? WebAssembly.instantiateStreaming(source, go.importObject)
    : source
        .then(function (r) {
          return r.arrayBuffer();
        })
        .then(function (bytes) {
          return WebAssembly.instantiate(bytes, go.importObject);
        });

  instantiate
    .then(function (result) {
      return go.run(result.instance);
    })
    .catch(function (err) {
      fail("could not start the client: " + err);
    });
})();
