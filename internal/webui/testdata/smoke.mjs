// A smoke test for the compiled WebAssembly client, run under Node.
//
// Everything else about the browser UI is tested in Go: the fold is shared
// (there is only one), the resume loop is driven against a real attach
// server, the presentation layer is a pure function of folded state, and
// the access boundary is exercised over real HTTP. What none of that
// touches is whether the thing a browser actually downloads BOOTS: whether
// fetch.go's syscall/js code resolves a promise correctly, whether reading
// a fetch body's ReadableStream produces the bytes the NDJSON decoder
// expects, and whether the renderer puts the run on the page.
//
// That gap is real and it is the kind that ships. This closes it without a
// browser: Node has WebAssembly, fetch, and streams, so the only thing
// missing is a DOM, and the client uses a small enough part of one to stub
// honestly. What is stubbed is stated below; what is not stubbed is the
// client, which is the same binary the server serves.
//
// Usage: node smoke.mjs <one-time-link> <path-to-senro-ui.wasm> <path-to-wasm_exec.js> <path-to-index.html>
// Prints a line per assertion and exits non-zero on the first failure.

import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const [, , link, wasmPath, , indexPath] = process.argv;
if (!link || !wasmPath || !indexPath) {
  console.error(
    "usage: node smoke.mjs <one-time-link> <path-to-wasm> <path-to-wasm_exec.js> <path-to-index.html>",
  );
  process.exit(2);
}

const origin = new URL(link).origin;

function fail(msg) {
  console.error("FAIL: " + msg);
  process.exit(1);
}

// --- Walk the one-time link, exactly as a browser would ------------------

const handoff = await fetch(link, { redirect: "manual" });
if (handoff.status !== 303) {
  fail(`the one-time link answered ${handoff.status}, want 303`);
}
const setCookie = handoff.headers.get("set-cookie");
if (!setCookie) {
  fail("the handoff set no cookie");
}
const cookie = setCookie.split(";")[0];

// --- A DOM, stubbed to exactly what the client touches -------------------
//
// textContent, className, and the four calls view.go makes. Nodes record
// their children so an assertion can ask what was rendered.

class Node {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this._text = "";
    this.className = "";
    this.listeners = {};
    this.parent = null;
  }
  get textContent() {
    if (this.children.length === 0) return this._text;
    return this.children.map((c) => c.textContent).join(" ");
  }
  set textContent(v) {
    // Assigning textContent replaces every child, which is exactly what the
    // real DOM does and is how view.go clears a list.
    this._text = v;
    this.children = [];
  }
  appendChild(c) {
    c.parent = this;
    this.children.push(c);
    return c;
  }
  setAttribute(k, v) {
    this.attrs[k] = v;
  }
  getAttribute(k) {
    return k in this.attrs ? this.attrs[k] : null;
  }
  // Recorded rather than discarded, so the harness can actually click
  // something. A no-op here would mean the control buttons were only ever
  // checked for existing, which is the half of the feature that cannot
  // break.
  addEventListener(type, fn) {
    (this.listeners[type] ??= []).push(fn);
  }
  // click dispatches at this node, as a delegated handler sees it: the
  // event's target is the node clicked, and view.go's handlers read their
  // data attributes off it.
  click() {
    for (const node of [this, ...ancestorsOf(this)]) {
      for (const fn of node.listeners.click ?? []) {
        fn({ target: this });
      }
    }
  }
  // find walks this subtree for the first node carrying attr=value.
  find(attr, value) {
    if (this.getAttribute(attr) === value) return this;
    for (const c of this.children) {
      const hit = c.find(attr, value);
      if (hit) return hit;
    }
    return null;
  }
}

// ancestorsOf is the parent chain, which the stub maintains only well
// enough for delegation: appendChild records a parent.
function ancestorsOf(node) {
  const out = [];
  for (let p = node.parent; p; p = p.parent) out.push(p);
  return out;
}

// The stubbed DOM's elements come from the REAL page, parsed out of the
// same assets/index.html the server serves, rather than from a list kept
// here.
//
// That is not tidiness. A hand-maintained list is a second description of
// the page, and when the client learned to look up an element this list did
// not have, the client panicked at boot on a null -- which is what a
// browser would have done too, but for the opposite reason: the browser's
// page HAS the element. A harness that has to be updated alongside the page
// eventually disagrees with it, and then it is either failing on something
// real or passing on something broken, with no way to tell which from here.
const ids = [...readFileSync(indexPath, "utf8").matchAll(/\bid="([^"]+)"/g)].map(
  (m) => m[1],
);
if (ids.length === 0) {
  fail(`no element ids found in ${indexPath}: the harness would stub an empty page`);
}
const byId = Object.fromEntries(ids.map((id) => [id, new Node("div")]));

globalThis.document = {
  getElementById: (id) => byId[id] ?? null,
  createElement: (tag) => new Node(tag),
};
globalThis.location = { origin, href: origin + "/" };

// requestAnimationFrame, as a timer. The client paints on frames and the
// real one does not fire in a headless process.
globalThis.requestAnimationFrame = (fn) => setTimeout(() => fn(Date.now()), 16);

// Every request the client makes is same-origin and relative to
// location.origin, and the browser would attach the session cookie itself.
// Node does not have a cookie jar, so this does what the browser does.
const nodeFetch = globalThis.fetch;
globalThis.fetch = (input, init = {}) => {
  const headers = new Headers(init.headers || {});
  headers.set("cookie", cookie);
  // The client is same-origin; a browser would send this and the server
  // checks it.
  headers.set("sec-fetch-site", "same-origin");
  // A browser sets Origin on every POST it makes, and POST /api/control
  // refuses a request without one: on loopback, SameSite does not tell this
  // server apart from another port on the same host, so the origin is the
  // check that does. Setting it here is emulating the browser, not
  // bypassing the server.
  if ((init.method || "GET").toUpperCase() !== "GET") {
    headers.set("origin", origin);
  }
  return nodeFetch(input, { ...init, headers });
};

// --- Boot the client -----------------------------------------------------

const require = createRequire(import.meta.url);
const wasmExec = process.argv[4] || null;
if (wasmExec) {
  require(wasmExec);
} else {
  fail("no wasm_exec.js path given");
}

const go = new Go();
const bytes = readFileSync(wasmPath);
const result = await WebAssembly.instantiate(bytes, go.importObject);
// go.run resolves when the Go program's main returns, which for this client
// is never: it ends in select{}. Not awaited, deliberately.
go.run(result.instance);

// --- Assert the run reached the page ------------------------------------

// Two things are asserted, and the second is the one that matters.
//
// "build" and the run's name come from GET /api/state, the snapshot. A
// client whose streaming path was completely broken would still show them,
// so on their own they prove only that a single fetch worked.
//
// "later" and "build" reaching a terminal state come from events the Go
// side emits only AFTER this client has a subscription open. They can only
// arrive down GET /api/stream, which means they have been through the fetch
// body's ReadableStream, the NDJSON decoder, and api.RunState.Apply. That
// is the whole client, running, doing its actual job.

const deadline = Date.now() + 30_000;
let sawSnapshot = false;
let lastSeen = "";

while (Date.now() < deadline) {
  const name = byId["run-name"].textContent;
  const rows = byId["steps"].children;
  const ids = rows.map((li) => li.getAttribute("data-step"));
  lastSeen = `run-name=${JSON.stringify(name)} steps=${JSON.stringify(ids)}`;

  if (!sawSnapshot && name === "smoke-run" && ids.includes("build")) {
    sawSnapshot = true;
    console.log("OK snapshot: run-name=" + name + " steps=" + JSON.stringify(ids));
  }

  if (sawSnapshot && ids.includes("later")) {
    const build = rows.find((li) => li.getAttribute("data-step") === "build");
    if (!build.textContent.includes("succeeded")) {
      // The step this client saw start is now finished on the server. If
      // the row still says running, events reached the page but the fold
      // did not apply them.
      lastSeen = "build row is " + JSON.stringify(build.textContent);
      await new Promise((r) => setTimeout(r, 25));
      continue;
    }
    const status = byId["run-status"].textContent;
    if (!status) {
      fail("the run has no status badge");
    }
    console.log("OK stream: steps=" + JSON.stringify(ids));
    console.log("OK stream: build row=" + JSON.stringify(build.textContent));
    console.log("OK run status=" + status);
    await assertAControlRequestGoesThrough();
    console.log("SMOKE PASSED");
    process.exit(0);
  }
  await new Promise((r) => setTimeout(r, 25));
}
fail(
  (sawSnapshot
    ? "the snapshot rendered but nothing arrived down the event stream; "
    : "the client never rendered the snapshot; ") + "last saw " + lastSeen
);


// --- Assert a control button actually does something ---------------------
//
// Rendering a button proves nothing about pressing one. This clicks the
// run's Pause control and waits for the page to report an outcome, which
// exercises the whole path that only exists in the compiled binary:
// view.go's delegated click handler reading its data attributes, main.go
// encoding an api.Frame, fetch.go's POST through syscall/js, the UI
// server's origin check and op allowlist, and the answer coming back.
//
// Pause specifically, because it is the one offered control that carries no
// confirmation: window.confirm is not stubbed, and an action that needed it
// would be asserting the harness rather than the client.
//
// What is NOT asserted is that the engine accepted it. There is no engine
// behind this attach server, only a hub, so the honest expectation is an
// answer, not a success. An answer is the thing that proves the path.
async function assertAControlRequestGoesThrough() {
  const button = byId["run-actions"].find("data-op", "run.pause");
  if (!button) {
    const offered = byId["run-actions"].children.map((b) =>
      b.getAttribute("data-op"),
    );
    fail(
      "the run offers no pause control on a live run; offered: " +
        JSON.stringify(offered),
    );
  }
  if (button.getAttribute("data-op-confirm") !== null) {
    fail("the pause control now asks for confirmation, which this harness does not stub");
  }

  button.click();

  const until = Date.now() + 15_000;
  while (Date.now() < until) {
    const notice = byId["notice"].textContent;
    if (notice && notice.includes("run.pause")) {
      console.log("OK control: notice=" + JSON.stringify(notice));
      return;
    }
    await new Promise((r) => setTimeout(r, 25));
  }
  fail("clicking pause produced no outcome on the page within 15s");
}
