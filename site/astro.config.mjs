import { defineConfig } from "astro/config";
import sitemap from "@astrojs/sitemap";
import { visit } from "unist-util-visit";

// GitHub Pages project site. This is the ONE place the site's domain is
// declared: everything else (llms.txt/llms-full.txt, the sitemap, canonical
// links) derives it from Astro's `site`/`base` context rather than
// hardcoding it a second time. Change it here and it changes everywhere.
//
// The Pages workflow passes BASE_PATH (usually "/senro"); locally it's "/".
// Normalized to always end in a trailing slash: import.meta.env.BASE_URL
// mirrors this value verbatim (Astro does not add the slash back), and every
// page/component in this site builds hrefs as `${base}docs/...`. Without a
// guaranteed trailing slash here, that concatenation silently produces
// "/senrodocs" instead of "/senro/docs". actions/configure-pages's own
// base_path output has no trailing slash, so this was reproduced with a real
// `BASE_PATH=/senro build`, not a hypothetical.
const rawBase = process.env.BASE_PATH || "/";
const base = rawBase.endsWith("/") ? rawBase : `${rawBase}/`;

// remarkMermaid turns ```mermaid fenced code blocks into a <pre class="mermaid">
// container that the client-side mermaid renderer (see DocsLayout.astro) picks
// up, instead of letting Shiki syntax-highlight the diagram source as code. The
// diagram text is HTML-escaped so characters like the --> arrow survive as
// textContent for mermaid to parse.
function remarkMermaid() {
  return (tree) => {
    visit(tree, "code", (node, index, parent) => {
      if (node.lang !== "mermaid" || !parent || typeof index !== "number") return;
      const escaped = String(node.value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
      parent.children[index] = {
        type: "html",
        value: `<pre class="mermaid">${escaped}</pre>`,
      };
    });
  };
}

// remarkBaseLinks prefixes root-relative Markdown links with the configured
// base. Astro rewrites `base` into component hrefs but NOT into links written
// inside Markdown, so on a project Pages site (base "/senro/") a page linking
// "/docs/steps/" resolves to <origin>/docs/steps/ and 404s: the docs are at
// <origin>/senro/docs/steps/. Every cross-link in the docs tree is written
// root-relative, so without this the whole tree is broken the moment it is
// served under a base, and only there, which is why a base-less build looks
// fine.
//
// Anchors, external URLs and protocol-relative URLs are left alone, as is
// anything already carrying the base, so the transform is idempotent.
function remarkBaseLinks() {
  const prefix = base.replace(/\/$/, "");
  return (tree) => {
    if (!prefix) return;
    const fix = (node) => {
      const url = node.url;
      if (typeof url !== "string") return;
      if (!url.startsWith("/") || url.startsWith("//")) return;
      if (url === prefix || url.startsWith(prefix + "/")) return;
      node.url = prefix + url;
    };
    visit(tree, "link", fix);
    visit(tree, "definition", fix);
  };
}

// movedPages keeps the URLs published before the "write your own X" pages were
// moved next to the built-ins they extend.
//
// The target is base-prefixed by hand, unlike the key. Astro rewrites `base`
// into the file layout (so the key stays root-relative and the emitted page
// lands where the base serves it) but NOT into a redirect's target, so a bare
// "/docs/analyzers/custom" sends a GitHub Pages visitor to <origin>/docs/...
// when the site lives at <origin>/senro/docs/... . The link checker cannot see
// it either: it strips the base off every href before resolving, so both forms
// resolve to the same dist file and the broken one passes. This is the same
// hazard remarkBaseLinks exists for on Markdown links.
const moved = {
  "/docs/extend/analyzer": "/docs/analyzers/custom",
  "/docs/extend/analyzer-genkit": "/docs/analyzers/genkit",
  "/docs/extend/notifier": "/docs/notifications/custom",
  "/docs/extend/trigger-source": "/docs/triggers/custom",
  "/docs/extend/unit-graph": "/docs/monorepo/unit-graphs/custom",
};
const basePrefix = base.replace(/\/$/, "");
const movedPages = Object.fromEntries(
  Object.entries(moved).map(([from, to]) => [from, basePrefix + to]),
);

export default defineConfig({
  // The origin the site is actually served from. Canonical links, the
  // sitemap and robots.txt's Sitemap line are all derived from it, so a
  // value that is not where the site lives points crawlers and readers at
  // a host that does not answer.
  //
  // This is the GitHub Pages project URL, which is what
  // `gh api repos/xavidop/senro/pages` reports and where the Docs workflow
  // publishes. To move to a custom domain, do BOTH: add site/public/CNAME
  // containing the bare domain, and change this line to match. Doing only
  // one leaves the built HTML disagreeing with where it is served.
  site: "https://xavidop.github.io",
  base,
  // Internal doc links are root-relative (/docs/...), so they resolve the same
  // with or without a trailing slash; "ignore" lets the dev server serve both
  // forms rather than 404-ing bare URLs.
  trailingSlash: "ignore",
  devToolbar: { enabled: false },
  build: { format: "directory" },
  // /llms.txt (index) and /llms-full.txt (all docs as one Markdown file) are
  // native static endpoints under src/pages, so they render in dev and build
  // alike. See src/lib/docs-content.ts.
  // The four "write your own X" pages and the two analyzer pages used to live
  // under /docs/extend/. They now sit next to the built-ins they extend, so a
  // reader arrives at the shipped destinations, providers, graphs and analyzers
  // first and at the interface second. These keep every link published before
  // that move working.
  redirects: movedPages,
  integrations: [sitemap()],
  // mermaid is imported dynamically, from a client script that only runs on
  // pages carrying a diagram. Vite therefore does not see it during its
  // initial dependency scan and pre-bundles it lazily, on the first page that
  // asks - and that re-optimization invalidates the module graph mid-request,
  // so the very import that triggered it fails with
  // "504 (Outdated Optimize Dep)" and the diagram never renders. Naming it
  // here pre-bundles it up front, so the dynamic import always hits a warm,
  // stable dep and diagrams render on first load.
  vite: {
    optimizeDeps: {
      include: ["mermaid"],
    },
  },
  markdown: {
    remarkPlugins: [remarkMermaid, remarkBaseLinks],
    shikiConfig: {
      // Two themes, not one. A single light theme was pinned here while the
      // site itself has a full dark palette, so in dark mode every code block
      // kept its light-theme colours: punctuation is near-black in
      // github-light, which on a dark background is invisible. `type Sink
      // interface {` read as `type Sink interface`, and `Emit(api.Event)` lost
      // both parens and the dot.
      //
      // Shiki emits the second theme as a --shiki-dark custom property per
      // token; DocsLayout switches to it under the same three conditions the
      // rest of the palette uses.
      themes: {
        light: "github-light",
        dark: "github-dark",
      },
      wrap: false,
    },
  },
});
