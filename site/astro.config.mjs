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

export default defineConfig({
  // The custom domain (CNAME) the site is served from: see this file's own
  // top comment. senro.dev is a placeholder until a real domain is chosen;
  // change only this line.
  site: "https://senro.dev",
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
  integrations: [sitemap()],
  markdown: {
    remarkPlugins: [remarkMermaid],
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
