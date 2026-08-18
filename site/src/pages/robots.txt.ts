import type { APIRoute } from "astro";

// GET /robots.txt: generated, not static, so the sitemap URL is derived
// from Astro's own `site` config (astro.config.mjs) instead of hardcoding
// the domain a second time. See astro.config.mjs's top comment: that file is
// the one place the site's domain is declared.
export const GET: APIRoute = ({ site }) => {
  const sitemapURL = new URL("sitemap-index.xml", site ?? "https://senro.dev/").href;
  const body = `User-agent: *\nAllow: /\n\nSitemap: ${sitemapURL}\n`;
  return new Response(body, {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
};
