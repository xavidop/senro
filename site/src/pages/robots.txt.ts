import type { APIRoute } from "astro";

// GET /robots.txt: generated, not static, so the sitemap URL is derived from
// Astro's own config instead of hardcoding a domain a second time. See
// astro.config.mjs: `site` is the one place the origin is declared.
//
// BASE_URL matters as much as `site`. On a project Pages site the whole thing
// is served under /senro/, and @astrojs/sitemap writes its entries with that
// prefix, so a Sitemap: line built from the origin alone points at a path
// that does not exist. There is no fallback origin here on purpose: a wrong
// absolute URL is worse than a relative one, and Astro only leaves `site`
// unset when nobody configured it.
export const GET: APIRoute = ({ site }) => {
  const base = import.meta.env.BASE_URL || "/";
  const path = `${base.replace(/\/$/, "")}/sitemap-index.xml`;
  const sitemapURL = site ? new URL(path, site).href : path;
  const body = `User-agent: *\nAllow: /\n\nSitemap: ${sitemapURL}\n`;
  return new Response(body, {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
};
