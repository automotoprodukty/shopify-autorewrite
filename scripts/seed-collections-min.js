// scripts/seed-collections-min.js
// One-off seeder: creates ALL collections from taxonomia.json if missing.
// It writes taxonomy.* metafields and custom.sub_collections (parent -> child links).
// It does NOT assign images.
//
// Usage:
//   SHOPIFY_SHOP=xxx SHOPIFY_TOKEN=shpat_... SHOPIFY_API_VERSION=2024-04 node scripts/seed-collections-min.js

import fs from "fs";
import path from "path";

const SHOP = process.env.SHOPIFY_SHOP;
const TOKEN = process.env.SHOPIFY_TOKEN;
const API = process.env.SHOPIFY_API_VERSION || "2024-04";

if (!SHOP || !TOKEN) {
  console.error("Set SHOPIFY_SHOP and SHOPIFY_TOKEN env vars.");
  process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let last = 0;
async function rate(min = 600) {
  const wait = Math.max(0, min - (Date.now() - last));
  if (wait) await sleep(wait);
  last = Date.now();
}
async function fetchRetry(url, opts = {}, retries = 3) {
  for (let i = 0; i <= retries; i++) {
    await rate();
    const res = await fetch(url, opts);
    if (res.status !== 429) return res;
    await sleep(500 * (i + 1));
  }
  return await fetch(url, opts);
}

function normalize(s) {
  return String(s || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function loadTaxo() {
  const file = path.join(process.cwd(), "taxonomia.json");
  const raw = JSON.parse(fs.readFileSync(file, "utf8"));
  const ensureName = (n) => {
    if (!n) return n;
    if (!n.name && n.title) n.name = n.title;
    return n;
  };
  const isTree = Array.isArray(raw) || raw?.children || raw?.name || raw?.title;
  if (isTree) {
    const rootArr = Array.isArray(raw) ? raw : [raw];
    const fix = (n) => ({ ...ensureName(n), children: (n.children || []).map(fix) });
    return rootArr.map(fix);
  }
  const brands = raw.BRANDS || [];
  const tpl = raw.TEMPLATE || {};
  const clone = (o) => JSON.parse(JSON.stringify(o));
  const expand = (brand) => {
    const repl = (s) => String(s || "").replaceAll("{{BRAND}}", brand);
    const walk = (node) => {
      const src = clone(node);
      src.name = repl(src.name || src.title || brand);
      if (src.node_slug === undefined && src.slug) src.node_slug = src.slug;
      src.children = (src.children || []).map(walk);
      return src;
    };
    return walk({ name: tpl.title || "{{BRAND}}", node_slug: tpl.node_slug, children: tpl.children || [] });
  };
  return brands.map(expand);
}

async function listCollections(pageInfo = null) {
  const url = new URL(`https://${SHOP}.myshopify.com/admin/api/${API}/custom_collections.json`);
  url.searchParams.set("limit", "250");
  if (pageInfo) url.searchParams.set("page_info", pageInfo);
  const r = await fetchRetry(url.toString(), { headers: { "X-Shopify-Access-Token": TOKEN } });
  if (!r.ok) throw new Error(await r.text());
  const j = await r.json();
  return { items: j.custom_collections || [], link: r.headers.get("link") || "" };
}

async function findByTitle(title) {
  const want = normalize(title);
  let page = null;
  while (true) {
    const { items, link } = await listCollections(page);
    const hit = items.find((c) => normalize(c.title) === want);
    if (hit) return hit;
    const m = /<([^>]+page_info=([^&>]+)[^>]*)>; rel="next"/.exec(link || "");
    if (!m) return null;
    page = m[2];
  }
}

async function createCollection(title) {
  const r = await fetchRetry(`https://${SHOP}.myshopify.com/admin/api/${API}/custom_collections.json`, {
    method: "POST",
    headers: { "X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify({ custom_collection: { title } })
  });
  if (!r.ok) throw new Error(await r.text());
  return (await r.json()).custom_collection;
}

async function ensureCollection(title) {
  return (await findByTitle(title)) || (await createCollection(title));
}

async function upsertMetafieldCollection(id, namespace, key, type, value) {
  const r = await fetchRetry(`https://${SHOP}.myshopify.com/admin/api/${API}/metafields.json`, {
    method: "POST",
    headers: { "X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify({ metafield: { owner_resource: "collection", owner_id: id, namespace, key, type, value } })
  });
  if (!r.ok) {
    const t = await r.text();
    console.warn(`Metafield fail ${id} ${namespace}.${key}: ${r.status} ${t}`);
  }
}

async function setSubCollections(parentId, childIds) {
  const gids = childIds.map((id) => `gid://shopify/Collection/${id}`);
  await upsertMetafieldCollection(parentId, "custom", "sub_collections", "list.collection_reference", JSON.stringify(gids));
}

async function seedTree() {
  const roots = loadTaxo();
  console.log(`Seeding ${roots.length} brand root(s)…`);

  for (const root of roots) {
    const stack = [];
    async function visit(node, level) {
      const title = node.name || node.title;
      const coll = await ensureCollection(title);
      // taxonomy.*
      await upsertMetafieldCollection(coll.id, "taxonomy", "level", "number_integer", String(level));
      await upsertMetafieldCollection(coll.id, "taxonomy", "node_slug", "single_line_text_field", String(node.node_slug || ""));
      if (stack.length) {
        await upsertMetafieldCollection(coll.id, "taxonomy", "parent", "number_integer", String(stack[stack.length - 1].id));
      }
      // link parent -> child
      if (stack.length) {
        await setSubCollections(stack[stack.length - 1].id, [coll.id]);
      }
      stack.push({ id: coll.id, title });
      for (const ch of node.children || []) await visit(ch, level + 1);
      stack.pop();
    }
    await visit(root, 0);
  }
  console.log("DONE ✅ (collections created + taxonomy linked; images skipped)");
}

seedTree().catch((e) => {
  console.error(e);
  process.exit(1);
});
