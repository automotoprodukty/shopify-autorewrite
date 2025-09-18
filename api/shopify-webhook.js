// Serverless endpoint pre Shopify webhook (Vercel)
// 1) Overí HMAC podpis
// 2) Dotiahne produkt z Admin GraphQL
// 3) Zavolá OpenAI (JSON výstup)
// 4) Upraví produkt (title, description, tags, option names)
// 5) Preloží hodnoty vo variantoch podľa nového zoznamu (mapovanie podľa indexu)
// 6) Zapíše metafield automation.processed=true

import crypto from "crypto";
import fs from "fs";
import path from "path";
// ==== Feature flags (env) ====
const ENABLE_COLLECTIONS_ATTACH = (process.env.ENABLE_COLLECTIONS_ATTACH || "true") === "true";
const DRY_RUN = (process.env.DRY_RUN || "true") === "true"; // odporúčam nechať najprv true
// --- Load taxonomy.json at startup and keep in memory
let taxonomia = null;
function loadTaxonomia() {
  if (taxonomia) return taxonomia;
  const file = path.join(process.cwd(), "taxonomia.json");
  try {
    const data = fs.readFileSync(file, "utf8");
    const raw = JSON.parse(data);

    // Normalizer helpers
    const ensureName = (node) => {
      if (!node) return node;
      if (!node.name && node.title) node.name = node.title;
      return node;
    };

    // Case A: already a full tree (array or single root)
    const isPreExpandedTree = Array.isArray(raw) || (raw && raw.children) || (raw && raw.name) || (raw && raw.title);

    if (isPreExpandedTree) {
      const rootArr = Array.isArray(raw) ? raw : [raw];
      // ensure every node has name & children
      const fix = (n) => {
        n = ensureName({ ...n });
        n.children = Array.isArray(n.children) ? n.children.map(fix) : [];
        return n;
      };
      taxonomia = rootArr.map(fix);
      console.log("TAXO: loaded pre-expanded tree with", taxonomia.length, "root nodes");
      return taxonomia;
    }

    // Case B: templated format with BRANDS + TEMPLATE ({ title: "{{BRAND}}", children:[...] })
    const brands = Array.isArray(raw.BRANDS) ? raw.BRANDS : [];
    const tpl = raw.TEMPLATE || null;
    if (!tpl || brands.length === 0) {
      throw new Error("taxonomia.json: unsupported format. Expect pre-expanded tree OR {BRANDS, TEMPLATE}.");
    }

    // Deep clone util
    const deepClone = (o) => JSON.parse(JSON.stringify(o));

    const expandBrand = (brand) => {
      const replaceBrandTokens = (s) => String(s || "").replaceAll("{{BRAND}}", brand);
      const walk = (node) => {
        const src = ensureName(deepClone(node));
        src.name = replaceBrandTokens(src.name || src.title || brand);
        if (src.node_slug === undefined && src.slug) src.node_slug = src.slug; // compatibility
        if (src.facets && !Array.isArray(src.facets)) {
          // allow comma-separated string
          src.facets = String(src.facets).split(",").map(x => x.trim()).filter(Boolean);
        }
        src.children = Array.isArray(src.children) ? src.children.map(walk) : [];
        return src;
      };
      const root = walk({ name: tpl.title || "{{BRAND}}", children: tpl.children || [], node_slug: tpl.node_slug, facets: tpl.facets });
      return root;
    };

    const expanded = brands.map(expandBrand);
    taxonomia = expanded;
    console.log("TAXO: expanded template for", brands.length, "brands");
    return taxonomia;
  } catch (e) {
    console.error("Failed to load/expand taxonomia.json:", e);
    taxonomia = [];
    return taxonomia;
  }
}

// --- Helper: Given leaf collection name, find full branch (ancestor names up to root) in taxonomy
function getTaxonomyBranchFromLeaf(leafName) {
  const tax = loadTaxonomia();
  if (!tax || tax.length === 0) return [];
  const normalize = (s) => String(s || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[‐‑–—]/g, "-");
  const want = normalize(leafName);

  let foundPath = [];
  const visit = (node, path=[]) => {
    if (!node) return false;
    const nm = normalize(node.name || node.title);
    if (nm === want) { foundPath = [...path, node]; return true; }
    if (Array.isArray(node.children)) {
      for (const ch of node.children) {
        if (visit(ch, [...path, node])) return true;
      }
    }
    return false;
  };

  for (const root of tax) { if (visit(root, [])) break; }
  return foundPath.map(n => n.name || n.title);
}

// --- Helper: Given leaf collection name, return full branch as node objects (with node_slug, etc.)
function getTaxonomyBranchNodesFromLeaf(leafName) {
  const tax = loadTaxonomia();
  if (!tax || tax.length === 0) return [];
  const normalize = (s) => String(s || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[‐‑–—]/g, "-");
  const want = normalize(leafName);

  let foundPath = [];
  const visit = (node, path=[]) => {
    if (!node) return false;
    const nm = normalize(node.name || node.title);
    if (nm === want) { foundPath = [...path, node]; return true; }
    if (Array.isArray(node.children)) {
      for (const ch of node.children) {
        if (visit(ch, [...path, node])) return true;
      }
    }
    return false;
  };

  for (const root of tax) { if (visit(root, [])) break; }
  return foundPath;
}

// --- Brand detection (from product/vendor/tags and AI tags later)
function detectBrandFromProduct(p, out = {}) {
  const norm = (s)=>String(s||"").toLowerCase();
  const hay = [
    p?.vendor, p?.title, ...(p?.tags||[]),
    ...(Array.isArray(out?.base_tags)? out.base_tags: []),
    ...(Array.isArray(out?.subtags)? out.subtags: [])
  ].filter(Boolean).map(norm).join(" ");
  const map = {
    "audi":"AUDI","bmw":"BMW","mercedes-benz":"MERCEDES-BENZ","mercedes":"MERCEDES-BENZ",
    "škoda":"ŠKODA","skoda":"ŠKODA","volkswagen":"VOLKSWAGEN","vw":"VOLKSWAGEN",
    "seat":"SEAT","peugeot":"PEUGEOT","citroen":"CITROEN","renault":"RENAULT","ford":"FORD",
    "toyota":"TOYOTA","honda":"HONDA","hyundai":"HYUNDAI","kia":"KIA","mazda":"MAZDA","opel":"OPEL",
    "nissan":"NISSAN","fiat":"FIAT","volvo":"VOLVO","mini":"MINI","porsche":"PORSCHE","tesla":"TESLA","dacia":"DACIA"
  };
  for (const k of Object.keys(map)) {
    if (hay.includes(k)) return map[k];
  }
  return null;
}

// --- Leaves under a brand root (return only leaf nodes with node_slug)
function getBrandLeaves(brandName) {
  const tax = loadTaxonomia();
  if (!tax?.length || !brandName) return [];
  const norm = (s)=>String(s||"").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"");
  const root = tax.find(r => norm(r.name||r.title) === norm(brandName));
  if (!root) return [];
  const leaves = [];
  (function walk(n, path=[]) {
    const kids = Array.isArray(n.children) ? n.children : [];
    const me = { ...n, path: [...path, (n.name||n.title)] };
    if (!kids.length) {
      if (me.node_slug) leaves.push(me);
    } else {
      kids.forEach(ch => walk(ch, me.path));
    }
  })(root, []);
  return leaves; // [{name,title,node_slug,facets,path:[...]}]
}

// --- Find full branch by node_slug within a brand root
function getBranchBySlug(brandName, nodeSlug) {
  const tax = loadTaxonomia();
  if (!tax?.length || !brandName || !nodeSlug) return [];
  const norm = (s)=>String(s||"").toLowerCase();
  const root = tax.find(r => norm(r.name||r.title) === norm(brandName));
  if (!root) return [];
  const path = [];
  let found = null;
  (function dfs(n) {
    path.push(n);
    if (String(n.node_slug||"") === String(nodeSlug)) { found = [...path]; }
    if (!found) (n.children||[]).forEach(dfs);
    if (!found) path.pop();
  })(root);
  return found || [];
}

// --- Find brand root and an immediate child node by name or node_slug
function findBrandRoot(brandName){
  const tax = loadTaxonomia();
  if (!tax?.length || !brandName) return null;
  const norm = (s)=>String(s||"").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"");
  return tax.find(r => norm(r.name||r.title) === norm(brandName)) || null;
}
function findBrandChildNode(brandName, childNameOrSlug){
  const root = findBrandRoot(brandName);
  if (!root || !childNameOrSlug) return null;
  const norm = (s)=>String(s||"").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"");
  const want = norm(childNameOrSlug);
  const kids = Array.isArray(root.children) ? root.children : [];
  return kids.find(ch => norm(ch.node_slug||"")===want || norm(ch.name||ch.title)===want) || null;
}
// --- Collect all leaves under a specific node (subtree)
function getLeavesUnderNode(node){
  if (!node) return [];
  const leaves = [];
  (function walk(n, path=[]) {
    const kids = Array.isArray(n.children) ? n.children : [];
    const me = { ...n, path: [...path, (n.name||n.title)] };
    if (!kids.length) {
      if (me.node_slug) leaves.push(me);
    } else {
      kids.forEach(ch => walk(ch, me.path));
    }
  })(node, []);
  return leaves;
}

// --- Lightweight AI call to pick collection slugs from whitelist
async function aiPickCollectionSlugs({ title, vendor, tags, description, allowedLeaves, taxonomyTree }) {
  const sys = `ÚLOHA: Vyber presne tie node_slug(y) z poskytnutého zoznamu, ktoré najlepšie zodpovedajú produktu.
- Vráť len JSON {"collections_node_slugs":[...]}.
- Používaj IBA node_slug z whitelistu. Ak si neistý, vráť prázdne pole.
- Nevracaj iné kľúče, názvy ani texty.`;
  const user = {
    product: {
      title, vendor, tags: (tags||[]).slice(0,20),
      description: String(description||"").slice(0,2000)
    },
    allowed_leaves: Array.isArray(allowedLeaves) ? allowedLeaves : [], // [{slug,label}]
    taxonomy_tree: taxonomyTree
  };
  const body = {
    model: "gpt-4o-mini",
    temperature: 0.0,
    response_format: { type: "json_object" },
    messages: [
      { role: "system", content: sys },
      { role: "user", content: JSON.stringify(user) }
    ]
  };
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });
  const j = await r.json();
  const raw = j?.choices?.[0]?.message?.content || "{}";
  try { return JSON.parse(raw); } catch { return { collections_node_slugs: [] }; }
}


export const config = { api: { bodyParser: false } };

const ADMIN = (path) =>
  `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/${path}`;

async function readRawBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

async function gql(query, variables) {
  const r = await fetch(ADMIN("graphql.json"), {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ query, variables })
  });
  const j = await r.json();
  if (j.errors) throw new Error(JSON.stringify(j.errors));
  if (j.data?.userErrors?.length) throw new Error(JSON.stringify(j.data.userErrors));
  return j.data;
}

async function getProduct(productGid) {
  return gql(
    `
    query($id: ID!) {
      product(id: $id) {
        id
        title
        vendor
        descriptionHtml
        tags
        options { id name position values }
        variants(first: 250) {
          edges { node { id title selectedOptions { name value } } }
        }
        metafields(first: 10, namespace: "automation") {
          edges { node { key value } }
        }
      }
    }
  `,
    { id: productGid }
  );
}

async function openAIRewrite(payloadText) {
  // Použijeme Chat Completions – stabilný tvar odpovede
  const body = {
    model: "gpt-4o-mini",
    temperature: 0.2,
    response_format: { type: "json_object" },
    messages: [
      {
        role: "system",
        content:
          "Si asistent pre úpravu Shopify produktov v slovenčine. Každý výstup musí byť 100 % slovensky (bez angličtiny, okrem skratiek typu LED, USB). Vráť iba čistý JSON podľa kľúčov: title (string), description (string), base_tags (array), subtags (array), extra_tags (array), collections (array), options (array objektov { name, position?, values? }). Nikdy nevymýšľaj parametre, ktoré nie sú vo vstupe. Pravidlá: 1) Názov: krátky, výstižný, bez emoji. Ak názov obsahuje variant/prídavné meno, uveď ho ZA pomlčkou: „Fólia na okno – priesvitná“. 2) Popis (vždy po slovensky) — presné formátovanie: • Krátky úvodný odsek (problém → riešenie). Po ňom prázdny riadok. • <strong>🚗 Výhody:</strong> (presne tento nadpis, bez medzery pod ním) body začínajú ✅, minimálne 4, žiadne <ul>. • prázdny riadok, potom <strong>📦 Špecifikácia:</strong> (presne tento nadpis) odrážky začínajú znakom •, len parametre zo vstupu; značku neuvádzaj, ak je NoEnName_Null. • prázdny riadok, potom <strong>🎯 Pre koho je určený:</strong> (presne tento nadpis) odrážky začínajú znakom •, minimálne 3. • Medzi nadpisom a prvou odrážkou NESMIE byť prázdny riadok; medzi poslednou odrážkou sekcie a ďalším nadpisom PRÁZDNÝ riadok BYŤ MUSÍ. 3) Tagy: • Základné tagy: buď konkrétna značka auta, alebo „Univerzálny“ + PRESNE jeden z: Interiér | Exteriér | Starostlivosť o auto | Vychytávky | Oblečenie | Doplnky. – Značky sa môžu kombinovať (napr. Audi, Mercedes). – Ak je produkt „Univerzálny“, musí mať len „Univerzálny“ + jeden z uvedených (nekombinovať medzi sebou). Správne: „Univerzálny“, „Interiér“. Nesprávne: „Univerzálny“, „Interiér“, „Exteriér“. • SubTagy: {Značka} {Interiér|Exteriér|Komponenty|Oblečenie}. Pre každú ZNAČKU len jeden typ. Nesprávne: „Audi Exteriér“, „Audi Oblečenie“. Správne: „Audi Exteriér“, „Peugeot Exteriér“. • Extra tagy: voľné kľúčové slová (modely, elektrika, osvetlenie…). 4) Kolekcie: • Určujú sa IBA zo Základných tagov a SubTagov. Každý ne-univerzálny produkt musí mať kolekciu základného tagu aj príslušného SubTagu (napr. Audi, Audi Exteriér). Univerzálne produkty majú kolekciu len podľa základného tagu (Interiér/Exteriér/Starostlivosť o auto/Vychytávky/Oblečenie/Doplnky). 5) Varianty/Options: • Ak je 1 option → premenuj na „Varianty“ a prelož všetky values do slovenčiny. • Ak sú 2+ options → prelož názvy optionov aj všetky values (napr. Color→Farba, pcs→ks, Black→čierna). Zachovaj mapovanie variantov index-to-index. Ak produkt obsahuje option values, vždy vráť kompletný preložený zoznam values pre každú option (napr. „pcs“ → „ks“, „Black“ → „čierna“). Ak je len 1 option, premenuj ju na „Varianty“ a prelož všetky values. 6) Anti-loop: metafield automation.processed = true. 7) Výstup: striktne po slovensky; žiadne anglické slová v názve/odrážkach; zachovaj presnú diakritiku a formát nadpisov a odrážok. Dodrž presné zalomenia riadkov: po úvodnom odseku vlož nový prázdny riadok; pred každým nadpisom vlož jeden prázdny riadok; po nadpise žiadny prázdny riadok; každá odrážka na novom riadku. Vráť len čistý JSON bez komentárov alebo dodatočného textu."
      },
      { role: "user", content: payloadText }
    ]
  };

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const j = await r.json();

  // Očakávaný tvar: j.choices[0].message.content (string s JSONom)
  const content =
    j?.choices?.[0]?.message?.content ??
    (() => {
      throw new Error("OpenAI: no content in chat completion");
    })();

  // Pre istotu odstránime prípadné trojité backticky
  const clean = content.trim().replace(/^```(?:json)?\n?/i, "").replace(/```$/, "");

  return JSON.parse(clean);
}

// --- Enforce exact description formatting (blank lines & headings)
function formatDescription(desc) {
  if (!desc) return "";
  let s = String(desc).replace(/\r\n/g, "\n").trim();

  // Ensure strong headings exist (wrap plain headings if needed)
  s = s.replace(/(^|\n)\s*🚗\s*Výhody:\s*/g, "\n\n<strong>🚗 Výhody:</strong>\n");
  s = s.replace(/(^|\n)\s*📦\s*Špecifikácia:\s*/g, "\n\n<strong>📦 Špecifikácia:</strong>\n");
  s = s.replace(/(^|\n)\s*🎯\s*Pre koho je určený:\s*/g, "\n\n<strong>🎯 Pre koho je určený:</strong>\n");

  // If headings already have <strong>, normalize spacing around them
  s = s.replace(/\s*<strong>🚗\s*Výhody:\s*<\/strong>\s*/g, "\n\n<strong>🚗 Výhody:</strong>\n");
  s = s.replace(/\s*<strong>📦\s*Špecifikácia:\s*<\/strong>\s*/g, "\n\n<strong>📦 Špecifikácia:</strong>\n");
  s = s.replace(/\s*<strong>🎯\s*Pre koho je určený:\s*<\/strong>\s*/g, "\n\n<strong>🎯 Pre koho je určený:</strong>\n");

  // After headings: no blank line allowed (already ensured by the trailing \n above)

  // Make sure list items start on a new line
  // Convert inline "✅" and "•" into line-start items
  s = s
    // collapse multiple spaces
    .replace(/[ \t]+/g, " ")
    // ensure each '✅ ' starts on a new line (but not duplicate newlines)
    .replace(/(?:\s+)?✅\s*/g, "\n✅ ")
    // ensure each '• ' starts on a new line
    .replace(/(?:\s+)?•\s*/g, "\n• ");

  // Remove accidental extra blank lines except those we want between sections
  s = s.replace(/\n{3,}/g, "\n\n");

  // Ensure there is a blank line before each heading (already enforced), and exactly one blank line between sections
  s = s
    .replace(/\n+\s*<strong>🚗 Výhody:<\/strong>\n/g, "\n\n<strong>🚗 Výhody:</strong>\n")
    .replace(/\n+\s*<strong>📦 Špecifikácia:<\/strong>\n/g, "\n\n<strong>📦 Špecifikácia:</strong>\n")
    .replace(/\n+\s*<strong>🎯 Pre koho je určený:<\/strong>\n/g, "\n\n<strong>🎯 Pre koho je určený:</strong>\n");

  // Trim leading/trailing newlines
  s = s.replace(/^\n+|\n+$/g, "");
  // Convert newlines to explicit HTML breaks so Shopify renders the layout
  s = s.replace(/\n/g, "<br>");
  return s;
}

async function productUpdate(input) {
  return gql(
    `
    mutation($input: ProductInput!) {
      productUpdate(input: $input) {
        product { id title tags descriptionHtml options { name position } }
        userErrors { field message }
      }
    }
  `,
    { input }
  );
}

async function variantsBulkUpdate(productId, variants) {
  return gql(
    `
    mutation($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        product { id }
        userErrors { field message }
      }
    }
  `,
    { productId, variants }
  );
}

async function metafieldsSet(ownerId) {
  return gql(
    `
    mutation($m: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $m) {
        metafields { id }
        userErrors { field message }
      }
    }
  `,
    {
      m: [
        {
          ownerId,
          type: "boolean",
          namespace: "automation",
          key: "processed",
          value: "true"
        }
      ]
    }
  );
}


const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// --- Simple Shopify API rate limiter & retry (handles 429)
let __lastShopifyCallAt = 0;
async function rateLimit(minIntervalMs = 600) { // ~2 calls/sec hard limit
  const now = Date.now();
  const wait = Math.max(0, minIntervalMs - (now - __lastShopifyCallAt));
  if (wait) await sleep(wait);
  __lastShopifyCallAt = Date.now();
}

async function fetchWithRetry(url, opts = {}, retries = 3) {
  let attempt = 0;
  while (true) {
    await rateLimit();
    const res = await fetch(url, opts);
    if (res.status !== 429) return res;
    if (attempt >= retries) return res; // give up, caller will handle
    const backoff = 500 * (attempt + 1);
    console.warn(`429 rate limit on ${url} -> retrying in ${backoff}ms (attempt ${attempt + 1}/${retries})`);
    await sleep(backoff);
    attempt++;
  }
}

async function waitForProduct(id, attempts = 12, delayMs = 1500) {
  for (let i = 0; i < attempts; i++) {
    try {
      const data = await getProduct(id);
      if (data?.product) return data.product;
    } catch (e) {
      // ignorujeme, skúsime znova
    }
    await sleep(delayMs);
  }
  return null;
}

async function productOptionsUpdate(productId, productOptions) {
  return gql(`
    mutation($productId: ID!, $productOptions: [ProductOptionInput!]!) {
      productOptionsUpdate(productId: $productId, productOptions: $productOptions) {
        product { id options { name position } }
        userErrors { field message }
      }
    }
  `, { productId, productOptions });
}

async function restUpdateProductOptions(numericProductId, optionNames, existingOptions) {
  const payloadOptions = optionNames.map((o) => {
    const matched = existingOptions.find((e) => (e.position ?? 0) === (o.position ?? 0));
    return {
      id: matched?.id,
      name: o.name,
      position: o.position
    };
  });

  const r = await fetch(
    `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/products/${numericProductId}.json`,
    {
      method: "PUT",
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        product: {
          id: numericProductId,
          options: payloadOptions
        }
      })
    }
  );

  if (!r.ok) {
    const t = await r.text();
    throw new Error(`REST options update failed: ${r.status} ${t}`);
  }
  return r.json();
}

function gidToNumeric(gid) {
  // e.g. gid://shopify/ProductVariant/56073641656694 -> 56073641656694
  if (!gid) return null;
  const parts = String(gid).split("/");
  return parts[parts.length - 1];
}

async function restUpdateVariantOptions(variantGid, option1, option2, option3) {
  const variantId = gidToNumeric(variantGid);
  const payload = { variant: { id: variantId } };
  if (typeof option1 !== "undefined") payload.variant.option1 = option1;
  if (typeof option2 !== "undefined") payload.variant.option2 = option2;
  if (typeof option3 !== "undefined") payload.variant.option3 = option3;

  const r = await fetch(
    `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/variants/${variantId}.json`,
    {
      method: "PUT",
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    }
  );

  if (!r.ok) {
    const t = await r.text();
    throw new Error(`REST variant update failed: ${r.status} ${t}`);
  }
  return r.json();
}

// ---- Normalization helper (case/diacritics/spacing insensitive compare)
function normalizeForMatch(raw) {
  if (!raw) return "";
  return String(raw)
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase()
    // remove accents/diacritics
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    // unifying dashes
    .replace(/[‐‑–—]/g, "-");
}

// ---- Collections helpers (REST)
async function restFindCustomCollectionByTitle(title) {
  const want = normalizeForMatch(title);
  const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/custom_collections.json?limit=250`;
  const r = await fetchWithRetry(url, {
    headers: { "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN }
  });
  if (!r.ok) throw new Error(`REST get custom_collections failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  const list = Array.isArray(j.custom_collections) ? j.custom_collections : [];
  // try exact normalized match first
  let found = list.find(c => normalizeForMatch(c.title) === want);
  if (found) return found;
  // fallback: startsWith (useful when kolekcie majú prefix/sufix)
  found = list.find(c => normalizeForMatch(c.title).startsWith(want));
  return found || null;
}

async function restCreateCustomCollection(title) {
  const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/custom_collections.json`;
  const r = await fetchWithRetry(url, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ custom_collection: { title } })
  });
  if (!r.ok) throw new Error(`REST create custom_collection failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return j.custom_collection;
}

async function restEnsureCustomCollection(title) {
  let c = await restFindCustomCollectionByTitle(title);
  if (c) return c;
  return await restCreateCustomCollection(title);
}

// --- Helper: Get custom collection by ID, including image and title
async function restGetCustomCollection(collectionId) {
  const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/custom_collections/${collectionId}.json?fields=id,image,title`;
  const r = await fetchWithRetry(url, {
    headers: { "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN }
  });
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`REST get custom_collection failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return j.custom_collection || null;
}

async function restCollectExists(productId, collectionId) {
  const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/collects.json?product_id=${productId}&collection_id=${collectionId}&limit=1`;
  const r = await fetchWithRetry(url, {
    headers: { "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN }
  });
  if (!r.ok) throw new Error(`REST get collects failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return (j.collects?.length || 0) > 0;
}

async function restCreateCollect(productId, collectionId) {
  const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/collects.json`;
  await rateLimit();
  const r = await fetchWithRetry(url, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ collect: { product_id: productId, collection_id: collectionId } })
  });
  if (r.status === 422) {
    // Likely already collected; ignore to save one GET and avoid rate hits
    const t = await r.text();
    if (/already|exists|has already been taken/i.test(t)) {
      console.warn(`Collect already exists (skipping): product ${productId} -> collection ${collectionId}`);
      return null;
    }
  }
  if (!r.ok) throw new Error(`REST create collect failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return j.collect;
}

// --- Files helpers (REST)
async function restSearchFilesByFilename(filename) {
  // Search by exact filename within Shopify Files
  // Docs: GET /admin/api/{version}/files.json?query=filename:<name>
  const q = `filename:${filename}`;
  const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/files.json?query=${encodeURIComponent(q)}&limit=5`;
  const r = await fetchWithRetry(url, { headers: { "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN }});
  if (r.status === 404) {
    console.warn("Files search 404 Not Found for", filename);
    return [];
  }
  if (!r.ok) throw new Error(`REST files search failed: ${r.status} ${await r.text()}`);
  
  const j = await r.json();
  const list = Array.isArray(j.files) ? j.files : [];
  return list;
}


// Try a URL, then again with a cache-buster query (CDNs can 404 until purge)
async function httpOkWithBust(url) {
  if (await httpHeadOk(url)) return true;
  const bust = url.includes("?") ? `&cb=${Date.now()}` : `?cb=${Date.now()}`;
  return await httpHeadOk(url + bust);
}

// Helper: Check if a URL exists via HEAD request, rate-limited, with fallback to GET if HEAD fails
async function httpHeadOk(url) {
  try {
    await rateLimit();
    // Try HEAD first
    let res = await fetch(url, { method: "HEAD", redirect: "follow" });
    if (res.ok) return true;
    // Some CDNs (or raw.githubusercontent) can be picky about HEAD; try a quick GET without reading the body
    res = await fetch(url, { method: "GET", redirect: "follow" });
    return res.ok;
  } catch {
    return false;
  }
}

async function findImageUrlForNodeSlug(node_slug) {
  const exts = (process.env.COLLECTION_IMAGE_EXTS || "png,jpg,jpeg,webp")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const base = (process.env.COLLECTION_IMAGE_BASE || "https://cdn.jsdelivr.net/gh/automotoprodukty/shopify-autorewrite@main/collections%20img/").trim();
  console.log("IMG BASE:", base);
  // If base is raw.githubusercontent.com, also prepare a jsDelivr fallback
  let fallbackBase = "";
  let fallbackBase2 = "";
  try {
    if (base.includes("raw.githubusercontent.com")) {
      // Example raw: https://raw.githubusercontent.com/<owner>/<repo>/<branch>/collections%20img/
      // jsDelivr:      https://cdn.jsdelivr.net/gh/<owner>/<repo>@<branch>/collections%20img/
      const parts = base.split("raw.githubusercontent.com/")[1]; // "<owner>/<repo>/<branch>/path/"
      if (parts) {
        const seg = parts.split("/");
        const owner = seg[0], repo = seg[1], branch = seg[2];
        const pathRest = seg.slice(3).join("/");
        fallbackBase = `https://cdn.jsdelivr.net/gh/${owner}/${repo}@${branch}/${pathRest}`;
      }
    }
    if (base.includes("cdn.jsdelivr.net/gh/")) {
      // Example jsDelivr: https://cdn.jsdelivr.net/gh/<owner>/<repo>@<branch>/<path> 
      // Raw:              https://raw.githubusercontent.com/<owner>/<repo>/<branch>/<path>
      try {
        const afterGh = base.split("cdn.jsdelivr.net/gh/")[1]; // "<owner>/<repo>@<branch>/<path>"
        if (afterGh) {
          const [owner, rest] = afterGh.split("/");
          const [repo, afterRepo] = rest.split("@");
          const branch = afterRepo.split("/")[0];
          const pathRest = afterRepo.slice(branch.length + 1); // remove "<branch>/"
          fallbackBase2 = `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/${pathRest}`;
        }
      } catch {}
    }
  } catch {}

  // A) Prefer external GitHub/CDN base when provided
  if (base) {
    console.log("IMG BASE:", base, "slug=", node_slug);
    const basesToTry = Array.from(new Set([base, fallbackBase, fallbackBase2].filter(Boolean)));
    for (const b of basesToTry) {
      if (b !== base) console.log("IMG FALLBACK BASE:", b);
      if (node_slug) {
        for (const ext of exts) {
          const url = `${b}${node_slug}.${ext}`;
          console.log("IMG try:", url);
          if (await httpOkWithBust(url)) return url;
        }
      }
      // fallback to default image(s)
      const defName = process.env.COLLECTION_IMAGE_DEFAULT || "default.png";
      const defCandidates = [defName, ...exts.map((e) => `default.${e}`)];
      for (const n of defCandidates) {
        const url = `${b}${n}`;
        console.log("IMG try fallback:", url);
        if (await httpOkWithBust(url)) return url;
      }
    }
    console.warn("IMG: no match under external base(s), returning null");
    // when external base is set, do not try Shopify Files
    return null;
  }

  // B) Fallback to Shopify Files search when no external base is configured
  console.log("IMG: external base not set; falling back to Shopify Files", node_slug);
  const candidates = [];
  if (node_slug) {
    for (const ext of exts) candidates.push(`${node_slug}.${ext}`);
  }
  const defCandidates = [process.env.COLLECTION_IMAGE_DEFAULT || "default.png", ...exts.map((e) => `default.${e}`)];
  candidates.push(...defCandidates);

  for (const name of candidates) {
    const files = await restSearchFilesByFilename(name);
    const hit = files.find((f) => (f?.filename || "").toLowerCase() === name.toLowerCase());
    if (hit?.url) return hit.url; // Shopify CDN URL
  }
  return null;
}

// --- Metafields (REST) for Custom Collections
async function restUpsertCollectionMetafield(collectionId, namespace, key, type, value) {
  await rateLimit();
  const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/metafields.json`;
  const r = await fetchWithRetry(url, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      metafield: {
        namespace,
        key,
        type,
        value,
        owner_resource: "collection",
        owner_id: collectionId
      }
    })
  });
  if (!r.ok) {
    const t = await r.text();
    console.warn(`Metafield upsert failed for collection ${collectionId} ${namespace}.${key}: ${r.status} ${t}`);
  }
}

async function setCollectionTaxonomyFields(nodesEnsured) {
  // nodesEnsured: [{ id, title, node_slug, facets, level, parentId, childId }]
  for (const n of nodesEnsured) {
    await restUpsertCollectionMetafield(n.id, "taxonomy", "level", "number_integer", String(n.level));
    if (n.parentId) await restUpsertCollectionMetafield(n.id, "taxonomy", "parent", "number_integer", String(n.parentId));
    const childrenStr = n.childId ? JSON.stringify([n.childId]) : JSON.stringify([]);
    await restUpsertCollectionMetafield(n.id, "taxonomy", "children", "json", childrenStr);
    if (n.node_slug) await restUpsertCollectionMetafield(n.id, "taxonomy", "node_slug", "single_line_text_field", n.node_slug);
    if (Array.isArray(n.facets) && n.facets.length) {
      await restUpsertCollectionMetafield(n.id, "taxonomy", "facets", "list.single_line_text_field", JSON.stringify(n.facets));
    }
  }
}

// --- Custom Sub Collections metafield (list of collection references)
async function restSetSubCollections(parentId, childIds = []) {
  // writes to namespace 'custom', key 'sub_collections', type list.collection_reference
  await restUpsertCollectionMetafield(
    parentId,
    "custom",
    "sub_collections",
    "list.collection_reference",
    JSON.stringify(childIds.map(id => `gid://shopify/Collection/${id}`))
  );
}

// Link custom.sub_collections for a linear branch (each parent points to its direct child)
async function setCustomSubCollectionsForBranch(nodesEnsured) {
  for (let i = 0; i < nodesEnsured.length; i++) {
    const parent = nodesEnsured[i];
    const child = nodesEnsured[i + 1];
    const childIds = child ? [child.id] : [];
    await restSetSubCollections(parent.id, childIds);
  }
}

// Ensure whole branch (create missing), set images if absent, then write taxonomy metafields
async function ensureBranchAndTaxonomy(branchNodes) {
  const ensured = [];
  for (let i = 0; i < branchNodes.length; i++) {
    const node = branchNodes[i];
    const title = node.name || node.title;
    const coll = await restEnsureCustomCollection(title);

    // Assign image once (only if missing)
    try {
      const full = await restGetCustomCollection(coll.id);
      if (!full?.image?.src) {
        const fileUrl = await findImageUrlForNodeSlug(node.node_slug);
        if (fileUrl) {
          const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/custom_collections/${coll.id}.json`;
          await fetch(url, {
            method: "PUT",
            headers: {
              "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
              "Content-Type": "application/json"
            },
            body: JSON.stringify({ custom_collection: { id: coll.id, image: { src: fileUrl } } })
          });
          console.log("Collection image set:", title, "<=", fileUrl);
        } else {
          console.log("Collection image missing (no matching file):", title, "node_slug=", node.node_slug);
        }
      }
    } catch (e) {
      console.warn("Image assign failed for", title, e?.message || e);
    }

    ensured.push({
      id: coll.id,
      title,
      node_slug: node.node_slug,
      facets: node.facets,
      level: i,
      parentId: null, // to be filled next pass
      childId: null
    });
  }
  // link parents/children (linear branch)
  for (let i = 0; i < ensured.length; i++) {
    ensured[i].parentId = i > 0 ? ensured[i - 1].id : null;
    ensured[i].childId = i < ensured.length - 1 ? ensured[i + 1].id : null;
  }
  await setCollectionTaxonomyFields(ensured);
  await setCustomSubCollectionsForBranch(ensured);
  return ensured;
}

// ==== Collections lookup (read-only) =========================================

let __collectionsByTitle = new Map();
let __collectionsLoaded = false;

function normalizeTitleMatch(s) {
  return normalizeForMatch(s); // využijeme tvoju helper funkciu
}

function tryLoadCollectionsMapFromFile() {
  try {
    const file = path.join(process.cwd(), "collections-map.json");
    const data = fs.readFileSync(file, "utf8");
    const arr = JSON.parse(data);
    if (Array.isArray(arr)) {
      __collectionsByTitle.clear();
      for (const rec of arr) {
        if (!rec?.title || !rec?.id) continue;
        __collectionsByTitle.set(normalizeTitleMatch(rec.title), Number(rec.id));
      }
      __collectionsLoaded = true;
      console.log("Collections map loaded from file with", __collectionsByTitle.size, "items");
      return true;
    }
  } catch (e) {
    console.warn("Collections map file not loaded:", e?.message || e);
  }
  return false;
}

async function ensureCollectionsCache() {
  if (__collectionsLoaded) return;
  if (!tryLoadCollectionsMapFromFile()) {
    throw new Error("collections-map.json not available; this webhook runs in LOOKUP-ONLY mode and requires it.");
  }
}

async function resolveCollectionIdByTitle(title) {
  await ensureCollectionsCache();
  return __collectionsByTitle.get(normalizeTitleMatch(title)) || null;
}

// Z taxonómie zoberieme vetvu (root -> ... -> leaf) a pre každý NÁZOV nájdeme existujúce ID v mape
async function resolveExistingBranchIdsBySlug(brandName, nodeSlug) {
  const branchNodes = getBranchBySlug(brandName, nodeSlug); // [root,...,leaf] z taxonomie
  if (!branchNodes.length) return [];
  const out = [];
  for (const n of branchNodes) {
    const title = n.name || n.title;
    const cid = await resolveCollectionIdByTitle(title);
    if (!cid) {
      console.warn("Collection TITLE not found in collections-map.json:", title);
      continue; // pripneme aspoň to, čo existuje
    }
    out.push({ id: cid, title });
  }
  return out; // v poradí od root po leaf
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).send("Method not allowed");

    // --- HMAC overenie
    const secret = process.env.SHOPIFY_WEBHOOK_SECRET;
    const hmacHeader = req.headers["x-shopify-hmac-sha256"];
    const raw = await readRawBody(req);
    const digest = crypto.createHmac("sha256", secret).update(raw, "utf8").digest("base64");
    if (digest !== hmacHeader) return res.status(401).send("HMAC failed");

    const body = JSON.parse(raw);
    const topic = req.headers["x-shopify-topic"];
    const productGid = `gid://shopify/Product/${body.id}`;

    // --- Stiahni produkt
const p = await waitForProduct(productGid);
if (!p) {
  console.error("Product not ready from GraphQL after retries:", productGid);
  return res.status(202).send("Product not ready yet");
}

// Anti-loop (s bezpečným ?.)
const processed = p.metafields?.edges?.some(
  (e) => e.node.key === "processed" && e.node.value === "true"
);
    if (processed) return res.status(200).send("Already processed");

    // --- Priprav prompt pre OpenAI
    const prompt = `
NÁZOV_PÔVODNÝ: ${p.title}
POPIS_PÔVODNÝ(HTML alebo text): ${p.descriptionHtml}
ZNAČKA(Vendor): ${p.vendor}
EXISTUJÚCE_TAGY(čiarkou oddelené): ${p.tags.join(",")}
MOŽNOSTI(JSON): ${JSON.stringify(p.options)}
VARIANTY(JSON): ${JSON.stringify(p.variants.edges.map((e) => e.node))}
CIEĽ: Vráť JSON s kľúčmi:
- title (string)
- description (string)
- base_tags (array string)
- subtags (array string)
- extra_tags (array string)
- collections (array string)  // voliteľné, môže ostať prázdne
- options (array objektov { name, position?, values? }) // ak values prídu, ber ich ako celý nový zoznam
`;

    const out = await openAIRewrite(prompt);
    
    // --- DIAGNOSTICS: log AI collections & provide safe fallback ---
    console.log("AI collections =>", out?.collections);

    // --- Deterministic auto-refine of slug picks (no manual hints)
    function normalizeSimple(s){ 
      return String(s||"").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,""); 
    }
    function buildTextForMatch(p){
      return normalizeSimple([p.title, p.descriptionHtml, ...(p.tags||[])].join(" "));
    }
    // Deterministic keyword → slug map for the most obvious cases (kept tiny on purpose)
    const KEYWORD_TO_SLUG = {
      "volanty": ["volant", "steering wheel"],
      // add more high-signal pairs later if needed
    };
    // Try to infer a more specific leaf by matching product text with tokens derived from node_slug
    function autoRefineSlugPicks(slugs, brand, p, leavesWhitelist){
      const text = buildTextForMatch(p);
      const leaves = Array.isArray(leavesWhitelist) ? leavesWhitelist : getBrandLeaves(brand);

      // 1) Respect AI if it picked any non-generic slug(s)
      const norm = (s)=>normalizeSimple(s);
      const cleaned = (Array.isArray(slugs) ? slugs : [])
        .map(s => String(s||""))
        .filter(Boolean);
      const nonGeneric = cleaned.filter(s => norm(s) !== "ine");
      if (nonGeneric.length) {
        const seen = new Set();
        const unique = nonGeneric.filter(s => (seen.has(s) ? false : (seen.add(s), true)));
        return unique;
      }

      // 2) Try deterministic keyword → slug within whitelist
      const lowers = text;
      const whitelistSet = new Set(leaves.map(l => String(l.node_slug||"")));
      for (const [slug, kws] of Object.entries(KEYWORD_TO_SLUG)) {
        if (!whitelistSet.has(slug)) continue; // respect subtree
        if (kws.some(kw => lowers.includes(normalizeSimple(kw)))) {
          return [slug];
        }
      }

      // 3) Token scoring within whitelist (avoid generic noise)
      const STOP = new Set(["a","na","do","pre","pod","nad","pri","po","z","s","bez","auto","ine","ostatne","material","drobny","drobne","autopoistky","karoseria","ochrana"]);
      let bestSlug = null;
      let bestScore = 0;
      const wordBoundaryMatch = (token) => {
        try {
          const re = new RegExp(`\\b${token}\\w*\\b`, "g");
          const m = text.match(re);
          return m ? m.length : 0;
        } catch { return 0; }
      };

      for (const leaf of leaves){
        const slug = String(leaf.node_slug||"");
        if (!slug) continue;
        const parts = slug.split(/[-_ ]+/).map(normalizeSimple).filter(t => t && t.length >= 4 && !STOP.has(t));
        if (!parts.length) continue;
        let score = 0;
        for (const t of parts){
          score += wordBoundaryMatch(t);
        }
        if (score > bestScore){
          bestScore = score;
          bestSlug = slug;
        }
      }
      if (bestScore > 0 && bestSlug){
        return [bestSlug];
      }
      return cleaned; // give back whatever came (likely ["ine"]) so caller can decide
    }

    // --- Slug-only classification (branch whitelist)
    let slugPicks = [];
    const detectedBrand = detectBrandFromProduct(p, out);
    if (detectedBrand) {
      // Try to infer preferred top-level subtree from AI collections like "AUDI Interiér", "AUDI Exteriér", etc.
      let preferredArea = null; // e.g. "interier" | "exterier" | "komponenty" ...
      if (Array.isArray(out?.collections)) {
        const norm = (s)=>String(s||"").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"");
        for (const c of out.collections) {
          const s = norm(c);
          // look for patterns "<brand> <area>"
          if (s.includes(norm(detectedBrand))) {
            const parts = s.split(" ");
            const idx = parts.indexOf(norm(detectedBrand));
            const next = parts[idx+1] || ""; // e.g. "interiér"
            if (next) { preferredArea = next; break; }
          }
        }
      }
      let leaves;
      if (preferredArea) {
        const topNode = findBrandChildNode(detectedBrand, preferredArea);
        leaves = topNode ? getLeavesUnderNode(topNode) : getBrandLeaves(detectedBrand);
      } else {
        leaves = getBrandLeaves(detectedBrand);
      }
      const allowed = leaves.map(x => ({ slug: x.node_slug, label: x.path ? x.path.join(" → ") : (x.name||x.title) }))
                            .filter(x => x.slug);
      try {
        const cls = await aiPickCollectionSlugs({
          title: p.title,
          vendor: p.vendor,
          tags: p.tags,
          description: p.descriptionHtml,
          allowedLeaves: allowed,
          taxonomyTree: loadTaxonomia()
        });
        slugPicks = Array.isArray(cls?.collections_node_slugs) ? cls.collections_node_slugs.filter(Boolean) : [];
        console.log("AI slug picks =>", slugPicks);
        slugPicks = autoRefineSlugPicks(slugPicks, detectedBrand, p, leaves);
        console.log("Slug picks after auto-refine =>", slugPicks);
      } catch (e) {
        console.warn("AI slug-pick failed:", e?.message || e);
      }
    } else {
      console.warn("Brand not detected -> skipping slug-only classification");
    }

    // --- Guard: ak je produkt univerzálny alebo multi-brand, nespúšťaj značkové vetvy
    // (removed fallback logic: rely only on slug picks)

    // --- 1) Update základných polí + názvy optionov
    const tags = [
      ...(out.base_tags || []),
      ...(out.subtags || []),
      ...(out.extra_tags || [])
    ];

    const optionNames = (out.options || [])
      .filter((o) => o?.name)
      .map((o, i) => ({
        name: o.name,
        position: o.position ?? i + 1
      }));

    // 2a) základné polia bez options
    await productUpdate({
      id: p.id,
      title: out.title,
      descriptionHtml: formatDescription(out.description),
      tags
    });

    // 2b) názvy možností samostatne – REST verzia
    if (optionNames.length) {
      await restUpdateProductOptions(body.id, optionNames, p.options);
    }

    // --- 2) Ak prišli nové values, premapuj varianty podľa indexu
    const newValuesByPos = new Map();
    (out.options || []).forEach((o, i) => {
      if (o.values && Array.isArray(o.values)) {
        const pos = o.position ?? i + 1; // 1-based
        newValuesByPos.set(pos, o.values);
      }
    });

      // Pomocné mapy: názov optiony -> {idx, values}
      const byName = Object.fromEntries(
        p.options.map((opt, idx) => [opt.name, { idx, values: opt.values }])
      );
      const posByName = Object.fromEntries(p.options.map((opt, idx) => [opt.name, idx + 1])); // 1-based

      // Pre každý variant vypočítaj nové option1/2/3 a aktualizuj cez REST
      for (const { node } of p.variants.edges) {
        let newOpt1, newOpt2, newOpt3;

        for (const so of node.selectedOptions) {
          const pos = (posByName[so.name] || 0);
          const oldValues = byName[so.name]?.values || [];
          const newList = newValuesByPos.get(pos);

          // Ak nemáme nový zoznam pre danú pozíciu, ponechaj pôvodnú hodnotu
          let newVal = so.value;
          if (newList && Array.isArray(newList)) {
            const oldIndex = oldValues.indexOf(so.value);
            if (oldIndex >= 0 && typeof newList[oldIndex] !== "undefined") {
              newVal = newList[oldIndex];
            }
          }

          if (pos === 1) newOpt1 = newVal;
          if (pos === 2) newOpt2 = newVal;
          if (pos === 3) newOpt3 = newVal;
        }

        // Aktualizuj variant cez REST (s krátkym retry na 404)
        let ok = false;
        for (let i = 0; i < 3 && !ok; i++) {
            try {
                await restUpdateVariantOptions(node.id, newOpt1, newOpt2, newOpt3);
                ok = true;
            } catch (err) {
                // ak variant ešte „nie je“, chvíľu počkáme a skúsime znova
                if (String(err).includes("404")) {
                    await sleep(700);
                    continue;
                }
                throw err;
            }
        }
      }

    // --- 3) Collections (LOOKUP ONLY: attach leaf + parents, nič nevytvárame/neupravujeme)
    if (Array.isArray(slugPicks) && slugPicks.length && detectedBrand) {
      const productNumericId = body.id;

      if (!ENABLE_COLLECTIONS_ATTACH) {
        console.warn("Collections attach disabled by flag");
      } else {
        for (const slug of slugPicks) {
          const branch = await resolveExistingBranchIdsBySlug(detectedBrand, slug); // [{id,title}] root->leaf
          if (!branch.length) {
            console.warn("No existing collections resolved for", detectedBrand, slug);
            continue;
          }
          for (const node of branch) {
            if (DRY_RUN) {
              console.log("[DRY_RUN] Would attach product", productNumericId, "->", node.title, `(#${node.id})`);
            } else {
              try {
                const exists = await restCollectExists(productNumericId, node.id);
                if (!exists) await restCreateCollect(productNumericId, node.id);
              } catch (e) {
                console.warn("Attach failed", node, e?.message || e);
              }
            }
          }
        }
      }
    } else {
      console.warn("Collections: no slug picks or no brand -> skipping");
    }

    // --- 4) anti-loop
    await metafieldsSet(p.id);

    return res.status(200).send("OK");
  } catch (e) {
    console.error(e);
    return res.status(500).send("Error");
  }
}
