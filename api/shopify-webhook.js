// Serverless endpoint pre Shopify webhook (Vercel).
// 1) Overí HMAC podpis
// 2) Dotiahne produkt z Admin GraphQL
// 3) Zavolá OpenAI (JSON výstup)
// 4) Upraví produkt (title, description, tags, option names)
// 5) Preloží hodnoty vo variantoch podľa nového zoznamu (mapovanie podľa indexu)
// 6) Zapíše metafield automation.processed=true

import crypto from "crypto";
import fs from "fs";
import path from "path";
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
  const r = await fetch(url, {
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
  const r = await fetch(url, {
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
  const r = await fetch(url, {
    headers: { "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN }
  });
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`REST get custom_collection failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return j.custom_collection || null;
}

async function restCollectExists(productId, collectionId) {
  const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/collects.json?product_id=${productId}&collection_id=${collectionId}&limit=1`;
  const r = await fetch(url, {
    headers: { "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN }
  });
  if (!r.ok) throw new Error(`REST get collects failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return (j.collects?.length || 0) > 0;
}

async function restCreateCollect(productId, collectionId) {
  const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/collects.json`;
  const r = await fetch(url, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ collect: { product_id: productId, collection_id: collectionId } })
  });
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
  const r = await fetch(url, { headers: { "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN }});
  if (!r.ok) throw new Error(`REST files search failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  const list = Array.isArray(j.files) ? j.files : [];
  return list;
}

async function findImageUrlForNodeSlug(node_slug) {
  const candidates = [];
  if (node_slug) {
    candidates.push(`${node_slug}.png`, `${node_slug}.jpg`, `${node_slug}.jpeg`, `${node_slug}.webp`);
  }
  candidates.push("default.png", "default.jpg", "default.jpeg", "default.webp");
  for (const name of candidates) {
    const files = await restSearchFilesByFilename(name);
    const hit = files.find(f => (f?.filename || "").toLowerCase() === name.toLowerCase());
    if (hit?.url) return hit.url; // CDN URL
  }
  return null;
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

    // Simple tag-based fallback helper (brand + základné kľúčové slová)
    function deriveLeafFromTags(tags = []) {
      const t = (Array.isArray(tags) ? tags : [])
        .map(x => String(x || "").toLowerCase());

      const brands = ["audi","bmw","mercedes-benz","mercedes","škoda","skoda","volkswagen","vw"];
      const brand = brands.find(b => t.includes(b));
      if (!brand) return null;

      // Normalizácia brandu na zápis v taxonómii
      let B = brand.toUpperCase();
      if (B === "SKODA") B = "ŠKODA";
      if (B === "VW") B = "VOLKSWAGEN";
      if (B === "MERCEDES") B = "MERCEDES-BENZ";

      // Heuristiky na pár najčastejších uzlov
      if (t.some(x => /platn(i|í)cky/.test(x))) return `${B} Brzdové platničky`;
      if (t.some(x => /kot(u|ú)c/.test(x)))    return `${B} Brzdový kotúč`;
      if (t.includes("brzdy"))                 return `${B} Brzdy`;
      if (t.includes("pneumatiky"))            return `${B} Pneumatiky`;
      if (t.includes("olej"))                  return `${B} Olej`;

      return `${B} Komponenty`;
    }

    if (!Array.isArray(out?.collections) || out.collections.length === 0) {
      // 1) Skús odvodiť z tagov v webhook payload-e
      const tagsFromBody = body?.tags || body?.product?.tags || [];
      const guess = deriveLeafFromTags(tagsFromBody);
      if (guess) {
        out.collections = [guess];
        console.warn("AI returned no collections -> using TAG fallback:", out.collections);
      } else {
        // 2) Dočasný fallback pre testovanie (zmaž po overení)
        out.collections = ["AUDI Brzdové platničky"];
        console.warn("AI returned no collections -> using TEMP fallback:", out.collections);
      }
    }

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

    // --- 3) Collections (taxonomy logic: ensure all ancestors, create if missing, assign images, ensure collects) ---
    if (Array.isArray(out.collections) && out.collections.length) {
      const productNumericId = body.id;
      for (const leafColTitle of out.collections) {
        const leafTitle = String(leafColTitle).trim();
        if (!leafTitle) continue;

        // Diagnostics: what leaf we were asked to process
        console.log("COLL: requested leaf =", leafTitle);

        // Get taxonomy branch (all ancestors to root) as nodes
        const branchNodes = getTaxonomyBranchNodesFromLeaf(leafTitle);
        console.log("TAXO BRANCH =>", leafTitle, "=>", branchNodes.map(n => n.name));

        // If taxonomy doesn't contain this leaf, skip gracefully (prevents creating wrong collections)
        if (!Array.isArray(branchNodes) || branchNodes.length === 0) {
          console.warn("TAXO: no branch found in taxonomia.json for:", leafTitle, "-> skip ensure/attach");
          continue;
        }

        for (const node of branchNodes) {
          const collTitle = node.name;
          if (!collTitle) continue;
          try {
            // Ensure collection exists (create if missing)
            const coll = await restEnsureCustomCollection(collTitle);
            // --- Assign image to collection (stricter: only if not already set) ---
            try {
              // Fetch latest collection data (with image)
              const full = await restGetCustomCollection(coll.id);
              if (!full?.image?.src) {
                if (node.node_slug) {
                  const fileUrl = await findImageUrlForNodeSlug(node.node_slug);
                  if (fileUrl) {
                    const url = `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/${process.env.SHOPIFY_API_VERSION || "2024-04"}/custom_collections/${coll.id}.json`;
                    await fetch(url, {
                      method: "PUT",
                      headers: {
                        "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
                        "Content-Type": "application/json"
                      },
                      body: JSON.stringify({
                        custom_collection: {
                          id: coll.id,
                          image: { src: fileUrl }
                        }
                      })
                    });
                  }
                }
              }
              // else: image is already set, skip
            } catch (imgErr) {
              // Ignore image errors, continue
              //console.warn("Collection image assign error", collTitle, imgErr?.message || imgErr);
            }
            // Ensure product is in collection (collect)
            const exists = await restCollectExists(productNumericId, coll.id);
            if (!exists) {
              await restCreateCollect(productNumericId, coll.id);
            }
          } catch (err) {
            console.error("Collection ensure/attach error:", collTitle, err?.message || err);
            // continue to next
          }
        }
      }
    }

    // --- 4) anti-loop
    await metafieldsSet(p.id);

    return res.status(200).send("OK");
  } catch (e) {
    console.error(e);
    return res.status(500).send("Error");
  }
}
