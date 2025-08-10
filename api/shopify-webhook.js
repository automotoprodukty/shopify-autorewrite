// Serverless endpoint pre Shopify webhook (Vercel).
// 1) Overí HMAC podpis
// 2) Dotiahne produkt z Admin GraphQL
// 3) Zavolá OpenAI (JSON výstup)
// 4) Upraví produkt (title, description, tags, option names)
// 5) Preloží hodnoty vo variantoch podľa nového zoznamu (mapovanie podľa indexu)
// 6) Zapíše metafield automation.processed=true

import crypto from "crypto";

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
          "Si asistent pre úpravu Shopify produktov. Dodrž: (1) krátky názov bez emoji, prídavné meno po pomlčke; (2) popis so štruktúrou Úvod/🚗 Výhody (✅)/📦 Špecifikácia (•)/🎯 Pre koho (•); (3) tagy: základné+subtagy+extra; (4) options: premenovať names; ak dodané values, sú to kompletné nové zoznamy; (5) nič nevymýšľaj mimo vstupu. Vráť len čistý JSON."
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

async function waitForProduct(id, attempts = 5, delayMs = 1200) {
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

    await productUpdate({
      id: p.id,
      title: out.title,
      descriptionHtml: out.description,
      tags,
      options: optionNames.length ? optionNames : undefined
    });

    // --- 2) Ak prišli nové values, premapuj varianty podľa indexu
    const newValuesByPos = new Map();
    (out.options || []).forEach((o, i) => {
      if (o.values && Array.isArray(o.values)) {
        const pos = o.position ?? i + 1; // 1-based
        newValuesByPos.set(pos, o.values);
      }
    });

    if (newValuesByPos.size > 0) {
      // Pomocné mapy: názov optiony -> {idx, values}
      const byName = Object.fromEntries(
        p.options.map((opt, idx) => [opt.name, { idx, values: opt.values }])
      );

      const bulk = p.variants.edges.map(({ node }) => {
        // pre každý selectedOption pozri, či máme novú sadu pre danú pozíciu
        const newOptions = node.selectedOptions.map((so) => {
          const pos = (byName[so.name]?.idx ?? 0) + 1; // 1-based
          const newList = newValuesByPos.get(pos);
          if (!newList) return so.value;

          const oldIndex = byName[so.name].values.indexOf(so.value);
          return newList[oldIndex] ?? so.value;
        });

        return { id: node.id, options: newOptions };
      });

      await variantsBulkUpdate(p.id, bulk);
    }

    // --- 3) (voliteľné) kolekcie – odporúčam riešiť cez automatické kolekcie na základe tagov

    // --- 4) anti-loop
    await metafieldsSet(p.id);

    return res.status(200).send("OK");
  } catch (e) {
    console.error(e);
    return res.status(500).send("Error");
  }
}
