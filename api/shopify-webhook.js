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

    // 2a) základné polia bez options
    await productUpdate({
      id: p.id,
      title: out.title,
      descriptionHtml: out.description,
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

    // --- 3) (voliteľné) kolekcie – odporúčam riešiť cez automatické kolekcie na základe tagov

    // --- 4) anti-loop
    await metafieldsSet(p.id);

    return res.status(200).send("OK");
  } catch (e) {
    console.error(e);
    return res.status(500).send("Error");
  }
}
