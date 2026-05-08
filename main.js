import fs from "node:fs/promises";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

const DOWNLOAD_DIR = "./brasoes";
const METADATA_PATH = path.join(DOWNLOAD_DIR, "index.json");
const DOWNLOAD_STATE_PATH = path.join(DOWNLOAD_DIR, "download-state.json");

const QUERY_TIMEOUT_MS = 90000;
const QUERY_RETRY_BASE_DELAY_MS = 5000;
const QUERY_RETRY_MAX_DELAY_MS = 60000;
const MAX_QUERY_ATTEMPTS = 5;
const DOWNLOAD_TIMEOUT_MS = 90000;
const DOWNLOAD_DELAY_MS = 2000;
const DOWNLOAD_RETRY_BASE_DELAY_MS = 5000;
const DOWNLOAD_RETRY_MAX_DELAY_MS = 60000;
const MAX_DOWNLOAD_ATTEMPTS = 5;

let sharp;

try {
  ({ default: sharp } = await import("sharp"));
} catch (err) {
  console.log("[ERRO] Dependência sharp não encontrada");
  console.log("Instale as dependências com: npm install");
  console.log(err);
  process.exit(1);
}

console.log("====================================");
console.log(" BRASOES BRASIL DOWNLOADER");
console.log("====================================");
console.log("");

const ufByIbgePrefix = {
  11: "RO",
  12: "AC",
  13: "AM",
  14: "RR",
  15: "PA",
  16: "AP",
  17: "TO",
  21: "MA",
  22: "PI",
  23: "CE",
  24: "RN",
  25: "PB",
  26: "PE",
  27: "AL",
  28: "SE",
  29: "BA",
  31: "MG",
  32: "ES",
  33: "RJ",
  35: "SP",
  41: "PR",
  42: "SC",
  43: "RS",
  50: "MS",
  51: "MT",
  52: "GO",
  53: "DF",
};

function getUf(ibge) {
  return ufByIbgePrefix[String(ibge).slice(0, 2)] || "XX";
}

function slugify(text) {
  return text
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\w\s-]/g, "")
    .trim()
    .replace(/\s+/g, "_");
}

function safeDecodeURIComponent(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function getExtension(url, contentType) {
  const lower = safeDecodeURIComponent(url).toLowerCase();
  const lowerContentType = contentType.toLowerCase();

  if (lower.includes(".svg") || lowerContentType.includes("svg")) {
    return "svg";
  }

  if (lower.includes(".png") || lowerContentType.includes("png")) {
    return "png";
  }

  if (
    lower.includes(".jpg") ||
    lower.includes(".jpeg") ||
    lowerContentType.includes("jpeg")
  ) {
    return "jpg";
  }

  if (lowerContentType.includes("webp")) {
    return "webp";
  }

  return "img";
}

function getStateKey(ibge) {
  return String(ibge);
}

function getTimestamp() {
  return new Date().toISOString();
}

function isRetryableStatus(status) {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

function parseRetryAfterMs(value) {
  if (!value) {
    return null;
  }

  const seconds = Number(value);

  if (Number.isFinite(seconds)) {
    return Math.max(0, seconds * 1000);
  }

  const retryDate = Date.parse(value);

  if (Number.isNaN(retryDate)) {
    return null;
  }

  return Math.max(0, retryDate - Date.now());
}

function getBackoffDelayMs(attempt, baseDelayMs, maxDelayMs, retryAfterMs) {
  const exponentialDelayMs = Math.min(
    maxDelayMs,
    baseDelayMs * 2 ** (attempt - 1)
  );

  if (retryAfterMs !== null) {
    return Math.min(maxDelayMs, Math.max(exponentialDelayMs, retryAfterMs));
  }

  return exponentialDelayMs;
}

async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchWithRetry(
  url,
  options = {},
  {
    timeoutMs = DOWNLOAD_TIMEOUT_MS,
    retryBaseDelayMs = DOWNLOAD_RETRY_BASE_DELAY_MS,
    retryMaxDelayMs = DOWNLOAD_RETRY_MAX_DELAY_MS,
    maxAttempts = MAX_DOWNLOAD_ATTEMPTS,
    label = "request",
    readBody = null,
  } = {}
) {
  let lastResponse = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    let retryAfterMs = null;

    try {
      const response = await fetchWithTimeout(url, options, timeoutMs);
      response.fetchAttempts = attempt;
      lastResponse = response;

      if (response.ok) {
        if (readBody) {
          response.fetchBody = await readBody(response);
        }

        return response;
      }

      console.log(
        `[WARN] ${label} retornou HTTP ${response.status} na tentativa ${attempt}/${maxAttempts}`
      );

      if (!isRetryableStatus(response.status) || attempt === maxAttempts) {
        return response;
      }

      retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"));
      await response.arrayBuffer().catch(() => {});
    } catch (err) {
      console.log(
        `[WARN] Falha em ${label} na tentativa ${attempt}/${maxAttempts}: ${err.message}`
      );

      if (attempt === maxAttempts) {
        throw err;
      }
    }

    if (attempt < maxAttempts) {
      const delayMs = getBackoffDelayMs(
        attempt,
        retryBaseDelayMs,
        retryMaxDelayMs,
        retryAfterMs
      );

      console.log(
        `[INFO] Aguardando ${(delayMs / 1000).toFixed(1)}s para retry com backoff`
      );
      await sleep(delayMs);
    }
  }

  return lastResponse;
}

async function readJsonFile(filepath, fallback) {
  try {
    return JSON.parse(await fs.readFile(filepath, "utf8"));
  } catch (err) {
    if (err.code === "ENOENT") {
      return fallback;
    }

    throw err;
  }
}

async function writeJsonFile(filepath, data) {
  const tmpPath = `${filepath}.tmp`;

  await fs.writeFile(tmpPath, JSON.stringify(data, null, 2));
  await fs.rename(tmpPath, filepath);
}

async function loadDownloadState() {
  const state = await readJsonFile(DOWNLOAD_STATE_PATH, {
    version: 1,
    updatedAt: null,
    downloads: {},
  });

  return {
    version: state.version || 1,
    updatedAt: state.updatedAt || null,
    downloads: state.downloads || {},
  };
}

async function saveDownloadState(state) {
  state.updatedAt = getTimestamp();
  await writeJsonFile(DOWNLOAD_STATE_PATH, state);
}

async function setDownloadStateRecord(state, record) {
  state.downloads[getStateKey(record.COD_IBGE)] = record;
  await saveDownloadState(state);
}

function buildStateRecord({
  ibge,
  uf,
  municipio,
  status,
  filename = null,
  contentType = null,
  sourceContentType = null,
  url = null,
  sourceUrl = null,
  error = null,
  attempts = null,
}) {
  return {
    COD_IBGE: String(ibge),
    UF: uf,
    MUNICIPIO: municipio,
    status,
    filename,
    contentType,
    sourceContentType,
    url,
    sourceUrl,
    error,
    attempts,
    updatedAt: getTimestamp(),
  };
}

function stateRecordToMetadata(record) {
  return {
    ibge: record.COD_IBGE,
    uf: record.UF,
    nome: record.MUNICIPIO,
    filename: record.filename,
    contentType: record.contentType,
    sourceContentType: record.sourceContentType,
    url: record.url,
    sourceUrl: record.sourceUrl,
  };
}

function extractIbgeFromFilename(filename) {
  const match = filename.match(/_(\d{7})\.[^.]+$/);
  return match?.[1] || null;
}

async function fileExists(filepath) {
  try {
    const stats = await fs.stat(filepath);
    return stats.isFile();
  } catch (err) {
    if (err.code === "ENOENT") {
      return false;
    }

    throw err;
  }
}

async function indexExistingFiles() {
  const existingPngByIbge = new Map();
  const existingSourceByIbge = new Map();

  let filenames = [];

  try {
    filenames = await fs.readdir(DOWNLOAD_DIR);
  } catch (err) {
    if (err.code !== "ENOENT") {
      throw err;
    }
  }

  for (const filename of filenames) {
    const ext = path.extname(filename).toLowerCase();
    const ibge = extractIbgeFromFilename(filename);

    if (!ibge) {
      continue;
    }

    if (ext === ".png") {
      existingPngByIbge.set(ibge, filename);
      continue;
    }

    if ([".svg", ".jpg", ".jpeg", ".webp"].includes(ext)) {
      existingSourceByIbge.set(ibge, filename);
    }
  }

  return {
    existingPngByIbge,
    existingSourceByIbge,
  };
}

async function convertToPng(buffer) {
  return sharp(buffer, {
    limitInputPixels: false,
  })
    .png()
    .toBuffer();
}

function withWikimediaPngRenderParams(imageUrl) {
  const url = new URL(imageUrl);
  url.searchParams.set("width", "1024");
  return url.toString();
}

console.log("[1/7] Criando pasta ./brasoes");

await fs.mkdir(DOWNLOAD_DIR, {
  recursive: true,
});

console.log("[OK] Pasta criada");
console.log("");

console.log("[2/7] Montando query SPARQL");

const query = `
SELECT ?municipio ?municipioLabel ?ibge ?brasao WHERE {
  ?municipio wdt:P1585 ?ibge.
  ?municipio wdt:P94 ?brasao.

  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "pt,en".
  }
}
`;

console.log("[OK] Query criada");
console.log("");

console.log("[3/7] Preparando request Wikidata");

const endpoint =
  "https://query.wikidata.org/sparql?format=json&query=" +
  encodeURIComponent(query);

console.log("Endpoint:");
console.log(endpoint);
console.log("");

console.log("[4/7] Consultando Wikidata");

let response;

try {
  response = await fetchWithRetry(
    endpoint,
    {
      headers: {
        "User-Agent": "brasoes-brasil/1.0",
        Accept: "application/json",
      },
    },
    {
      timeoutMs: QUERY_TIMEOUT_MS,
      retryBaseDelayMs: QUERY_RETRY_BASE_DELAY_MS,
      retryMaxDelayMs: QUERY_RETRY_MAX_DELAY_MS,
      maxAttempts: MAX_QUERY_ATTEMPTS,
      label: "consulta Wikidata",
      readBody: (response) => response.json(),
    }
  );
} catch (err) {
  console.log("");
  console.log("[ERRO] Falha na consulta Wikidata");
  console.log(err);
  process.exit(1);
}

console.log("[OK] Resposta recebida");
console.log("HTTP STATUS:", response.status);
console.log("Tentativas:", response.fetchAttempts);
console.log("");

if (!response.ok) {
  console.log("[ERRO] Wikidata retornou erro HTTP");
  process.exit(1);
}

console.log("[5/7] Convertendo JSON");

const data = response.fetchBody;

console.log("[OK] JSON convertido");
console.log("");

const rows = data.results.bindings;

console.log("Total de registros encontrados:", rows.length);
console.log("");

if (!rows.length) {
  console.log("[ERRO] Nenhum resultado encontrado");
  process.exit(1);
}

console.log("[6/7] Iniciando downloads");
console.log("");

let success = 0;
let skipped = 0;
let converted = 0;
let failed = 0;

const metadata = [];
const downloadState = await loadDownloadState();
const { existingPngByIbge, existingSourceByIbge } = await indexExistingFiles();

for (let i = 0; i < rows.length; i++) {
  const row = rows[i];

  console.log("--------------------------------------------------");
  console.log(`[${i + 1}/${rows.length}]`);

  try {
    const ibge = row.ibge?.value;
    const nome = row.municipioLabel?.value;
    const brasao = row.brasao?.value;

    console.log("Município:", nome);
    console.log("IBGE:", ibge);

    const uf = getUf(ibge);

    console.log("UF:", uf);

    const safeName = slugify(nome);

    console.log("Slug:", safeName);

    const filename = `${uf}_${safeName}_${ibge}.png`;
    const filepath = path.join(DOWNLOAD_DIR, filename);
    const stateKey = getStateKey(ibge);

    const markFailure = async (error, extra = {}) => {
      await setDownloadStateRecord(
        downloadState,
        buildStateRecord({
          ibge,
          uf,
          municipio: nome,
          status: "failed",
          filename,
          error,
          attempts: MAX_DOWNLOAD_ATTEMPTS,
          ...extra,
        })
      );

      failed++;
    };

    if (!brasao) {
      console.log("[SKIP] Sem brasão");
      await markFailure("Sem brasão");
      continue;
    }

    let imageUrl = brasao.replace(
      "http://commons.wikimedia.org/wiki/Special:FilePath/",
      "https://commons.wikimedia.org/wiki/Special:FilePath/"
    );

    console.log("URL original:");
    console.log(imageUrl);

    const existingStateRecord = downloadState.downloads[stateKey];
    const stateFilename = existingStateRecord?.filename;
    const stateFilepath = stateFilename
      ? path.join(DOWNLOAD_DIR, stateFilename)
      : filepath;

    if (
      existingStateRecord?.status === "downloaded" &&
      stateFilename?.toLowerCase().endsWith(".png") &&
      (await fileExists(stateFilepath))
    ) {
      console.log("[SKIP] Já baixado no estado:", stateFilename);
      metadata.push(stateRecordToMetadata(existingStateRecord));
      skipped++;
      continue;
    }

    const existingPngFilename = existingPngByIbge.get(String(ibge));

    if (existingPngFilename) {
      console.log("[SKIP] PNG já existe:", existingPngFilename);

      const record = buildStateRecord({
        ibge,
        uf,
        municipio: nome,
        status: "downloaded",
        filename: existingPngFilename,
        contentType: "image/png",
        url: imageUrl,
        sourceUrl: imageUrl,
      });

      await setDownloadStateRecord(downloadState, record);
      metadata.push(stateRecordToMetadata(record));
      skipped++;
      continue;
    }

    const existingSourceFilename = existingSourceByIbge.get(String(ibge));

    if (existingSourceFilename) {
      console.log("[INFO] Convertendo arquivo existente:", existingSourceFilename);

      try {
        const sourceFilepath = path.join(DOWNLOAD_DIR, existingSourceFilename);
        const sourceBuffer = await fs.readFile(sourceFilepath);
        const pngBuffer = await convertToPng(sourceBuffer);

        if (pngBuffer.length < 100) {
          throw new Error("PNG convertido ficou muito pequeno");
        }

        await fs.writeFile(filepath, pngBuffer);

        const record = buildStateRecord({
          ibge,
          uf,
          municipio: nome,
          status: "downloaded",
          filename,
          contentType: "image/png",
          sourceContentType: `local/${path
            .extname(existingSourceFilename)
            .slice(1)}`,
          url: imageUrl,
          sourceUrl: imageUrl,
        });

        await setDownloadStateRecord(downloadState, record);
        metadata.push(stateRecordToMetadata(record));

        console.log("[OK] PNG gerado a partir do arquivo existente");
        converted++;
        continue;
      } catch (err) {
        console.log("[WARN] Conversão local falhou, tentando baixar novamente");
        console.log(err);
      }
    }

    console.log(`[INFO] Aguardando ${DOWNLOAD_DELAY_MS / 1000}s antes do download`);
    await sleep(DOWNLOAD_DELAY_MS);

    let img = await fetchWithRetry(
      imageUrl,
      {
        headers: {
          "User-Agent": "brasoes-brasil/1.0",
        },
        redirect: "follow",
      },
      {
        label: "download imagem",
        readBody: async (response) => Buffer.from(await response.arrayBuffer()),
      }
    );
    let downloadAttempts = img.fetchAttempts || null;

    console.log("HTTP imagem:", img.status);

    if (!img.ok) {
      console.log("[ERRO] Download falhou");
      await markFailure(`Download falhou com HTTP ${img.status}`, {
        attempts: downloadAttempts,
        url: imageUrl,
        sourceUrl: imageUrl,
      });
      continue;
    }

    const contentType = img.headers.get("content-type") || "";

    console.log("Content-Type:", contentType);

    if (!contentType.toLowerCase().startsWith("image/")) {
      console.log("[ERRO] Não é imagem");
      await markFailure("Resposta não é imagem", {
        attempts: downloadAttempts,
        contentType: "image/png",
        sourceContentType: contentType,
        url: imageUrl,
        sourceUrl: img.url || imageUrl,
      });
      continue;
    }

    const responseUrl = img.url || imageUrl;
    const ext = getExtension(responseUrl, contentType);

    console.log("Extensão detectada:", ext);

    let buffer = img.fetchBody;
    let sourceUrl = responseUrl;
    let sourceContentType = contentType;

    console.log("Tamanho:", buffer.length, "bytes");

    if (buffer.length < 100) {
      console.log("[ERRO] Arquivo muito pequeno");
      await markFailure("Arquivo muito pequeno", {
        attempts: downloadAttempts,
        contentType: "image/png",
        sourceContentType,
        url: imageUrl,
        sourceUrl,
      });
      continue;
    }

    console.log("Convertendo para PNG...");

    let pngBuffer;

    try {
      pngBuffer = await convertToPng(buffer);
    } catch (err) {
      if (ext !== "svg") {
        throw err;
      }

      const pngRenderUrl = withWikimediaPngRenderParams(imageUrl);

      console.log("[WARN] Conversão SVG local falhou, usando renderização PNG:");
      console.log(err);
      console.log(pngRenderUrl);

      await sleep(DOWNLOAD_DELAY_MS);

      img = await fetchWithRetry(
        pngRenderUrl,
        {
          headers: {
            "User-Agent": "brasoes-brasil/1.0",
          },
          redirect: "follow",
        },
        {
          label: "renderização PNG",
          readBody: async (response) =>
            Buffer.from(await response.arrayBuffer()),
        }
      );
      downloadAttempts = (downloadAttempts || 0) + (img.fetchAttempts || 0);

      console.log("HTTP render PNG:", img.status);

      if (!img.ok) {
        console.log("[ERRO] Download da renderização PNG falhou");
        await markFailure(`Renderização PNG falhou com HTTP ${img.status}`, {
          attempts: downloadAttempts,
          contentType: "image/png",
          sourceContentType,
          url: imageUrl,
          sourceUrl: pngRenderUrl,
        });
        continue;
      }

      sourceContentType = img.headers.get("content-type") || "";
      sourceUrl = pngRenderUrl;

      console.log("Content-Type render PNG:", sourceContentType);

      if (!sourceContentType.toLowerCase().startsWith("image/")) {
        console.log("[ERRO] Renderização não é imagem");
        await markFailure("Renderização não é imagem", {
          attempts: downloadAttempts,
          contentType: "image/png",
          sourceContentType,
          url: imageUrl,
          sourceUrl,
        });
        continue;
      }

      buffer = img.fetchBody;

      console.log("Tamanho render PNG:", buffer.length, "bytes");

      if (buffer.length < 100) {
        console.log("[ERRO] Renderização PNG muito pequena");
        await markFailure("Renderização PNG muito pequena", {
          attempts: downloadAttempts,
          contentType: "image/png",
          sourceContentType,
          url: imageUrl,
          sourceUrl,
        });
        continue;
      }

      pngBuffer = await convertToPng(buffer);
    }

    console.log("Tamanho PNG:", pngBuffer.length, "bytes");

    if (pngBuffer.length < 100) {
      console.log("[ERRO] PNG muito pequeno");
      await markFailure("PNG muito pequeno", {
        attempts: downloadAttempts,
        contentType: "image/png",
        sourceContentType,
        url: imageUrl,
        sourceUrl,
      });
      continue;
    }

    console.log("Arquivo final:", filename);
    console.log("Caminho final:", filepath);
    console.log("Salvando arquivo...");

    await fs.writeFile(filepath, pngBuffer);

    console.log("[OK] Arquivo salvo");

    const record = buildStateRecord({
      ibge,
      uf,
      municipio: nome,
      status: "downloaded",
      filename,
      contentType: "image/png",
      sourceContentType,
      url: imageUrl,
      sourceUrl,
      attempts: downloadAttempts,
    });

    await setDownloadStateRecord(downloadState, record);
    metadata.push(stateRecordToMetadata(record));

    success++;
  } catch (err) {
    console.log("[ERRO FATAL]");
    console.log(err);

    const ibge = row.ibge?.value;
    const nome = row.municipioLabel?.value;
    const uf = ibge ? getUf(ibge) : "XX";

    if (ibge) {
      await setDownloadStateRecord(
        downloadState,
        buildStateRecord({
          ibge,
          uf,
          municipio: nome,
          status: "failed",
          error: err.message,
          attempts: MAX_DOWNLOAD_ATTEMPTS,
        })
      );
    }

    failed++;
  }

  console.log("");
}

console.log("--------------------------------------------------");
console.log("");
console.log("[7/7] Salvando metadata");

await fs.writeFile(
  METADATA_PATH,
  JSON.stringify(metadata, null, 2)
);

console.log("[OK] Metadata salva");
console.log("");

console.log("====================================");
console.log(" FINALIZADO");
console.log("====================================");
console.log("Sucesso:", success);
console.log("Convertidos:", converted);
console.log("Ignorados:", skipped);
console.log("Falhas:", failed);
console.log("Metadata:", METADATA_PATH);
console.log("Estado:", DOWNLOAD_STATE_PATH);
console.log("Pasta:", DOWNLOAD_DIR);
console.log("====================================");
