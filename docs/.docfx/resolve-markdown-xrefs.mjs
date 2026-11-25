// Rewrites the <xref> tags DocFX's Markdown output leaves unresolved into Markdown links, and the
// HTML paragraph and list blocks it writes into Markdown, so the links inside them render.
// Run after `docfx docfx.json`, from the repository root:
//   node docs/.docfx/resolve-markdown-xrefs.mjs
// The HTML build's xrefmap.yml supplies each uid's page and display name. A uid outside this
// library links to Microsoft Learn when it is a System or Microsoft type, and is plain code otherwise.
import { readFileSync, readdirSync, writeFileSync, existsSync } from "node:fs";
import { join } from "node:path";

const markdownDir = "docs/api/markdown";
const xrefMapPath = "docs/api/docfx/xrefmap.yml";

const entries = new Map();
let current = null;
for (const line of readFileSync(xrefMapPath, "utf8").split(/\r?\n/)) {
  const uid = line.match(/^- uid: (.*)$/);
  if (uid) {
    current = { name: null, nameWithType: null, href: null };
    entries.set(uid[1], current);
    continue;
  }
  const field = line.match(/^  (name|nameWithType|href): (.*)$/);
  if (current && field) current[field[1]] = field[2];
}

const anchorsByFile = new Map();
function hasAnchor(file, anchor) {
  if (!anchorsByFile.has(file)) {
    const path = join(markdownDir, file);
    anchorsByFile.set(file, existsSync(path) ? readFileSync(path, "utf8") : "");
  }
  return anchorsByFile.get(file).includes(`id="${anchor}"`);
}

function learnUrl(uid) {
  const path = uid
    .replace(/\(.*$/, "")
    .replace(/\{.*\}/, "")
    .replace(/`/g, "-")
    .toLowerCase();
  return `https://learn.microsoft.com/dotnet/api/${path}`;
}

function resolve(uid) {
  const entry = entries.get(uid);
  if (entry && entry.href && !entry.href.startsWith("http")) {
    const [page, anchor] = entry.href.replace(/\.html(#|$)/, ".md$1").split("#");
    const text = entry.nameWithType ?? entry.name ?? uid;
    const target = anchor && hasAnchor(page, anchor) ? `${page}#${anchor}` : page;
    return `[\`${text}\`](${target})`;
  }
  const shortName = uid.replace(/\(.*$/, "").split(".").pop();
  if (/^(System|Microsoft)\./.test(uid)) {
    const text = entry?.name ?? shortName;
    return `[\`${text}\`](${learnUrl(uid)})`;
  }
  return `\`${entry?.name ?? shortName}\``;
}

// DocFX writes each <para> as a <p> line, a Markdown paragraph, and a </p> line, and each <list>
// as <ul><li> ... </li><li> ... </li></ul> lines. A line that opens one of those tags starts an
// HTML block, and Markdown renders an HTML block's contents as literal text, links included. This
// rewrites the paragraphs as blank-line separated Markdown, the lists as "- " items, and <b> as **.
const blockTags = new Map([
  ["<p>", { blank: true }],
  ["</p>", { blank: true }],
  ["<ul><li>", { blank: true, opensList: true, bullet: true }],
  ["</li><li>", { bullet: true }],
  ["</li></ul>", { blank: true, closesList: true }],
]);
let blocksRewritten = 0;
function rewriteHtmlBlocks(text) {
  const out = [];
  let inList = false;
  let bullet = false;
  let insertedBlank = false;
  const lastIsBlank = () => out.length === 0 || out[out.length - 1].replace(/\r$/, "") === "";
  const lines = text.split("\n");
  // The element after the final newline is the end of the file, not a line, so it passes through.
  const end = lines.pop();
  for (const raw of lines) {
    const eol = raw.endsWith("\r") ? "\r" : "";
    const line = raw.replace(/\r$/, "");
    const tag = blockTags.get(line);
    if (tag) {
      blocksRewritten++;
      if (tag.blank && !lastIsBlank()) {
        out.push(eol);
        insertedBlank = true;
      }
      if (tag.opensList) inList = true;
      if (tag.closesList) inList = false;
      if (tag.bullet) bullet = true;
      continue;
    }
    if (line === "" && insertedBlank && lastIsBlank()) continue;
    insertedBlank = false;
    let rewrittenLine = line.replace(/<b>(.*?)<\/b>/g, "**$1**");
    if (inList && line !== "") {
      rewrittenLine = (bullet ? "- " : "  ") + rewrittenLine;
      bullet = false;
    }
    out.push(rewrittenLine + eol);
  }
  out.push(end);
  return out.join("\n");
}

const xrefTag = /<xref href="([^"]*)"[^>]*><\/xref>/g;
let rewritten = 0;
for (const file of readdirSync(markdownDir).filter((name) => name.endsWith(".md"))) {
  const path = join(markdownDir, file);
  const text = readFileSync(path, "utf8");
  const resolved = rewriteHtmlBlocks(
    text.replace(xrefTag, (_, href) => {
      rewritten++;
      return resolve(decodeURIComponent(href));
    }),
  );
  if (resolved !== text) writeFileSync(path, resolved);
}

// The HTML site's landing page, as the Markdown set's README.
const landing = readFileSync("docs/.docfx/index.md", "utf8").replace(/<xref:([^>]+)>/g, (_, uid) => {
  rewritten++;
  return resolve(uid);
});
writeFileSync(join(markdownDir, "README.md"), landing);

const left = readdirSync(markdownDir)
  .filter((name) => name.endsWith(".md"))
  .reduce((count, name) => count + (readFileSync(join(markdownDir, name), "utf8").match(/<xref[ :]/g)?.length ?? 0), 0);
const blocksLeft = readdirSync(markdownDir)
  .filter((name) => name.endsWith(".md"))
  .reduce((count, name) => count + (readFileSync(join(markdownDir, name), "utf8").match(/^<\/?(p|ul|li)>/gm)?.length ?? 0), 0);
console.log(`Rewrote ${rewritten} xref tags; ${left} remain.`);
console.log(`Rewrote ${blocksRewritten} HTML block lines; ${blocksLeft} remain.`);
if (left > 0 || blocksLeft > 0) process.exitCode = 1;
