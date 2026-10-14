// Outreach composer shared by Influencers, Wave builder and Waves.
// Outreach.open(creators, {onDone}) — creators: [{key, platform, handle, name, avatar, user_id, followers, paid_status, post_urls}]
// Writes a DM (team templates with {first_name} / {name} / {handle}), opens x.com with the text prefilled
// (you press send there, logged into the account you picked), and logs who it went to and from which of our accounts.
(function () {
  const css = `
.ox-ov { position: fixed; inset: 0; background: rgba(0,0,0,.35); display: none; align-items: center; justify-content: center; z-index: 80; padding: 16px; }
.ox-ov.open { display: flex; }
.ox { border: 1px solid var(--border); border-radius: 12px; background: var(--surface); color: var(--text); padding: 18px;
  width: min(640px, 100%); max-height: calc(100vh - 32px); overflow: auto; box-shadow: 0 12px 40px rgba(0,0,0,.25); font-size: 14px; }
.ox h3 { margin: 0 0 6px; font-size: 15px; }
.ox p { margin: 0 0 10px; color: var(--muted); font-size: 12.5px; }
.ox label.f { display: block; font-size: 12px; color: var(--muted); margin: 10px 0 4px; }
.ox input, .ox select, .ox textarea { width: 100%; box-sizing: border-box; font: inherit; color: inherit; background: var(--surface);
  border: 1px solid var(--border); border-radius: 8px; padding: 7px 10px; }
.ox textarea { min-height: 130px; resize: vertical; line-height: 1.5; }
.ox button { font: inherit; color: inherit; border: 1px solid var(--border); background: var(--surface); border-radius: 8px; padding: 6px 12px; cursor: pointer; white-space: nowrap; }
.ox button:hover { background: var(--surface-2); }
.ox button.pri { background: var(--accent); border-color: var(--accent); color: var(--accent-ink, #fff); font-weight: 600; }
.ox button.dng { color: var(--bad); }
.ox .to { display: flex; gap: 10px; align-items: center; background: var(--surface-2); border-radius: 10px; padding: 8px 10px; }
.ox .to img, .ox .to .ph { width: 34px; height: 34px; border-radius: 50%; object-fit: cover; flex: none; background: var(--border); }
.ox .to b { display: block; } .ox .to small { color: var(--muted); }
.ox .to .q { margin-left: auto; font-size: 12px; color: var(--muted); white-space: nowrap; }
.ox .tpl { display: flex; gap: 6px; } .ox .tpl select { flex: 1; width: auto; }
.ox .hint { font-size: 11.5px; color: var(--muted); margin-top: 4px; }
.ox .hist { margin-top: 10px; font-size: 12px; border-top: 1px solid var(--border); padding-top: 8px; max-height: 140px; overflow: auto; }
.ox .hist div { padding: 4px 0; border-bottom: 1px dashed var(--border); }
.ox .hist div:last-child { border-bottom: none; }
.ox .hist .m { color: var(--muted); white-space: pre-wrap; }
.ox .btns { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; justify-content: flex-end; margin-top: 12px; }
.ox .msg { margin-right: auto; font-size: 12.5px; color: var(--muted); }`;
  const html = `
<form class="ox" role="dialog" aria-modal="true" aria-labelledby="oxTitle">
  <h3 id="oxTitle">Message creator</h3>
  <p>Pick the X account you're sending from (be logged into it on x.com), write or pick a template, then
    <b>Open DM in X</b>: the chat opens with your text filled in — press send there. Every message is logged
    with the account it went from.</p>
  <div class="to" data-o="to"></div>
  <label class="f">Sending from (our X account)</label>
  <input data-o="from" list="oxSenders" placeholder="@naceai" autocomplete="off">
  <datalist id="oxSenders"></datalist>
  <label class="f">Template</label>
  <div class="tpl">
    <select data-o="tpl"></select>
    <button type="button" data-o="tplSave" title="Save the message below as a template for the team">Save as template</button>
    <button type="button" data-o="tplDel" class="dng" title="Delete this template">✕</button>
  </div>
  <label class="f">Message</label>
  <textarea data-o="text" placeholder="Hey {first_name}! We're Nace AI…"></textarea>
  <div class="hint">{name}, {first_name} and {handle} are filled in for each creator. <span data-o="len"></span></div>
  <div class="hist" data-o="hist" hidden></div>
  <div class="btns">
    <span class="msg" data-o="msg"></span>
    <button type="button" data-o="close">Close</button>
    <button type="button" data-o="skip" title="Go to the next creator without logging">Skip</button>
    <button type="button" data-o="logOnly" title="Already sent it another way: just log it">Log only</button>
    <button type="submit" class="pri" data-o="send">Open DM in X</button>
  </div>
</form>`;
  const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const fmtN = (n) => n == null || !isFinite(n) ? "—" : n >= 1e6 ? (n / 1e6).toFixed(2) + "M" : n >= 1e4 ? (n / 1e3).toFixed(1) + "K" : Math.round(n).toLocaleString();
  const pref = { get(k) { try { return localStorage.getItem(k) || ""; } catch (e) { return ""; } }, set(k, v) { try { localStorage.setItem(k, v); } catch (e) {} } };
  async function api(path, body) {
    const r = await fetch(path, body ? { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) } : {});
    if (r.status === 401) { location.href = "/login"; throw new Error("signed out"); }
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || "HTTP " + r.status);
    return j;
  }
  const fill = (t, c) => t.replace(/\{first_name\}/g, (c.name || c.handle || "").split(/\s+/)[0])
    .replace(/\{name\}/g, c.name || c.handle || "").replace(/\{handle\}/g, c.platform === "x" ? "@" + c.handle : c.name || "");
  const who = (c) => (c.platform || "x") === "x" ? "@" + c.handle : c.name;

  // Link to the DM chat with a creator (no text): used by the Waves roster too.
  function dmUrl(c, text) {
    if ((c.platform || "x") !== "x") return c.post_urls?.[0] || null;
    if (!c.user_id) return `https://x.com/${encodeURIComponent(c.handle)}`;
    return `https://x.com/messages/compose?recipient_id=${encodeURIComponent(c.user_id)}` + (text ? `&text=${encodeURIComponent(text)}` : "");
  }

  let ov, $o, queue = [], idx = 0, OUT = { senders: [], templates: [], log: [] }, onDone = null;
  function mount() {
    if (ov) return;
    const st = document.createElement("style"); st.textContent = css; document.head.appendChild(st);
    ov = document.createElement("div"); ov.className = "ox-ov"; ov.innerHTML = html; document.body.appendChild(ov);
    $o = (k) => ov.querySelector(`[data-o="${k}"]`);
    ov.querySelector("form").addEventListener("submit", (e) => { e.preventDefault(); send(true); });
    $o("logOnly").onclick = () => send(false);
    $o("skip").onclick = () => { idx++; show(); };
    $o("close").onclick = close;
    ov.addEventListener("click", (e) => { if (e.target === ov) close(); });
    document.addEventListener("keydown", (e) => { if (e.key === "Escape" && ov.classList.contains("open")) close(); });
    $o("text").addEventListener("input", updLen);
    $o("tpl").addEventListener("change", () => {
      const t = OUT.templates.find((t) => String(t.id) === $o("tpl").value);
      if (t) $o("text").value = t.body;
      pref.set("xca_tpl", $o("tpl").value); $o("tplDel").hidden = !t; updLen();
    });
    $o("tplSave").onclick = async () => {
      const body = $o("text").value.trim();
      if (!body) { $o("msg").textContent = "Write the message first"; return; }
      const cur = OUT.templates.find((t) => String(t.id) === $o("tpl").value);
      const name = prompt("Template name", cur?.name || "First outreach");
      if (!name) return;
      try {
        const r = await api("/api/outreach/templates/save", { name, body });
        OUT.templates = (await api("/api/outreach")).templates;
        tpls(r.id); pref.set("xca_tpl", String(r.id));
        $o("msg").textContent = `Template "${name}" saved for the team`;
      } catch (err) { $o("msg").textContent = "Couldn't save: " + err.message; }
    };
    $o("tplDel").onclick = async () => {
      const t = OUT.templates.find((t) => String(t.id) === $o("tpl").value);
      if (!t || !confirm(`Delete template "${t.name}" for everyone?`)) return;
      await api("/api/outreach/templates/delete", { id: t.id });
      OUT.templates = OUT.templates.filter((x) => x.id !== t.id); tpls(""); pref.set("xca_tpl", "");
    };
  }
  function tpls(sel) {
    $o("tpl").innerHTML = `<option value="">— no template —</option>` + OUT.templates.map((t) =>
      `<option value="${t.id}" ${String(t.id) === String(sel) ? "selected" : ""}>${esc(t.name)}</option>`).join("");
    $o("tplDel").hidden = !$o("tpl").value;
  }
  function close() { ov.classList.remove("open"); onDone && onDone(); }
  const cur = () => queue[idx];
  function show() {
    const c = cur();
    if (!c) { close(); return; }
    $o("to").innerHTML = `${c.avatar ? `<img src="${esc(c.avatar)}" alt="" referrerpolicy="no-referrer">` : `<span class="ph"></span>`}
      <div><b>${esc(c.name || c.handle)}</b><small>${esc(who(c))} · ${fmtN(c.followers)} followers${c.paid_status ? " · " + (c.paid_status === "paid" ? "paid" : "not paid") : ""}</small></div>
      <span class="q">${queue.length > 1 ? `${idx + 1} of ${queue.length}` : ""}</span>`;
    ov.querySelector("#oxTitle").textContent = "Message " + who(c);
    $o("skip").hidden = queue.length < 2;
    $o("send").textContent = (c.platform || "x") === "x" ? "Open DM in X" : "Copy text & open";
    const mine = (OUT.log || []).filter((l) => l.creator_key === c.key);
    $o("hist").hidden = !mine.length;
    $o("hist").innerHTML = mine.length ? `<b>Already contacted ${mine.length}×</b>` + mine.map((l) =>
      `<div>${new Date(l.sent_at).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })} · from <b>@${esc(l.sender)}</b> by ${esc((l.sent_by || "").split("@")[0])}
        ${l.message ? `<div class="m">${esc(l.message.slice(0, 280))}</div>` : ""}</div>`).join("") : "";
    updLen();
  }
  function updLen() { const c = cur(); if (c) { const n = fill($o("text").value, c).length; $o("len").textContent = n ? `${n} characters` : ""; } }
  async function send(openX) {
    const c = cur(); if (!c) return;
    const sender = $o("from").value.trim();
    if (!sender) { $o("msg").textContent = "Which of our X accounts are you sending from?"; $o("from").focus(); return; }
    const text = fill($o("text").value, c);
    const direct = (c.platform || "x") === "x" && c.user_id;
    if (openX) {   // must open inside the click, or the browser blocks the new tab
      if (!direct) navigator.clipboard?.writeText(text).catch(() => {});
      const url = dmUrl(c, text);
      if (url) window.open(url, "_blank", "noopener");
    }
    pref.set("xca_sender", sender);
    try {
      await api("/api/outreach/log", { key: c.key, sender, message: text });
      OUT.log = [{ creator_key: c.key, sender: sender.replace(/^@/, ""), message: text, sent_at: new Date().toISOString(), sent_by: "you" }, ...(OUT.log || [])];
      $o("msg").textContent = `Logged: ${who(c)} from ${sender.startsWith("@") ? sender : "@" + sender}` + (openX && !direct ? " — text copied, paste it into their DMs" : "");
      idx++;
      if (idx < queue.length) show(); else setTimeout(close, openX ? 600 : 300);
    } catch (err) { $o("msg").textContent = "Couldn't log: " + err.message; }
  }
  async function open(creators, opts = {}) {
    mount();
    queue = creators.filter(Boolean); idx = 0; onDone = opts.onDone || null;
    if (!queue.length) return;
    $o("msg").textContent = "";
    try { OUT = await api("/api/outreach"); } catch (e) { /* still usable without history */ }
    ov.querySelector("#oxSenders").innerHTML = OUT.senders.map((s) => `<option value="@${esc(s)}">`).join("");
    if (!$o("from").value) $o("from").value = pref.get("xca_sender") || (OUT.senders[0] ? "@" + OUT.senders[0] : "");
    tpls(pref.get("xca_tpl"));
    if (!$o("text").value) { const t = OUT.templates.find((t) => String(t.id) === $o("tpl").value); if (t) $o("text").value = t.body; }
    ov.classList.add("open");
    show();
  }
  window.Outreach = { open, dmUrl };
})();
