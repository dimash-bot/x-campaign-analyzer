// Drag-to-resize table columns on every page. Drag the right edge of a header; double-click it to reset the table.
// Widths are remembered per page + table in this browser (localStorage).
(function () {
  const css = `
.cr-h { position: absolute; top: 0; right: -4px; width: 9px; height: 100%; cursor: col-resize; z-index: 3; user-select: none; }
.cr-h::after { content: ""; position: absolute; top: 20%; bottom: 20%; left: 4px; width: 1px; background: var(--border); opacity: 0; transition: opacity .15s; }
th:hover > .cr-h::after, .cr-h.on::after { opacity: 1; background: var(--accent); width: 2px; left: 3px; }
table.cr-fixed { table-layout: fixed; }
table.cr-fixed th, table.cr-fixed td { overflow: hidden; text-overflow: ellipsis; }
body.cr-drag, body.cr-drag * { cursor: col-resize !important; user-select: none !important; }`;
  const st = document.createElement("style"); st.textContent = css; document.head.appendChild(st);

  const store = {
    get(k) { try { return JSON.parse(localStorage.getItem(k)); } catch (e) { return null; } },
    set(k, v) { try { v == null ? localStorage.removeItem(k) : localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} },
  };
  const tables = () => [...document.querySelectorAll("table")].filter((t) => t.tHead && t.tHead.rows.length);
  const keyOf = (t) => `xca_cols:${location.pathname}:${t.id || t.tBodies[0]?.id || tables().indexOf(t)}`;
  const heads = (t) => [...t.tHead.rows[t.tHead.rows.length - 1].cells];

  function freeze(t, widths) {
    const ths = heads(t);
    ths.forEach((th, i) => { const w = widths[i]; th.style.width = th.style.minWidth = th.style.maxWidth = w + "px"; });
    t.style.width = widths.reduce((a, b) => a + b, 0) + "px";
    t.style.minWidth = "0";
    t.classList.add("cr-fixed");
  }
  function reset(t) {
    heads(t).forEach((th) => { th.style.width = th.style.minWidth = th.style.maxWidth = ""; });
    t.style.width = t.style.minWidth = "";
    t.classList.remove("cr-fixed");
    store.set(keyOf(t), null);
  }

  let suppressClick = false;
  document.addEventListener("click", (e) => { if (suppressClick) { e.stopPropagation(); e.preventDefault(); suppressClick = false; } }, true);

  function attach(t) {
    const ths = heads(t);
    if (!ths.length) return;
    // restore saved widths (only if the table still has the same number of columns)
    if (!t.dataset.crDone) {
      const saved = store.get(keyOf(t));
      if (saved && saved.n === ths.length) freeze(t, saved.w);
      t.dataset.crDone = "1";
    }
    ths.forEach((th, i) => {
      if (th.querySelector(":scope > .cr-h")) return;
      if (getComputedStyle(th).position === "static") th.style.position = "relative";
      const h = document.createElement("span");
      h.className = "cr-h"; h.title = "Drag to resize · double-click to reset";
      h.addEventListener("click", (e) => e.stopPropagation());
      h.addEventListener("dblclick", (e) => { e.stopPropagation(); e.preventDefault(); reset(t); });
      h.addEventListener("pointerdown", (e) => {
        e.preventDefault(); e.stopPropagation();
        const cols = heads(t), widths = cols.map((c) => c.getBoundingClientRect().width);
        freeze(t, widths);   // switch to fixed layout at the current sizes, then move just this column
        const x0 = e.clientX, w0 = widths[i];
        h.classList.add("on"); document.body.classList.add("cr-drag");
        const move = (ev) => { widths[i] = Math.max(36, Math.round(w0 + ev.clientX - x0)); freeze(t, widths); };
        const up = (ev) => {
          window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", up); window.removeEventListener("pointercancel", up);
          h.classList.remove("on"); document.body.classList.remove("cr-drag");
          if (Math.abs(ev.clientX - x0) > 2) suppressClick = true, setTimeout(() => { suppressClick = false; }, 0);
          store.set(keyOf(t), { n: widths.length, w: widths });
        };
        window.addEventListener("pointermove", move); window.addEventListener("pointerup", up); window.addEventListener("pointercancel", up);
      });
      th.appendChild(h);
    });
  }

  let queued = false;
  const scan = () => { queued = false; tables().forEach(attach); };
  new MutationObserver(() => { if (!queued) { queued = true; requestAnimationFrame(scan); } })
    .observe(document.body, { childList: true, subtree: true });
  scan();
})();
