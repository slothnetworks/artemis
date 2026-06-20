"use strict";

// Shared helpers used by every page.
window.$ = (sel, el = document) => el.querySelector(sel);
window.$$ = (sel, el = document) => [...el.querySelectorAll(sel)];

window.toast = function (msg, bad = false) {
    const t = document.getElementById("toast");
    if (!t) { alert(msg); return; }
    t.textContent = msg;
    t.className = "toast" + (bad ? " bad" : "");
    setTimeout(() => (t.className = "toast hidden"), 3500);
};

// api performs a JSON request against the REST API and throws on non-2xx.
window.api = async function (method, path, body) {
    const opts = { method, headers: {}, credentials: "include" };
    if (body !== undefined) {
        opts.headers["Content-Type"] = "application/json";
        opts.body = JSON.stringify(body);
    }
    const res = await fetch(path, opts);
    if (res.status === 204) return null;
    const ct = res.headers.get("Content-Type") || "";
    const data = ct.includes("json") ? await res.json() : await res.text();
    if (!res.ok) {
        const msg = (data && data.error) || (typeof data === "string" && data) || "request failed";
        throw new Error(msg);
    }
    return data;
};

// confirmDelete deletes a resource then runs onDone (or navigates).
window.confirmDelete = async function (path, message, onDone) {
    if (!confirm(message || "Delete this item?")) return;
    try {
        await api("DELETE", path);
        toast("deleted");
        if (typeof onDone === "function") onDone();
        else location.reload();
    } catch (e) { toast(e.message, true); }
};
