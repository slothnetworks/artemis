"use strict";

// Edge health from loss% and p95 latency (ns): ok | warn | bad.
function edgeHealth(s) {
    if (!s) return "ok";
    const lossPct = s.loss_pct || 0;
    const p95ms = (s.p95 || 0) / 1e6;
    if (lossPct >= 20 || p95ms >= 250) return "bad";
    if (lossPct >= 2 || p95ms >= 80) return "warn";
    return "ok";
}
const healthColor = { ok: "#4ade80", warn: "#facc15", bad: "#f87171" };

function edgeLabel(s) {
    if (!s) return "";
    const p95ms = ((s.p95 || 0) / 1e6).toFixed(1);
    return `${p95ms}ms / ${(s.loss_pct || 0).toFixed(0)}%`;
}

const edgeId = (src, dst) => `${src}__${dst}`;

const cy = cytoscape({
    container: document.getElementById("cy"),
    style: [
        { selector: "node", style: {
            "background-color": "#6366f1", "label": "data(label)",
            "color": "#e2e8f0", "font-size": 11, "font-weight": 600, "text-valign": "center",
            "text-halign": "center", "text-margin-y": -15, "width": 26, "height": 26,
            "border-width": 2, "border-color": "#312e81",
        }},
        // Base edges: colored by health, NO label (keeps a large mesh readable).
        { selector: "edge", style: {
            "width": 2, "line-color": "data(color)", "target-arrow-color": "data(color)",
            "target-arrow-shape": "triangle", "arrow-scale": 0.8,
            "curve-style": "bezier", "control-point-step-size": 32,
            "opacity": 0.85, "label": "", "font-size": 9, "color": "#cbd5e1",
            "text-rotation": "autorotate", "text-background-color": "#0b1220",
            "text-background-opacity": 0.85, "text-background-padding": 3, "text-background-shape": "roundrectangle",
        }},
        // Show labels only where it matters: degraded edges and hovered edges.
        { selector: 'edge[health = "warn"], edge[health = "bad"], edge.hl', style: { "label": "data(label)" }},
        { selector: "edge.hl", style: { "width": 3.5, "opacity": 1, "z-index": 99 }},
        { selector: "edge.alerting", style: { "width": 4, "line-color": "#f87171", "target-arrow-color": "#f87171", "line-style": "dashed" }},
    ],
    layout: { name: "cose", animate: false, padding: 40, nodeRepulsion: 9000, idealEdgeLength: 140 },
    minZoom: 0.2, maxZoom: 2.5, wheelSensitivity: 0.2,
});

// Highlight an edge (and show its label) on hover.
cy.on("mouseover", "edge", (e) => e.target.addClass("hl"));
cy.on("mouseout", "edge", (e) => e.target.removeClass("hl"));

function upsertNode(name) {
    if (cy.getElementById(name).length === 0) {
        cy.add({ group: "nodes", data: { id: name, label: name } });
    }
}

function upsertEdge(src, dst, snap) {
    upsertNode(src);
    upsertNode(dst);
    const id = edgeId(src, dst);
    const health = edgeHealth(snap);
    const data = { color: healthColor[health], label: edgeLabel(snap), health };
    const e = cy.getElementById(id);
    if (e.length === 0) {
        cy.add({ group: "edges", data: { id, source: src, target: dst, ...data } });
        relayout();
    } else {
        e.data(data);
    }
}

// Relayout (debounced) only when the node/edge set grows, then fit to view.
let relayoutTimer = null;
function relayout() {
    clearTimeout(relayoutTimer);
    relayoutTimer = setTimeout(() => {
        cy.layout({ name: "cose", animate: false, padding: 40, nodeRepulsion: 9000, idealEdgeLength: 140 }).run();
        cy.fit(null, 50);
    }, 250);
}

async function loadTopology() {
    const t = await api("GET", "/api/topology");
    (t.nodes || []).forEach(n => upsertNode(n.name));
    (t.edges || []).forEach(e => upsertEdge(e.src, e.dst, e.snapshot));
}

function connectSSE() {
    const es = new EventSource("/api/stream");
    const status = document.getElementById("status");
    es.onopen = () => { status.textContent = "live"; status.style.color = "#4ade80"; };
    es.onerror = () => { status.textContent = "reconnecting…"; status.style.color = "#facc15"; };
    es.onmessage = (ev) => {
        let m;
        try { m = JSON.parse(ev.data); } catch { return; }
        if (m.type === "edge") {
            upsertEdge(m.src, m.dst, m.snapshot);
        } else if (m.type === "alert") {
            const e = cy.getElementById(edgeId(m.src, m.dst));
            if (e.length) e[m.state === "firing" ? "addClass" : "removeClass"]("alerting");
        }
    };
}

// Edge drill-down chart.
let chart = null;
cy.on("tap", "edge", async (evt) => {
    const e = evt.target;
    const [src, dst] = e.id().split("__");
    document.getElementById("edge-panel").classList.remove("hidden");
    document.getElementById("edge-title").textContent = `${src} → ${dst}`;
    document.getElementById("edge-diff").href = `/diff?a=${encodeURIComponent(src)}&b=${encodeURIComponent(dst)}`;
    const series = await api("GET", `/api/edges/${encodeURIComponent(src)}/${encodeURIComponent(dst)}/series`);
    const xs = [], p95 = [], loss = [];
    (series || []).forEach(pt => {
        xs.push(new Date(pt.at).getTime() / 1000);
        p95.push((pt.snap.p95 || 0) / 1e6);
        loss.push(pt.snap.loss_pct || 0);
    });
    const opts = {
        width: document.getElementById("edge-chart").clientWidth, height: 220,
        series: [{}, { label: "p95 ms", stroke: "#6366f1" }, { label: "loss %", stroke: "#f87171", scale: "%" }],
        scales: { "%": { side: 1 } }, axes: [{ stroke: "#94a3b8" }, { stroke: "#94a3b8" }, { side: 1, scale: "%", stroke: "#94a3b8" }],
    };
    if (chart) chart.destroy();
    chart = new uPlot(opts, [xs, p95, loss], document.getElementById("edge-chart"));
});

loadTopology().then(connectSSE);
