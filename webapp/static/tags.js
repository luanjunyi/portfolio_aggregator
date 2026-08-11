// Tags & dimensions UI for the portfolio dashboard.
// - "Tags" column: shows each position's tags for the dimension picked in the
//   header dropdown; click chips to remove, "+" to add.
// - "Manage tags" popover: full CRUD over dimensions and their tags.
// All state lives server-side (portfolio.db); we refetch the model after writes.
(function () {
    "use strict";
    const ROOT = document.body.dataset.scriptRoot || "";

    let model = { dimensions: [], position_tags: {} };
    let activeDimId = null;

    // --- helpers -----------------------------------------------------------
    async function api(method, path, body) {
        const opts = { method, headers: {} };
        if (body !== undefined) {
            opts.headers["Content-Type"] = "application/json";
            opts.body = JSON.stringify(body);
        }
        const res = await fetch(ROOT + path, opts);
        let data = {};
        try { data = await res.json(); } catch (e) { /* no body */ }
        if (!res.ok) throw new Error(data.error || "Request failed (" + res.status + ")");
        return data;
    }

    function el(tag, props, children) {
        const n = document.createElement(tag);
        Object.assign(n, props || {});
        (children || []).forEach((c) => n.append(c));
        return n;
    }

    const dimById = (id) => model.dimensions.find((d) => d.id === id);
    const assignedIds = (symbol) => model.position_tags[symbol] || [];

    async function loadModel() {
        model = await api("GET", "/api/tag-model");
        if (!model.dimensions.some((d) => d.id === activeDimId)) {
            // Default the Tags column to the "strategy" dimension when present,
            // otherwise fall back to the first dimension.
            const preferred = model.dimensions.find((d) => d.name.toLowerCase() === "strategy");
            activeDimId = preferred ? preferred.id
                : (model.dimensions.length ? model.dimensions[0].id : null);
        }
        renderDimSelect();
        renderAllCells();
        if (!manager.hidden) renderManager();
    }

    // --- column: dimension selector + per-row tags -------------------------
    const dimSelect = document.getElementById("tagDimSelect");
    function renderDimSelect() {
        dimSelect.innerHTML = "";
        if (!model.dimensions.length) {
            dimSelect.append(el("option", { textContent: "(no dimensions)", value: "" }));
            dimSelect.disabled = true;
            return;
        }
        dimSelect.disabled = false;
        model.dimensions.forEach((d) =>
            dimSelect.append(el("option", { textContent: d.name, value: String(d.id) }))
        );
        dimSelect.value = String(activeDimId);
    }
    dimSelect.addEventListener("change", () => {
        activeDimId = Number(dimSelect.value);
        renderAllCells();
    });

    function renderAllCells() {
        document.querySelectorAll(".tags-cell").forEach(renderCell);
    }

    function renderCell(cell) {
        const symbol = cell.dataset.symbol;
        cell.innerHTML = "";
        const dim = dimById(activeDimId);
        if (!dim) {
            cell.dataset.sortValue = "";
            cell.append(el("span", { className: "muted", textContent: "—" }));
            return;
        }
        const wrap = el("div", { className: "tag-chips" });
        const ids = assignedIds(symbol);
        const assigned = dim.tags.filter((t) => ids.includes(t.id));
        // sort key for the Tags column: assigned tag names (current dimension)
        cell.dataset.sortValue = assigned.map((t) => t.name.toLowerCase()).sort().join(", ");
        assigned.forEach((t) => {
                const x = el("span", { className: "x", textContent: "×", title: "Remove" });
                x.addEventListener("click", () => togglePositionTag(symbol, t.id, false));
                wrap.append(el("span", { className: "tag-chip" }, [t.name + " ", x]));
            });
        const add = el("span", { className: "tag-add-btn", textContent: "+", title: "Add tag" });
        add.addEventListener("click", (e) => { e.stopPropagation(); openTagMenu(add, symbol); });
        wrap.append(add);
        cell.append(wrap);
    }

    async function togglePositionTag(symbol, tagId, add) {
        try {
            await api("POST", "/api/position-tag", { symbol, tag_id: tagId, action: add ? "add" : "remove" });
            const ids = model.position_tags[symbol] || (model.position_tags[symbol] = []);
            const i = ids.indexOf(tagId);
            if (add && i === -1) ids.push(tagId);
            if (!add && i !== -1) ids.splice(i, 1);
            renderAllCells();
            if (openMenu && openMenu.symbol === symbol) renderTagMenu();
        } catch (err) {
            alert(err.message);
        }
    }

    // --- add-tag dropdown menu (one shared element) ------------------------
    let openMenu = null; // { node, anchor, symbol }
    function openTagMenu(anchor, symbol) {
        closeTagMenu();
        const node = el("div", { className: "tag-menu" });
        openMenu = { node, anchor, symbol };
        renderTagMenu();
        document.body.append(node);
        const r = anchor.getBoundingClientRect();
        node.style.top = window.scrollY + r.bottom + 4 + "px";
        node.style.left = window.scrollX + r.left + "px";
    }
    function renderTagMenu() {
        if (!openMenu) return;
        const { node, symbol } = openMenu;
        node.innerHTML = "";
        const dim = dimById(activeDimId);
        const ids = assignedIds(symbol);
        if (!dim || !dim.tags.length) {
            node.append(el("div", { className: "empty", textContent: "No tags in this dimension. Use “Manage tags”." }));
            return;
        }
        dim.tags.forEach((t) => {
            const checked = ids.includes(t.id);
            const item = el("div", { className: "item" + (checked ? " checked" : ""), textContent: t.name });
            item.addEventListener("click", (e) => { e.stopPropagation(); togglePositionTag(symbol, t.id, !checked); });
            node.append(item);
        });
    }
    function closeTagMenu() {
        if (openMenu) { openMenu.node.remove(); openMenu = null; }
    }

    // --- Manage popover: CRUD ----------------------------------------------
    const manager = document.getElementById("tagManager");
    const managerBody = document.getElementById("tagManagerBody");
    const managerError = document.getElementById("tagManagerError");
    const manageBtn = document.getElementById("manageTagsBtn");

    function showError(msg) {
        managerError.textContent = msg;
        managerError.hidden = !msg;
    }

    async function mutate(fn) {
        showError("");
        try { await fn(); await loadModel(); }
        catch (err) { showError(err.message); }
    }

    function renderManager() {
        managerBody.innerHTML = "";
        if (!model.dimensions.length) {
            managerBody.append(el("div", { className: "muted", textContent: "No dimensions yet. Add one below." }));
        }
        model.dimensions.forEach((dim) => {
            const block = el("div", { className: "dim-block" });

            // dimension row: name + rename + delete
            const rename = el("button", { className: "icon-btn", textContent: "✎", title: "Rename dimension" });
            rename.addEventListener("click", () => {
                const name = prompt("Rename dimension", dim.name);
                if (name && name.trim()) mutate(() => api("PATCH", "/api/dimensions/" + dim.id, { name: name.trim() }));
            });
            const del = el("button", { className: "icon-btn danger", textContent: "🗑", title: "Delete dimension" });
            del.addEventListener("click", () => {
                if (confirm('Delete dimension "' + dim.name + '" and all its tags?'))
                    mutate(() => api("DELETE", "/api/dimensions/" + dim.id));
            });
            block.append(el("div", { className: "dim-row" }, [
                el("span", { className: "dim-name", textContent: dim.name }), rename, del,
            ]));

            // tags
            const tagsWrap = el("div", { className: "dim-tags" });
            if (!dim.tags.length) tagsWrap.append(el("span", { className: "muted", textContent: "no tags yet" }));
            dim.tags.forEach((t) => {
                const x = el("span", { className: "x", textContent: "×", title: "Delete tag" });
                x.addEventListener("click", () => {
                    if (confirm('Delete tag "' + t.name + '"? It will be removed from all positions.'))
                        mutate(() => api("DELETE", "/api/tags/" + t.id));
                });
                const rn = el("span", { style: "cursor:pointer", title: "Rename tag", textContent: t.name + " " });
                rn.addEventListener("click", () => {
                    const name = prompt("Rename tag", t.name);
                    if (name && name.trim()) mutate(() => api("PATCH", "/api/tags/" + t.id, { name: name.trim() }));
                });
                tagsWrap.append(el("span", { className: "dim-tag" }, [rn, x]));
            });
            block.append(tagsWrap);

            // add tag
            const input = el("input", { className: "form-control form-control-sm", placeholder: "New tag" });
            const addBtn = el("button", { className: "btn btn-sm btn-outline-primary", textContent: "Add" });
            const addTag = () => {
                const name = input.value.trim();
                if (name) mutate(() => api("POST", "/api/tags", { dimension_id: dim.id, name }));
            };
            addBtn.addEventListener("click", addTag);
            input.addEventListener("keydown", (e) => { if (e.key === "Enter") addTag(); });
            block.append(el("div", { className: "dim-add" }, [input, addBtn]));

            managerBody.append(block);
        });
    }

    function openManager() {
        showError("");
        renderManager();
        manager.hidden = false;
        const r = manageBtn.getBoundingClientRect();
        manager.style.top = window.scrollY + r.bottom + 6 + "px";
        // keep it on-screen: right-align to the button
        const left = window.scrollX + r.right - manager.offsetWidth;
        manager.style.left = Math.max(8, left) + "px";
    }
    function closeManager() { manager.hidden = true; }

    manageBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        manager.hidden ? openManager() : closeManager();
    });
    document.getElementById("tagManagerClose").addEventListener("click", closeManager);
    document.getElementById("addDimBtn").addEventListener("click", () => {
        const input = document.getElementById("newDimName");
        const name = input.value.trim();
        if (name) mutate(() => api("POST", "/api/dimensions", { name })).then(() => { input.value = ""; });
    });
    document.getElementById("newDimName").addEventListener("keydown", (e) => {
        if (e.key === "Enter") document.getElementById("addDimBtn").click();
    });

    // outside-click closes the menu / popover
    document.addEventListener("click", (e) => {
        if (openMenu && !openMenu.node.contains(e.target)) closeTagMenu();
        if (!manager.hidden && !manager.contains(e.target) && e.target !== manageBtn) closeManager();
    });

    loadModel();
})();
