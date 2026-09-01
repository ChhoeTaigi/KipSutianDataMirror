
import json
from pathlib import Path


def _entry_label(entry):
    hanji = entry.get("漢字", "")
    lomaji = entry.get("羅馬字", "")
    if hanji and lomaji:
        return f"{hanji} ({lomaji})"
    return hanji or lomaji or "?"


def _flat_fields(entry):
    """Return entry-level fields (everything except 義項, which is nested)."""
    return {k: v for k, v in entry.items() if k != "義項"}


def _diff_definitions(old_defs, new_defs):
    """Compare two義項 lists. Returns a list of change descriptions."""
    old_map = {str(d["義項id"]): d for d in old_defs if d.get("義項id")}
    new_map = {str(d["義項id"]): d for d in new_defs if d.get("義項id")}

    changes = []
    for did in sorted(new_map.keys() - old_map.keys()):
        d = new_map[did]
        changes.append(f"  - Added definition {did}: {d.get('解說', '')[:60]}")
    for did in sorted(old_map.keys() - new_map.keys()):
        d = old_map[did]
        changes.append(f"  - Removed definition {did}: {d.get('解說', '')[:60]}")
    for did in sorted(old_map.keys() & new_map.keys()):
        if old_map[did] != new_map[did]:
            changes.append(f"  - Modified definition {did}")
    return changes


def diff_versions(old_json_path, new_json_path):
    """Compare two KipSutianData.json files and return a diff dict."""
    with open(old_json_path, encoding="utf-8") as f:
        old_entries = json.load(f)
    with open(new_json_path, encoding="utf-8") as f:
        new_entries = json.load(f)

    old_map = {str(e["詞目id"]): e for e in old_entries}
    new_map = {str(e["詞目id"]): e for e in new_entries}

    old_ids = set(old_map)
    new_ids = set(new_map)

    added_ids = sorted(new_ids - old_ids, key=int)
    removed_ids = sorted(old_ids - new_ids, key=int)
    common_ids = old_ids & new_ids

    added = [{"id": eid, "label": _entry_label(new_map[eid])} for eid in added_ids]
    removed = [{"id": eid, "label": _entry_label(old_map[eid])} for eid in removed_ids]

    modified = []
    for eid in sorted(common_ids, key=int):
        old_e, new_e = old_map[eid], new_map[eid]
        if old_e == new_e:
            continue

        field_changes = []
        old_flat, new_flat = _flat_fields(old_e), _flat_fields(new_e)
        all_keys = sorted(set(old_flat) | set(new_flat))
        for k in all_keys:
            ov, nv = old_flat.get(k), new_flat.get(k)
            if ov != nv:
                field_changes.append(f"  - `{k}`: {_fmt(ov)} → {_fmt(nv)}")

        def_changes = _diff_definitions(
            old_e.get("義項", []), new_e.get("義項", [])
        )

        modified.append({
            "id": eid,
            "label": _entry_label(new_e),
            "field_changes": field_changes,
            "def_changes": def_changes,
        })

    return {
        "old_count": len(old_entries),
        "new_count": len(new_entries),
        "added": added,
        "removed": removed,
        "modified": modified,
    }


def _fmt(val):
    if val is None:
        return "(empty)"
    s = str(val)
    if len(s) > 80:
        return s[:77] + "..."
    return s


def write_diff_md(diff, old_version, new_version, out_path):
    """Write a VERSION_DIFF.md from a diff dict."""
    lines = [
        f"# Version Diff: {old_version} → {new_version}",
        "",
        f"- Previous: **{old_version}** ({diff['old_count']} entries)",
        f"- Current: **{new_version}** ({diff['new_count']} entries)",
        f"- Added: **{len(diff['added'])}** | Removed: **{len(diff['removed'])}** | Modified: **{len(diff['modified'])}**",
        "",
    ]

    if diff["added"]:
        lines.append("## Added Entries")
        lines.append("")
        for e in diff["added"]:
            lines.append(f"- `{e['id']}` {e['label']}")
        lines.append("")

    if diff["removed"]:
        lines.append("## Removed Entries")
        lines.append("")
        for e in diff["removed"]:
            lines.append(f"- `{e['id']}` {e['label']}")
        lines.append("")

    if diff["modified"]:
        lines.append("## Modified Entries")
        lines.append("")
        for e in diff["modified"]:
            lines.append(f"### `{e['id']}` {e['label']}")
            lines.append("")
            for c in e["field_changes"]:
                lines.append(c)
            for c in e["def_changes"]:
                lines.append(c)
            lines.append("")

    Path(out_path).write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {out_path}")
