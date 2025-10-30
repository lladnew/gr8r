// dbadmin-react-site/PublishingTable.tsx v1.0.1 FIXED: default sorting and clearing so that mass edit will stay
// dbadmin-react-site/PublishingTable.tsx
// v1.0.0 NEW: Publishing table view (mirrors VideosTable UX, publishing-specific fields)
// - Endpoint: /db1/publishing (GET, POST, DELETE)
// - Row key: publishing_id (fallback: platform_media_id, then title)
// - Status options: Hold, Queued, Scheduling, Scheduled, Posted, Error
// - Editable fields: status (no-clear), scheduled_at (clearable), platform_url (optional clearable)
// - Bulk edit: status, scheduled_at
// - LocalStorage keys are namespaced to avoid clashing with VideosTable

import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  ColumnDef,
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  SortingState,
  useReactTable,
} from "@tanstack/react-table";
import * as Tooltip from "@radix-ui/react-tooltip";
import ColumnSelectorModal from "./ColumnSelectorModal";

// ---------- Storage namespace so we don't collide with VideosTable ----------
const STORAGE_PREFIX = "publishing";
const STORAGE_SORTING_KEY = `${STORAGE_PREFIX}_sorting`;
const STORAGE_VISIBILITY_KEY = `${STORAGE_PREFIX}_columnVisibility`;

// ---------- Types ----------
type RecordType = { [key: string]: any };

// Default visible columns for publishing table
const defaultVisible = [
  "title",
  "channel",
  "platform",
  "status",
  "scheduled_at",
  "platform_media_id",
];

const DEFAULT_SORT_COL = "scheduled_at";
const DEFAULT_SORT: SortingState = [{ id: DEFAULT_SORT_COL, desc: true }];

// ---------- Status Pills (Publishing) ----------
const STATUS_OPTIONS = [
  "Hold",
  "Queued",
  "Scheduling",
  "Scheduled",
  "Posted",
  "Error",
] as const;

type StatusType = (typeof STATUS_OPTIONS)[number];

const statusPillClasses: Record<StatusType, string> = {
  Queued: "bg-blue-100 text-blue-800 border-blue-200",
  Scheduling: "bg-indigo-100 text-indigo-800 border-indigo-200",
  Scheduled: "bg-green-100 text-green-800 border-green-200",
  Posted: "bg-emerald-100 text-emerald-800 border-emerald-200",
  Hold: "bg-red-100 text-red-800 border-red-200",
  Error: "bg-red-500 text-white border-red-600",
};

// ---------- Small pill badge ----------
function Pill({
  children,
  className = "",
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium ${className}`}>
      {children}
    </span>
  );
}

// ---------- Editability ----------
const EDITABLE_NO_CLEAR = new Set(["status"]);
const EDITABLE_WITH_CLEAR = new Set(["scheduled_at", "platform_url"]); // include platform_url only if you want it editable/clearable

function isEditable(field: string) {
  return EDITABLE_NO_CLEAR.has(field) || EDITABLE_WITH_CLEAR.has(field);
}

// ---------- Date helpers ----------
function toLocalDatetimeInputValue(iso?: string | null): string {
  if (!iso) return "";
  const d = new Date(iso);
  const pad = (n: number) => n.toString().padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(
    d.getHours()
  )}:${pad(d.getMinutes())}`;
}
function fromLocalDatetimeInputValue(val: string): string | null {
  if (!val) return null;
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : d.toISOString();
}

// ---------- Timezone helpers (same as VideosTable) ----------
function getDefaultTZ() {
  try {
    const saved = localStorage.getItem("tz") || "";
    if (saved) return saved;
  } catch {}
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
  } catch {
    return "UTC";
  }
}

type TzChoice = { id: string; descriptor: string };
const TZ_CHOICES: TzChoice[] = [
  { id: "UTC", descriptor: "UTC" },
  { id: "America/New_York", descriptor: "Eastern (New York)" },
  { id: "America/Chicago", descriptor: "Central (Chicago)" },
  { id: "America/Denver", descriptor: "Mountain (Denver)" },
  { id: "America/Los_Angeles", descriptor: "Pacific (Los Angeles)" },
  { id: "America/Phoenix", descriptor: "Arizona (Phoenix, no DST)" },
  { id: "America/Anchorage", descriptor: "Alaska (Anchorage)" },
  { id: "Pacific/Honolulu", descriptor: "Hawaii (Honolulu)" },
  { id: "Europe/London", descriptor: "UK (London)" },
  { id: "Europe/Berlin", descriptor: "Central Europe (Berlin)" },
  { id: "Europe/Moscow", descriptor: "Moscow" },
  { id: "Asia/Kolkata", descriptor: "India (Kolkata)" },
  { id: "Asia/Shanghai", descriptor: "China (Shanghai)" },
  { id: "Asia/Tokyo", descriptor: "Japan (Tokyo)" },
  { id: "Australia/Sydney", descriptor: "Australia (Sydney)" },
];

function getTzAbbr(tz: string, at: Date): string {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      timeZoneName: "short",
    }).formatToParts(at);
    return parts.find((p) => p.type === "timeZoneName")?.value || tz;
  } catch {
    return tz;
  }
}

function getTzGmtOffset(tz: string, at: Date): string {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      timeZoneName: "shortOffset",
      hour: "2-digit",
    }).formatToParts(at);
    const off = parts.find((p) => p.type === "timeZoneName")?.value; // e.g. "GMT-4"
    if (off) return off.replace("UTC", "GMT");
  } catch {}
  try {
    const fmt = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz,
      hour12: false,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
    const parts = Object.fromEntries(
      fmt
        .formatToParts(at)
        .filter((p) =>
          ["year", "month", "day", "hour", "minute", "second"].includes(p.type)
        )
        .map((p) => [p.type, p.value])
    ) as Record<string, string>;
    const localMs = Date.UTC(
      +parts.year,
      +parts.month - 1,
      +parts.day,
      +parts.hour,
      +parts.minute,
      +parts.second
    );
    const diffMin = Math.round((localMs - at.getTime()) / 60000);
    const sign = diffMin >= 0 ? "+" : "-";
    const abs = Math.abs(diffMin);
    const hh = String(Math.floor(abs / 60)).padStart(2, "0");
    const mm = String(abs % 60).padStart(2, "0");
    return `GMT${sign}${hh}:${mm}`;
  } catch {
    return "GMT";
  }
}

function tzMenuLabelFriendly(choice: TzChoice): string {
  const now = new Date();
  const abbr = getTzAbbr(choice.id, now);
  const gmt = getTzGmtOffset(choice.id, now);
  return `${choice.descriptor} — ${abbr} (${gmt})`;
}

function tzMenuLabel(tz: string): string {
  const now = new Date();
  const abbr = getTzAbbr(tz, now);
  const gmt = getTzGmtOffset(tz, now);
  return `${abbr} (${gmt}) — ${tz}`;
}

// Compact scheduled display
function formatScheduledCompact(iso?: string | null, tz?: string): string {
  if (!iso) return "-";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "-";
  const zone = tz || getDefaultTZ();

  const timeWithTz = new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone: zone,
    timeZoneName: "short",
  }).format(d);

  const wk = new Intl.DateTimeFormat(undefined, {
    weekday: "short",
    timeZone: zone,
  }).format(d);
  const mo = new Intl.DateTimeFormat(undefined, {
    month: "short",
    timeZone: zone,
  }).format(d);
  const day = new Intl.DateTimeFormat(undefined, {
    day: "2-digit",
    timeZone: zone,
  }).format(d);
  const yr = new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    timeZone: zone,
  }).format(d);

  const wkDot = /\.$/.test(wk) ? wk : `${wk}.`;
  return `${timeWithTz}, ${wkDot} ${mo} ${day}, ${yr}`;
}

// ---------- Component ----------
export default function PublishingTable() {
  const [data, setData] = useState<RecordType[]>([]);
  const [columns, setColumns] = useState<ColumnDef<RecordType>[]>([]);
  const [columnVisibility, setColumnVisibility] = useState<Record<string, boolean>>(
    {}
  );
  const [globalFilter, setGlobalFilter] = useState("");
  const [showColumnModal, setShowColumnModal] = useState(false);
  const [copiedCellId, setCopiedCellId] = useState<string | null>(null);
  const copiedCellIdRef = useRef<string | null>(null);
  const copyTimerRef = useRef<number | null>(null);

  const DEFAULT_SORT: SortingState = [{ id: "title", desc: false }];

  const [sorting, setSorting] = useState<SortingState>(() => {
    try {
      const saved = JSON.parse(localStorage.getItem(STORAGE_SORTING_KEY) || "null");
      return Array.isArray(saved) && saved.length ? saved : DEFAULT_SORT;
    } catch {
      return DEFAULT_SORT;
    }
  });

  const [pagination, setPagination] = useState({ pageIndex: 0, pageSize: 25 });

  // Edit sheet state (status, scheduled_at, optional platform_url)
  const [editOpen, setEditOpen] = useState(false);
  const [editRecord, setEditRecord] = useState<RecordType | null>(null);
  const [editLocal, setEditLocal] = useState<Record<string, any>>({});
  const [editClears, setEditClears] = useState<Set<string>>(new Set());

  // Confirm-clear modal
  const [confirmClearOpen, setConfirmClearOpen] = useState(false);
  const [confirmClearFields, setConfirmClearFields] = useState<string[]>([]);
  const [confirmInput, setConfirmInput] = useState("");

  // Bulk-edit state (status + scheduled_at only)
  const [bulkOpen, setBulkOpen] = useState(false);
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set<string>());
  const selectedKeysRef = useRef<Set<string>>(new Set());
  selectedKeysRef.current = selectedKeys;

  const [selectAllOnPage, setSelectAllOnPage] = useState(false);
  const selectAllOnPageRef = useRef(false);
  selectAllOnPageRef.current = selectAllOnPage;

  const [bulkStatus, setBulkStatus] = useState<StatusType | "">("");
  const [bulkScheduledLocal, setBulkScheduledLocal] = useState<string>("");
  const [bulkClearScheduled, setBulkClearScheduled] = useState<boolean>(false);

  // TZ state
  const [tz, setTz] = useState<string>(() => getDefaultTZ());
  useEffect(() => {
    try {
      localStorage.setItem("tz", tz);
    } catch {}
  }, [tz]);

  // Helpers
  const getRowKey = (r: RecordType): string =>
    String(
      r.publishing_id ??
      r.id ??
      `${r.platform ?? "x"}:${r.platform_media_id ?? r.title ?? Math.random()}`
    );

  // column vis persistence
  useEffect(() => {
    if (Object.keys(columnVisibility).length) {
      localStorage.setItem(STORAGE_VISIBILITY_KEY, JSON.stringify(columnVisibility));
    }
  }, [columnVisibility]);

  // clear selection when filter/sort/page changes
  useEffect(() => {
    if (selectedKeys.size) {
      setSelectedKeys(new Set());
      setSelectAllOnPage(false);
      setBulkOpen(false);
    }
  }, [globalFilter, pagination.pageIndex, pagination.pageSize]);

  // Ensure sorting references an existing column
  useEffect(() => {
    if (!columns.length || !sorting.length) return;
    const colIds = new Set(
      columns.map(
        (c) => ((c as any).id as string) ?? ((c as any).accessorKey as string)
      )
    );
    const [first, ...rest] = sorting;
    if (first && !colIds.has(first.id)) {
      setSorting(rest.length ? rest.filter((s) => colIds.has(s.id)) : DEFAULT_SORT);
    }
  }, [columns, sorting]);

  useEffect(() => {
    copiedCellIdRef.current = copiedCellId;
  }, [copiedCellId]);

  // initial fetch + columns
  useEffect(() => {
    (async () => {
      const API_BASE = import.meta.env.DEV ? "" : "https://admin.gr8r.com";
      const res = await fetch(`${API_BASE}/db1/publishing`, {
        credentials: import.meta.env.DEV ? "same-origin" : "include",
      });

      const records = await res.json();
      if (records.length) {
        setData(records);
        const sample = records[0];
        const allKeys = Object.keys(sample);
        const savedVisibility = JSON.parse(
          localStorage.getItem(STORAGE_VISIBILITY_KEY) || "null"
        );

        let visibility = Object.fromEntries(
          allKeys.map((k) => [k, savedVisibility?.[k] ?? defaultVisible.includes(k)])
        );
        if (allKeys.includes(DEFAULT_SORT_COL) && visibility[DEFAULT_SORT_COL] === false) {
          visibility = { ...visibility, [DEFAULT_SORT_COL]: true };
        }
        // Always show publishing_id if present but not necessarily default-visible
        if (allKeys.includes("publishing_id") && visibility["publishing_id"] === undefined) {
          visibility["publishing_id"] = false; // hidden by default, can be enabled
        }
        setColumnVisibility(visibility);

        setColumns(() => {
          const baseCols: ColumnDef<RecordType>[] = allKeys.map((key) => ({
            accessorKey: key,
            enableSorting: true,
            header: ({ column }) => {
              const dir = column.getIsSorted();
              return (
                <button
                  type="button"
                  onClick={column.getToggleSortingHandler()}
                  className="flex items-center gap-1 cursor-pointer select-none hover:underline"
                  aria-sort={
                    dir === "asc" ? "ascending" : dir === "desc" ? "descending" : "none"
                  }
                  title="Click to sort"
                >
                  {key}
                  <span className="inline-block w-3 text-xs">
                    {dir === "asc" ? "▲" : dir === "desc" ? "▼" : ""}
                  </span>
                </button>
              );
            },
            cell: (info) => {
              const val = info.getValue();
              const cellId = `${info.row.id}-${key}`;

              if (key === "status" && typeof val === "string" && (STATUS_OPTIONS as readonly string[]).includes(val)) {
                const cls = statusPillClasses[val as StatusType];
                return <Pill className={cls}>{val}</Pill>;
              }
              if (key === "scheduled_at") {
                return (
                  <div className="truncate max-w-[360px]">
                    {formatScheduledCompact(val as string | null, tz)}
                  </div>
                );
              }

              const display = val?.toString() || "-";
              const isCopied = copiedCellIdRef.current === cellId;

              return (
                <Tooltip.Root delayDuration={200} open={isCopied || undefined}>
                  <Tooltip.Trigger asChild>
                    <div
                      className={`truncate max-w-[200px] cursor-pointer px-1 ${
                        isCopied ? "bg-orange-100 transition duration-300" : ""
                      }`}
                      onClick={() => {
                        navigator.clipboard.writeText(display);
                        copiedCellIdRef.current = cellId;
                        setCopiedCellId(cellId);
                        if (copyTimerRef.current) {
                          window.clearTimeout(copyTimerRef.current);
                        }
                        copyTimerRef.current = window.setTimeout(() => {
                          setCopiedCellId(null);
                          copiedCellIdRef.current = null;
                          copyTimerRef.current = null;
                        }, 1000);
                      }}
                    >
                      {display}
                    </div>
                  </Tooltip.Trigger>
                  <Tooltip.Portal>
                    <Tooltip.Content
                      side="top"
                      sideOffset={5}
                      className={`z-50 rounded-md px-2 py-1 text-sm max-w-[500px] break-words border text-black ${
                        isCopied
                          ? "bg-orange-600 border-orange-600"
                          : "bg-white border-2 border-[#003E24]"
                      }`}
                    >
                      {isCopied ? (
                        <span className="text-green-200 font-semibold">Copied!</span>
                      ) : (
                        <>
                          <div className="text-gray-400 text-[10px] pb-1">
                            Clicking will copy contents to clipboard
                          </div>
                          <div className="text-[#003E24]">{display}</div>
                        </>
                      )}
                      <Tooltip.Arrow
                        className={isCopied ? "fill-orange-600" : "fill-[#003E24]"}
                      />
                    </Tooltip.Content>
                  </Tooltip.Portal>
                </Tooltip.Root>
              );
            },
          }));

          // Actions (kebab) column – sticky leftmost
          const kebabCol: ColumnDef<RecordType> = {
            id: "_actions",
            enableHiding: false,
            enableSorting: false,
            header: () => <div className="w-[56px]" aria-hidden />,
            cell: ({ row }) => {
              const rec = row.original as RecordType;
              return (
                <div className="pl-2 pr-1 py-1">
                  <button
                    className="px-2 py-1 text-xl leading-none rounded hover:bg-gray-100"
                    aria-label={`Edit ${rec.title ?? ""}`}
                    onClick={(e) => {
                      e.stopPropagation();
                      openEditSheet(rec);
                    }}
                  >
                    ⋮
                  </button>
                </div>
              );
            },
          };

          // Selection checkbox column – sticky after kebab
          const selectCol: ColumnDef<RecordType> = {
            id: "_select",
            header: () => {
              const pageKeys =
                table?.getRowModel().rows.map((r) => getRowKey(r.original)) ?? [];
              const allSelectedThisPage =
                pageKeys.length > 0 &&
                pageKeys.every((k) => selectedKeysRef.current.has(k));

              return (
                <input
                  type="checkbox"
                  aria-label="Select all on page"
                  checked={allSelectedThisPage}
                  onChange={(e) => {
                    const checked = e.target.checked;
                    setSelectedKeys((prev) => {
                      const next = new Set(prev);
                      if (checked) pageKeys.forEach((k) => next.add(k));
                      else pageKeys.forEach((k) => next.delete(k));
                      setBulkOpen(next.size > 0);
                      setSelectAllOnPage(checked);
                      return next;
                    });
                  }}
                />
              );
            },
            enableHiding: false,
            enableSorting: false,
            cell: ({ row }) => {
              const key = getRowKey(row.original as RecordType);
              const checked = selectedKeysRef.current.has(key);
              return (
                <input
                  type="checkbox"
                  aria-label={`Select ${row.original.title ?? key}`}
                  checked={checked}
                  onChange={(e) => {
                    const isChecked = e.target.checked;
                    setSelectedKeys((prev) => {
                      const next = new Set(prev);
                      if (isChecked) next.add(key);
                      else next.delete(key);
                      setBulkOpen(next.size > 0);

                      const pageKeys = table
                        .getRowModel()
                        .rows.map((r) => getRowKey(r.original));
                      const allSelectedThisPage =
                        pageKeys.length > 0 && pageKeys.every((k) => next.has(k));
                      setSelectAllOnPage(allSelectedThisPage);

                      return next;
                    });
                  }}
                />
              );
            },
          };

          return [kebabCol, selectCol, ...baseCols];
        });

        if (!sorting.length && allKeys.includes(DEFAULT_SORT_COL)) {
          setSorting(DEFAULT_SORT);
        }
      }
    })();

    // cleanup copy timer on unmount
    return () => {
      if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current);
    };
  }, []); // initial load only

  // ensure active sort col is visible
  useEffect(() => {
    const id = sorting[0]?.id as string | undefined;
    if (!id || !columns.length) return;
    const hasCol = columns.some(
      (c) => (c as any).accessorKey === id || (c as any).id === id
    );
    if (!hasCol) return;
    setColumnVisibility((prev) => {
      if (prev && prev[id] === false) {
        return { ...prev, [id]: true };
      }
      return prev;
    });
  }, [sorting, columns]);

  // reset to first page when filter/sort changes
  useEffect(() => setPagination((p) => ({ ...p, pageIndex: 0 })), [globalFilter]);
  useEffect(() => setPagination((p) => ({ ...p, pageIndex: 0 })), [sorting]);

  // persist sorting
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_SORTING_KEY, JSON.stringify(sorting));
    } catch {}
  }, [sorting]);

  // Table instance
  const table = useReactTable<RecordType>({
    data,
    columns,
    state: { columnVisibility, globalFilter, pagination, sorting },
    onColumnVisibilityChange: setColumnVisibility,
    onGlobalFilterChange: setGlobalFilter,
    onPaginationChange: setPagination,
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
    manualPagination: false,
  });

  // Derived counts
  const totalRows = table.getFilteredRowModel().rows.length;
  const pageIndex = table.getState().pagination.pageIndex;
  const pageSize = table.getState().pagination.pageSize;
  const pageCount = table.getPageCount();
  const currentSort = table.getState().sorting?.[0];
  const sortedByLabel = currentSort?.id ?? DEFAULT_SORT_COL;
  const sortedDirArrow = currentSort ? (currentSort.desc ? "↓" : "↑") : "↓";

  // ---------- Edit helpers ----------
  function visibleEditableFieldsFor(record: RecordType) {
    const hasVisibility = Object.keys(columnVisibility).length > 0;
    const visibleKeys = hasVisibility
      ? Object.keys(columnVisibility).filter((k) => columnVisibility[k])
      : Object.keys(record).filter((k) => defaultVisible.includes(k) || isEditable(k));
    return visibleKeys.filter((k) => isEditable(k) && k in record);
  }

  function openEditSheet(record: RecordType) {
    let fields = visibleEditableFieldsFor(record);
    if (!fields.length) {
      fields = Object.keys(record).filter((k) => isEditable(k));
    }
    const start: Record<string, any> = {};
    fields.forEach((k) => {
      if (k === "scheduled_at") {
        start[k] = toLocalDatetimeInputValue(record[k]);
      } else {
        const v = record[k];
        start[k] = v == null ? "" : String(v);
      }
    });
    setEditRecord(record);
    setEditLocal(start);
    setEditClears(new Set());
    setEditOpen(true);
  }

  async function savePartial(body: Record<string, any>) {
    const API_BASE = import.meta.env.DEV ? "" : "https://admin.gr8r.com";
    const res = await fetch(`${API_BASE}/db1/publishing`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: import.meta.env.DEV ? "same-origin" : "include",
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      return { ok: false as const, err: text || `HTTP ${res.status}` };
    }
    return { ok: true as const };
  }

  async function bulkSaveSelected() {
    const count = selectedKeys.size;
    if (!count) return;

    const byKey = new Map<string, RecordType>();
    data.forEach((r) => byKey.set(getRowKey(r), r));

    const nowISO = new Date().toISOString();

    type BulkResult =
      | { selKey: string; ok: true }
      | { selKey: string; ok: false; err: string };

    const keys: string[] = Array.from(selectedKeys);
    const jobs: Promise<BulkResult>[] = keys.map(async (selKey: string): Promise<BulkResult> => {
      const rec = byKey.get(selKey);
      if (!rec) return { selKey, ok: false as const, err: "Missing record in view" };

      const payload: Record<string, any> = {};
      // Identify the row
      if (rec.publishing_id != null) payload.publishing_id = rec.publishing_id;
      else if (rec.platform_media_id) payload.platform_media_id = rec.platform_media_id;
      else if (rec.title) payload.title = rec.title;

      // status
      if (bulkStatus) payload.status = bulkStatus;

      // scheduled_at
      if (bulkClearScheduled) {
        payload.clears = [...(payload.clears ?? []), "scheduled_at"];
      } else if (bulkScheduledLocal) {
        const iso = fromLocalDatetimeInputValue(bulkScheduledLocal);
        if (iso) payload.scheduled_at = iso;
      }

      // no-op?
      const keysToCheck = Object.keys(payload).filter(
        (k) => !["publishing_id", "platform_media_id", "title"].includes(k)
      );
      if (keysToCheck.length === 0) return { selKey, ok: true as const };

      // optimistic UI
      setData((prev) =>
        prev.map((r) => {
          if (getRowKey(r) !== selKey) return r;
          const next = { ...r };
          if (payload.status) next.status = payload.status;
          if (payload.scheduled_at) next.scheduled_at = payload.scheduled_at;
          if (payload.clears?.includes("scheduled_at")) next.scheduled_at = null;
          next.record_modified = nowISO;
          return next;
        })
      );

      const res = await savePartial(payload);
      if (!res.ok) {
        // rollback this one
        setData((prev) => prev.map((r) => (getRowKey(r) === selKey ? byKey.get(selKey)! : r)));
        return { selKey, ok: false as const, err: res.err };
      }
      return { selKey, ok: true as const };
    });

    const results = await Promise.all(jobs);
    const failures = results.filter(
      (r): r is { selKey: string; ok: false; err: string } => r.ok === false
    );
    if (failures.length) {
      alert(
        `Some rows failed to save (${failures.length}/${count}).\n` +
          failures
            .slice(0, 5)
            .map((f) => `• ${f.selKey}: ${f.err}`)
            .join("\n") +
          (failures.length > 5 ? `\n…and ${failures.length - 5} more` : "")
      );
    }
    setSelectedKeys(new Set());
    setSelectAllOnPage(false);
    setBulkOpen(false);
  }

  async function bulkDeleteSelected() {
    const count = selectedKeys.size;
    if (!count) return;

    const keys: string[] = Array.from(selectedKeys);
    const toSend = keys
      .map((k) => {
        const rec = data.find((r) => getRowKey(r) === k);
        if (!rec) return null;
        if (rec.publishing_id != null) return { publishing_id: rec.publishing_id };
        if (rec.platform_media_id) return { platform_media_id: rec.platform_media_id };
        return null;
      })
      .filter(Boolean) as Array<{ publishing_id?: number | string; platform_media_id?: string }>;

    if (!toSend.length) return;

    const API_BASE = import.meta.env.DEV ? "" : "https://admin.gr8r.com";
    const res = await fetch(`${API_BASE}/db1/publishing`, {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      credentials: import.meta.env.DEV ? "same-origin" : "include",
      body: JSON.stringify({ keys: toSend }),
    });

    if (!res.ok) {
      const text = await res.text();
      alert(`Delete failed: ${text || `HTTP ${res.status}`}`);
      return;
    }

    setData((prev) => prev.filter((r) => !selectedKeys.has(getRowKey(r))));
    setSelectedKeys(new Set());
    setSelectAllOnPage(false);
    setBulkOpen(false);
  }

  // Reset visible columns for this table (namespaced)
  const handleResetColumns = () => {
    const reset = Object.fromEntries(
      columns.map((c) => [
        (c.accessorKey as string) ?? (c.id as string),
        defaultVisible.includes((c.accessorKey as string) ?? (c.id as string)),
      ])
    );
    const activeSortId = (sorting[0]?.id as string) ?? DEFAULT_SORT_COL;
    if (activeSortId in reset) reset[activeSortId] = true;
    setColumnVisibility(reset);
    localStorage.setItem(STORAGE_VISIBILITY_KEY, JSON.stringify(reset));
  };

  // Table UI
  return (
    <Tooltip.Provider>
      <div className="overflow-x-auto">
        <ColumnSelectorModal
          isOpen={showColumnModal}
          onClose={() => setShowColumnModal(false)}
          onReset={handleResetColumns}
          columns={table.getAllLeafColumns().filter((c) => c.id !== "_select" && c.id !== "_actions")}
          title="Edit Visible Columns (Publishing)"
        />

        <div className="mb-2 flex flex-wrap items-center gap-4">
          <input
            type="text"
            className="border px-2 py-1 rounded"
            placeholder="Search..."
            value={globalFilter ?? ""}
            onChange={(e) => setGlobalFilter(e.target.value)}
          />

          <div className="flex items-center gap-2">
            <label htmlFor="pageSize" className="text-sm">
              Rows per page
            </label>
            <select
              id="pageSize"
              className="border px-2 py-1 rounded text-sm"
              value={pageSize}
              onChange={(e) => table.setPageSize(Number(e.target.value))}
            >
              {[10, 25, 50, 100].map((size) => (
                <option key={size} value={size}>
                  {size}
                </option>
              ))}
            </select>
          </div>

          <div className="ml-auto flex items-center gap-2">
            <span className="text-xs border rounded-full px-2 py-0.5 bg-gray-50">
              Sorted by <strong>{sortedByLabel}</strong> {sortedDirArrow}
            </span>
            <button
              className="text-xs underline"
              onClick={() => setSorting(DEFAULT_SORT)}
              title="Reset to default sort"
            >
              ↺ Default sort
            </button>
          </div>

          <button className="border px-2 py-1 rounded text-sm" onClick={() => setShowColumnModal(true)}>
            Change Viewable Fields
          </button>

          <div className="flex items-center gap-2">
            <label htmlFor="tz" className="text-sm">
              Time zone
            </label>
            <select
              id="tz"
              className="border px-2 py-1 rounded text-sm max-w-[420px]"
              value={tz}
              onChange={(e) => setTz(e.target.value)}
            >
              {useMemo(() => {
                // show user’s saved browser TZ at top if not in curated list
                if (TZ_CHOICES.some((c) => c.id === tz)) return TZ_CHOICES;
                const pretty = tz.includes("/") ? tz.split("/").pop()!.replace(/_/g, " ") : tz;
                return [{ id: tz, descriptor: pretty }, ...TZ_CHOICES];
              }, [tz]).map((z) => (
                <option key={z.id} value={z.id}>
                  {tzMenuLabelFriendly(z)}
                </option>
              ))}
            </select>
          </div>

          <div className="text-sm">
            {totalRows === 0 ? "0 results" : `Showing ${pageIndex * pageSize + 1}–${Math.min(totalRows, (pageIndex + 1) * pageSize)} of ${totalRows}`}
          </div>
        </div>

        {/* Bulk edit bar */}
        {bulkOpen && selectedKeys.size > 0 && (
          <div className="sticky top-0 z-40 border-b bg-white">
            <div className="flex flex-wrap items-end gap-3 p-2">
              <div className="text-sm mr-3">
                <strong>{selectedKeys.size}</strong> selected
              </div>

              {/* Status buttons */}
              <div>
                <label className="block text-xs">Status</label>
                <div className="flex gap-1 flex-wrap">
                  {STATUS_OPTIONS.map((opt) => (
                    <button
                      key={opt}
                      type="button"
                      className={`rounded-full border px-2 py-0.5 text-xs ${bulkStatus === opt ? "ring-2 ring-offset-1" : ""} ${statusPillClasses[opt]}`}
                      onClick={() => setBulkStatus((prev) => (prev === opt ? "" : opt))}
                      title={opt}
                    >
                      {opt}
                    </button>
                  ))}
                </div>
              </div>

              {/* Scheduled_at */}
              <div>
                <label className="block text-xs">Scheduled at ({tzMenuLabel(tz)})</label>
                <div className="flex items-center gap-2">
                  <input
                    type="datetime-local"
                    className="border rounded px-2 py-1 text-sm"
                    value={bulkScheduledLocal}
                    onChange={(e) => {
                      setBulkScheduledLocal(e.target.value);
                      setBulkClearScheduled(false);
                    }}
                  />
                  <label className="text-xs inline-flex items-center gap-1">
                    <input
                      type="checkbox"
                      checked={bulkClearScheduled}
                      onChange={(e) => {
                        setBulkClearScheduled(e.target.checked);
                        if (e.target.checked) setBulkScheduledLocal("");
                      }}
                    />
                    Clear
                  </label>
                </div>
              </div>

              <div className="ml-auto flex items-center gap-2">
                <button
                  className="px-3 py-1 border rounded"
                  onClick={() => {
                    setSelectedKeys(new Set());
                    setSelectAllOnPage(false);
                    setBulkOpen(false);
                    setBulkStatus("");
                    setBulkScheduledLocal("");
                    setBulkClearScheduled(false);
                  }}
                >
                  Cancel
                </button>
                <button
                  className="px-3 py-1 border rounded bg-[#003E24] text-white"
                  onClick={bulkSaveSelected}
                  disabled={!bulkStatus && !bulkScheduledLocal && !bulkClearScheduled}
                  title="Apply to selected"
                >
                  Save to {selectedKeys.size} rows
                </button>
                <button
                  className="px-3 py-1 border rounded bg-red-600 text-white"
                  onClick={async () => {
                    const confirm = window.prompt(
                      `Type YES to delete ${selectedKeys.size} publishing row(s). This is not reversible.`
                    );
                    if (confirm === "YES") await bulkDeleteSelected();
                  }}
                >
                  Delete {selectedKeys.size}
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Table */}
        <div className="overflow-auto max-h-[calc(100vh-200px)]" style={bulkOpen && selectedKeys.size > 0 ? { paddingTop: 56 } : undefined}>
          <table className="min-w-[1000px] divide-y divide-gray-300 text-sm table-fixed">
            <thead className="bg-gray-50">
              {table.getHeaderGroups().map((hg) => (
                <tr key={hg.id}>
                  {hg.headers.map((header) => (
                    <th
                      key={header.id}
                      className={
                        "px-2 py-1 text-left whitespace-nowrap sticky top-0 z-10 bg-white " +
                        (header.column.id === "scheduled_at"
                          ? "w-[360px] min-w-[360px] max-w-[360px] "
                          : "") +
                        (header.column.id === "_actions"
                          ? "sticky left-0 z-20 bg-white w-[56px] min-w-[56px] max-w-[56px] shadow-[inset_-8px_0_8px_-8px_rgba(0,0,0,0.08)]"
                          : header.column.id === "_select"
                          ? "sticky left-[56px] z-20 bg-white w-[40px] min-w-[40px] max-w-[40px] shadow-[inset_-8px_0_8px_-8px_rgba(0,0,0,0.08)]"
                          : "")
                      }
                    >
                      {flexRender(header.column.columnDef.header, header.getContext())}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {table.getRowModel().rows.map((row) => (
                <tr
                  key={row.id}
                  onContextMenu={(e) => {
                    e.preventDefault();
                    openEditSheet(row.original as RecordType);
                  }}
                >
                  {row.getVisibleCells().map((cell) => (
                    <td
                      key={cell.id}
                      className={
                        "px-2 py-1 whitespace-nowrap " +
                        (cell.column.id === "scheduled_at"
                          ? "w-[360px] min-w-[360px] max-w-[360px]"
                          : "overflow-hidden text-ellipsis max-w-[200px] ") +
                        (cell.column.id === "_actions"
                          ? "sticky left-0 z-10 bg-white w-[56px] min-w-[56px] max-w-[56px] shadow-[inset_-8px_0_8px_-8px_rgba(0,0,0,0.08)]"
                          : cell.column.id === "_select"
                          ? "sticky left-[56px] z-10 bg-white w-[40px] min-w-[40px] max-w-[40px] shadow-[inset_-8px_0_8px_-8px_rgba(0,0,0,0.08)]"
                          : "")
                      }
                    >
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Pager */}
        <div className="mt-4 flex flex-wrap items-center gap-4">
          <div className="text-sm">
            Page {pageCount === 0 ? 0 : pageIndex + 1} of {pageCount}
          </div>

          <div className="flex items-center gap-2">
            <button className="px-2 py-1 border rounded" onClick={() => table.setPageIndex(0)} disabled={!table.getCanPreviousPage()}>
              ⏮ First
            </button>
            <button className="px-2 py-1 border rounded" onClick={() => table.previousPage()} disabled={!table.getCanPreviousPage()}>
              Previous
            </button>
            <button className="px-2 py-1 border rounded" onClick={() => table.nextPage()} disabled={!table.getCanNextPage()}>
              Next
            </button>
            <button
              className="px-2 py-1 border rounded"
              onClick={() => table.setPageIndex(pageCount - 1)}
              disabled={!table.getCanNextPage()}
            >
              Last ⏭
            </button>
          </div>
        </div>
      </div>

      {/* Edit bottom sheet */}
      {editOpen && editRecord && (
        <div className="fixed inset-0 z-50 bg-black/40 flex items-end" onClick={() => setEditOpen(false)}>
          <div
            className="w-full bg-white rounded-t-2xl p-4 max-h-[85vh] overflow-auto"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between">
              <div className="font-semibold text-lg">Edit: {editRecord.title}</div>
              <button className="text-2xl leading-none" onClick={() => setEditOpen(false)}>
                ×
              </button>
            </div>

            {visibleEditableFieldsFor(editRecord).map((field) => {
              const canClear = EDITABLE_WITH_CLEAR.has(field);
              return (
                <div key={field} className="mt-4">
                  <label className="block text-sm font-medium mb-1">{field}</label>

                  {field === "scheduled_at" ? (
                    <input
                      type="datetime-local"
                      className="border rounded w-full px-2 py-1"
                      value={editLocal[field] ?? ""}
                      onChange={(e) => {
                        setEditLocal((s) => ({ ...s, [field]: e.target.value }));
                        setEditClears((prev) => {
                          const next = new Set(prev);
                          next.delete(field);
                          return next;
                        });
                      }}
                    />
                  ) : field === "status" ? (
                    <div className="flex flex-wrap gap-2">
                      {STATUS_OPTIONS.map((opt) => {
                        const active = (editLocal[field] ?? "") === opt;
                        const cls = statusPillClasses[opt];
                        return (
                          <button
                            key={opt}
                            type="button"
                            className={`rounded-full border px-3 py-1 text-xs font-medium ${cls} ${
                              active ? "ring-2 ring-offset-1" : "opacity-80 hover:opacity-100"
                            }`}
                            onClick={() => {
                              setEditLocal((s) => ({ ...s, [field]: opt }));
                            }}
                          >
                            {opt}
                          </button>
                        );
                      })}
                    </div>
                  ) : (
                    <textarea
                      className="border rounded w-full px-2 py-1 min-h-24"
                      value={editLocal[field] ?? ""}
                      onChange={(e) => {
                        setEditLocal((s) => ({ ...s, [field]: e.target.value }));
                        setEditClears((prev) => {
                          const next = new Set(prev);
                          next.delete(field);
                          return next;
                        });
                      }}
                    />
                  )}

                  <div className="mt-1 flex items-center gap-2">
                    {canClear ? (
                      <>
                        <button
                          className="px-2 py-0.5 text-xs border rounded"
                          onClick={() => {
                            setConfirmClearFields([field]);
                            setConfirmInput("");
                            setConfirmClearOpen(true);
                          }}
                        >
                          Clear
                        </button>
                        {editClears.has(field) && (
                          <span className="text-xs text-orange-700">Will clear on save</span>
                        )}
                      </>
                    ) : (
                      <span className="text-xs text-gray-500">Clearing disabled</span>
                    )}
                  </div>
                </div>
              );
            })}

            <div className="flex justify-end gap-2 mt-6">
              <button className="px-3 py-1 border rounded" onClick={() => setEditOpen(false)}>
                Cancel
              </button>
              <button
                className="px-3 py-1 border rounded bg-[#003E24] text-white"
                onClick={async () => {
                  if (!editRecord) return;

                  const payload: Record<string, any> = {};
                  // Identify publishing row for save
                  if (editRecord.publishing_id != null)
                    payload.publishing_id = editRecord.publishing_id;
                  else if (editRecord.platform_media_id)
                    payload.platform_media_id = editRecord.platform_media_id;
                  else if (editRecord.title) payload.title = editRecord.title;

                  const clears: string[] = [];

                  for (const field of visibleEditableFieldsFor(editRecord)) {
                    if (EDITABLE_WITH_CLEAR.has(field) && editClears.has(field)) {
                      clears.push(field);
                      continue;
                    }
                    const nextVal = editLocal[field];
                    const original = editRecord[field];

                    if (field === "scheduled_at") {
                      const nextISO = fromLocalDatetimeInputValue(nextVal);
                      const origISO = original ?? null;
                      if (nextISO !== origISO) {
                        if (nextISO) payload[field] = nextISO;
                      }
                    } else {
                      const normalizedNext = (nextVal ?? "").toString();
                      const normalizedOrig = (original ?? "").toString();
                      if (normalizedNext !== normalizedOrig) {
                        payload[field] = normalizedNext;
                      }
                    }
                  }

                  if (clears.length) payload.clears = clears;

                  if (
                    Object.keys(payload).filter(
                      (k) =>
                        !["publishing_id", "platform_media_id", "title", "clears"].includes(k)
                    ).length === 0 &&
                    !payload.clears
                  ) {
                    setEditOpen(false);
                    return;
                  }

                  // optimistic UI
                  const before = editRecord;
                  setData((prev) =>
                    prev.map((r) => {
                      if (getRowKey(r) !== getRowKey(before)) return r;
                      const next = { ...r, ...payload };
                      if (clears.length) clears.forEach((f) => (next[f] = null));
                      next.record_modified = new Date().toISOString();
                      return next;
                    })
                  );

                  const res = await savePartial(payload);
                  if (!res.ok) {
                    // rollback
                    setData((prev) =>
                      prev.map((r) => (getRowKey(r) === getRowKey(before) ? before : r))
                    );
                    alert(`Save failed: ${res.err}`);
                    return;
                  }
                  setEditOpen(false);
                }}
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Confirm-clear modal */}
      {confirmClearOpen && (
        <div
          className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center"
          onClick={() => setConfirmClearOpen(false)}
        >
          <div
            className="bg-white rounded-xl shadow-xl p-4 w-full max-w-md"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="font-semibold text-lg mb-2">Confirm Clear</div>
            <p className="text-sm mb-2">
              This will permanently clear: <strong>{confirmClearFields.join(", ")}</strong>.
              This is not reversible.
            </p>
            <p className="text-sm mb-3">
              Type <strong>YES</strong> to confirm.
            </p>
            <input
              className="border rounded w-full px-2 py-1"
              value={confirmInput}
              onChange={(e) => setConfirmInput(e.target.value)}
              placeholder="YES"
            />
            <div className="flex justify-end gap-2 mt-4">
              <button className="px-3 py-1 border rounded" onClick={() => setConfirmClearOpen(false)}>
                Cancel
              </button>
              <button
                className="px-3 py-1 border rounded bg-red-600 text-white"
                disabled={confirmInput !== "YES"}
                onClick={() => {
                  setEditClears((prev) => {
                    const next = new Set(prev);
                    confirmClearFields.forEach((f) => next.add(f));
                    return next;
                  });
                  setEditLocal((prev) => {
                    const next = { ...prev };
                    confirmClearFields.forEach((f) => {
                      next[f] = "";
                    });
                    return next;
                  });
                  setConfirmClearOpen(false);
                }}
              >
                Confirm
              </button>
            </div>
          </div>
        </div>
      )}
    </Tooltip.Provider>
  );
}
