//dbadmin-react-site/VideosTable.tsx v1.2.1 EDIT: moved kabab to left side with checkbox for mass edit and removed the kabab and checkbox columns from being changed in view
//dbadmin-react-site/VideosTable.tsx v1.2.1 ADDED: mass edit and delete capabilities
//dbadmin-react-site/VideosTable.tsx v1.2.0 CHANGE: removed 'Pending Schedule' and replaced 'Scheduled' with 'Post Ready' after successful Social Copy
//dbadmin-react-site/VideosTable.tsx v1.0.9 Adding Data Validation for status and videotype with dropdown.
//Adding timezone selection and friendly display of scheduled_at
//dbadmin-react-site/VideosTable.tsx v1.0.8 ADDED editing (UPSERT) capabilities to page//dbadmin-react-site/VideosTable.tsx v1.0.8 ADDED editing (UPSERT) capabilities to page
//dbadmin-react-site/VideosTable.tsx v1.0.7 ADDED column sorting capabilities
//dbadmin-react-site/VideosTable.tsx v1.0.6 ADDED proper pagination effects
//dbadmin-react-site/VideosTable.tsx v1.0.5 CHANGES: revised to use static dev validation when running in local dev mode; adjusted use dependency for copied cells to not re-fetch the whole table
//dbadmin-react-site/VideosTable.tsx v1.0.3 CHANGES: added pagination to show how many pages are availabe in UI
//dbadmin-react-site/VideosTable.tsx v1.0.2 CHANGES: sticky column headers on verticle scroll and horizontal scrollbar always visible
//dbadmin-react-site/VideosTable.tsx v1.0.1
import React, { useEffect, useState, useRef } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getPaginationRowModel,
  getFilteredRowModel,
  flexRender,
  ColumnDef,
  getSortedRowModel,
  type SortingState,
} from '@tanstack/react-table';
import * as Tooltip from '@radix-ui/react-tooltip';
import ColumnSelectorModal from './ColumnSelectorModal';

type HashtagMode = "replace" | "append" | "remove";
type BulkResult =
  | { selKey: string; ok: true }
  | { selKey: string; ok: false; err: string };


// ——— Hashtag helpers (module scope) ———
// Space-separated storage & display. We also accept commas/semicolons on input.
function parseHashtags(s: string): string[] {
  if (!s) return [];
  const tokens = s
    .split(/[,\s;]+/)                 // split on commas, any whitespace, or semicolons
    .map(t => t.trim())
    .filter(Boolean)
    .map(t => (t.startsWith("#") ? t : `#${t}`))
    .map(t => t.replace(/\s+/g, "")); // remove internal spaces inside a tag

  // de-dupe case-insensitively, preserve first seen
  const seen = new Set<string>();
  const out: string[] = [];
  for (const tok of tokens) {
    const key = tok.toLowerCase();
    if (!seen.has(key)) { seen.add(key); out.push(tok); }
  }
  return out;
}

function renderHashtags(tokens: string[]): string {
  // store/display space-separated tags
  return tokens.join(" ");
}

function applyHashtagMode(existing: string | null | undefined, entry: string, mode: HashtagMode): string {
  const current = parseHashtags(existing || "");
  const incoming = parseHashtags(entry || "");

  if (mode === "replace") {
    return renderHashtags(incoming);
  }

  if (mode === "append") {
    const map = new Map<string, string>(current.map(h => [h.toLowerCase(), h]));
    for (const inc of incoming) {
      const key = inc.toLowerCase();
      if (!map.has(key)) map.set(key, inc);
    }
    return renderHashtags(Array.from(map.values()));
  }

  // remove
  const removeSet = new Set(incoming.map(x => x.toLowerCase()));
  const kept = current.filter(h => !removeSet.has(h.toLowerCase()));
  return renderHashtags(kept);
}


// v1.0.8 ADD: editing helpers/consts
const EDITABLE_NO_CLEAR = new Set(["status", "video_type"]);
const EDITABLE_WITH_CLEAR = new Set([
  "scheduled_at", "social_copy_hook", "social_copy_body", "social_copy_cta", "hashtags"
]);

function isEditable(field: string) {
  return EDITABLE_NO_CLEAR.has(field) || EDITABLE_WITH_CLEAR.has(field);
}

function toLocalDatetimeInputValue(iso?: string | null): string {
  if (!iso) return "";
  const d = new Date(iso);
  const pad = (n: number) => n.toString().padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function fromLocalDatetimeInputValue(val: string): string | null {
  if (!val) return null;
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : d.toISOString();
}

type RecordType = { [key: string]: any };
const defaultVisible = ['title', 'status', 'scheduled_at'];
// Server default is record_modified DESC; mirror that in the UI:
const DEFAULT_SORT_COL = 'record_modified';
const DEFAULT_SORT: SortingState = [{ id: DEFAULT_SORT_COL, desc: true }];
// v1.0.9 ADD: enumerations + display helpers
const STATUS_OPTIONS = [
  "Post Ready",
  "Working",
  "Hold",
  "Pending Transcription",
] as const;

const VIDEO_TYPE_OPTIONS = [
  "Pivot Year",
  "Newsletter",
  "Other",
  "Unlisted",
] as const;

type StatusType = typeof STATUS_OPTIONS[number];
type VideoType = typeof VIDEO_TYPE_OPTIONS[number];

const statusPillClasses: Record<StatusType, string> = {
    "Post Ready": "bg-green-100 text-green-800 border-green-200",
  "Working": "bg-blue-100 text-blue-800 border-blue-200",
  "Hold": "bg-red-100 text-red-800 border-red-200",
  "Pending Transcription": "bg-yellow-100 text-yellow-900 border-yellow-200",
};

const videoTypePillClasses: Record<VideoType, string> = {
  "Pivot Year": "bg-orange-100 text-orange-800 border-orange-200",
  "Newsletter": "bg-green-100 text-green-800 border-green-200",
  "Other": "bg-purple-100 text-purple-800 border-purple-200",
  "Unlisted": "bg-red-100 text-red-800 border-red-200",
};

// Small pill badge component
function Pill({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return (
    <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium ${className}`}>
      {children}
    </span>
  );
}

// v1.0.9 ADD: timezone persistence + formatter
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

function formatLongLocal(iso?: string | null, tz?: string): string {
  if (!iso) return "-";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "-";

  const timeZone = tz || getDefaultTZ();
  // Example: Thursday, August 28, 2025 at 8:00 AM EDT
  const datePart = new Intl.DateTimeFormat(undefined, {
    weekday: "long",
    year: "numeric",
    month: "long",
    day: "numeric",
    timeZone,
  }).format(d);

  const timePart = new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone,
    timeZoneName: "short",
  }).format(d);

  // Remove comma before time if present in some locales, and join with "at"
  return `${datePart} at ${timePart}`;
}

// v1.0.9 REPLACE: curated timezone choices (1 per common zone)
type TzChoice = { id: string; descriptor: string };

const TZ_CHOICES: TzChoice[] = [
  { id: "UTC",                 descriptor: "UTC" },
  { id: "America/New_York",    descriptor: "Eastern (New York)" },
  { id: "America/Chicago",     descriptor: "Central (Chicago)" },
  { id: "America/Denver",      descriptor: "Mountain (Denver)" },
  { id: "America/Los_Angeles", descriptor: "Pacific (Los Angeles)" },
  { id: "America/Phoenix",     descriptor: "Arizona (Phoenix, no DST)" },
  { id: "America/Anchorage",   descriptor: "Alaska (Anchorage)" },
  { id: "Pacific/Honolulu",    descriptor: "Hawaii (Honolulu)" },
  { id: "Europe/London",       descriptor: "UK (London)" },
  { id: "Europe/Berlin",       descriptor: "Central Europe (Berlin)" },
  { id: "Europe/Moscow",       descriptor: "Moscow" },
  { id: "Asia/Kolkata",        descriptor: "India (Kolkata)" },
  { id: "Asia/Shanghai",       descriptor: "China (Shanghai)" },
  { id: "Asia/Tokyo",          descriptor: "Japan (Tokyo)" },
  { id: "Australia/Sydney",    descriptor: "Australia (Sydney)" },
];


// v1.0.9 ADD: TZ label + compact date/time formatting with DST-correct abbreviation
function getTzAbbr(tz: string, at: Date): string {
  try {
    const parts = new Intl.DateTimeFormat("en-US", { timeZone: tz, timeZoneName: "short" })
      .formatToParts(at);
    return parts.find(p => p.type === "timeZoneName")?.value || tz;
  } catch {
    return tz;
  }
}

// Try to use modern "shortOffset"; fallback to manual offset calc if unavailable
function getTzGmtOffset(tz: string, at: Date): string {
  try {
    const parts = new Intl.DateTimeFormat("en-US", { timeZone: tz, timeZoneName: "shortOffset", hour: "2-digit" })
      .formatToParts(at);
    const off = parts.find(p => p.type === "timeZoneName")?.value; // e.g. "GMT-4"
    if (off) return off.replace("UTC", "GMT");
  } catch {}
  // Fallback: compute offset minutes for `tz` at `at`
  try {
    const fmt = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz, hour12: false,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
    const parts = Object.fromEntries(
      fmt.formatToParts(at)
         .filter(p => ["year","month","day","hour","minute","second"].includes(p.type))
         .map(p => [p.type, p.value])
    ) as Record<string,string>;
    const localMs = Date.UTC(+parts.year, +parts.month - 1, +parts.day, +parts.hour, +parts.minute, +parts.second);
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
  const gmt  = getTzGmtOffset(choice.id, now);
  // e.g. "Eastern (New York) — EDT (GMT-04:00)"
  return `${choice.descriptor} — ${abbr} (${gmt})`;
}

function tzMenuLabel(tz: string): string {
  const now = new Date();
  const abbr = getTzAbbr(tz, now);
  const gmt = getTzGmtOffset(tz, now);
  return `${abbr} (${gmt}) — ${tz}`;
}

// Compact row display: "8:00 AM EDT, Mon., Sep 03, 2025"
function formatScheduledCompact(iso?: string | null, tz?: string): string {
  if (!iso) return "-";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "-";
  const zone = tz || getDefaultTZ();

  const timeWithTz = new Intl.DateTimeFormat(undefined, {
    hour: "numeric", minute: "2-digit", hour12: true,
    timeZone: zone, timeZoneName: "short",
  }).format(d); // e.g. "8:00 AM EDT"

  const wk = new Intl.DateTimeFormat(undefined, { weekday: "short", timeZone: zone }).format(d); // "Mon"
  const mo = new Intl.DateTimeFormat(undefined, { month: "short",  timeZone: zone }).format(d); // "Sep"
  const day = new Intl.DateTimeFormat(undefined, { day: "2-digit",  timeZone: zone }).format(d); // "03"
  const yr  = new Intl.DateTimeFormat(undefined, { year: "numeric", timeZone: zone }).format(d); // "2025"

  // Add a period after weekday abbreviation per your example
  const wkDot = /\.$/.test(wk) ? wk : `${wk}.`;
  return `${timeWithTz}, ${wkDot} ${mo} ${day}, ${yr}`;
}


export default function VideosTable() {
  const [data, setData] = useState<RecordType[]>([]);
  const [columns, setColumns] = useState<ColumnDef<RecordType>[]>([]);
  const [columnVisibility, setColumnVisibility] = useState<Record<string, boolean>>({});
  const [globalFilter, setGlobalFilter] = useState('');
  const [showColumnModal, setShowColumnModal] = useState(false);
  const [copiedCellId, setCopiedCellId] = useState<string | null>(null);
  const copiedCellIdRef = useRef<string | null>(null);
  const [sorting, setSorting] = useState<SortingState>(() => {
    try {
      return JSON.parse(localStorage.getItem('sorting') || '[]');
    } catch {
      return [];
    }
  });
  // v1.0.8 ADD: edit sheet state
  const [editOpen, setEditOpen] = useState(false);
  const [editRecord, setEditRecord] = useState<RecordType | null>(null);
  const [editLocal, setEditLocal] = useState<Record<string, any>>({});
  const [editClears, setEditClears] = useState<Set<string>>(new Set());

  // v1.0.8 ADD: confirmation modal for clears
  const [confirmClearOpen, setConfirmClearOpen] = useState(false);
  const [confirmClearFields, setConfirmClearFields] = useState<string[]>([]);
  const [confirmInput, setConfirmInput] = useState("");

  // v1.0.9 ADD: timezone state
  const [tz, setTz] = useState<string>(() => getDefaultTZ());
  useEffect(() => {
    try { localStorage.setItem("tz", tz); } catch {}
  }, [tz]);
  // v1.3.0 MASS EDIT: selection + bulk state
  const getRowKey = (r: RecordType): string => (r.id ? String(r.id) : String(r.title));
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());
  const [selectAllOnPage, setSelectAllOnPage] = useState(false);

  // Keep live copies for column renderers created once (sync each render)
  const selectedKeysRef = useRef<Set<string>>(new Set());
  selectedKeysRef.current = selectedKeys;

  const selectAllOnPageRef = useRef(false);
  selectAllOnPageRef.current = selectAllOnPage;


  // Bulk form state (only these fields are allowed)
  const [bulkOpen, setBulkOpen] = useState(false);
  const [bulkStatus, setBulkStatus] = useState<StatusType | "">("");
  const [bulkVideoType, setBulkVideoType] = useState<VideoType | "">("");
  const [bulkScheduledLocal, setBulkScheduledLocal] = useState<string>(""); // datetime-local
  const [bulkClearScheduled, setBulkClearScheduled] = useState<boolean>(false);

  const [bulkHashtags, setBulkHashtags] = useState<string>("");
  const [bulkHashtagsMode, setBulkHashtagsMode] = useState<HashtagMode>("replace");
  const [bulkClearHashtags, setBulkClearHashtags] = useState<boolean>(false);
  const [bulkHashtagsNoChange, setBulkHashtagsNoChange] = useState<boolean>(true); // ← NEW

  // Reset bulk inputs to safe defaults
  const resetBulkForm = React.useCallback(() => {
    setBulkStatus("");
    setBulkVideoType("");
    setBulkScheduledLocal("");
    setBulkClearScheduled(false);

    setBulkHashtags("");
    setBulkHashtagsMode("replace");
    setBulkClearHashtags(false);
    setBulkHashtagsNoChange(true);
  }, []);

  const prevSelSizeRef = useRef(0);
  useEffect(() => {
    const prev = prevSelSizeRef.current;
    const curr = selectedKeys.size;
    // when going from 0 selected -> >0 selected, reset the bulk form
    if (prev === 0 && curr > 0) resetBulkForm();
    prevSelSizeRef.current = curr;
  }, [selectedKeys.size, resetBulkForm]);

  // Confirmations
  const [confirmBulkSaveOpen, setConfirmBulkSaveOpen] = useState(false);
  const [confirmBulkDeleteOpen, setConfirmBulkDeleteOpen] = useState(false);
  const [confirmDeleteInput, setConfirmDeleteInput] = useState("");


  // If saved/browser tz isn't in curated list, include it at the top so it appears selected.
  const effectiveChoices: TzChoice[] = React.useMemo(() => {
    if (TZ_CHOICES.some(c => c.id === tz)) return TZ_CHOICES;
    const pretty = tz.includes("/") ? tz.split("/").pop()!.replace(/_/g, " ") : tz;
    return [{ id: tz, descriptor: pretty }, ...TZ_CHOICES];
  }, [tz]);

  useEffect(() => {
    if (!columns.length || !sorting.length) return;
    const colIds = new Set(
      columns.map(c => ((c as any).id as string) ?? ((c as any).accessorKey as string))
    );
    const [first, ...rest] = sorting;
    if (first && !colIds.has(first.id)) {
      setSorting(rest.length ? rest.filter(s => colIds.has(s.id)) : DEFAULT_SORT);
    }
  }, [columns, sorting]);

  useEffect(() => { copiedCellIdRef.current = copiedCellId; }, [copiedCellId]);
  const copyTimerRef = useRef<number | null>(null);
  const [pagination, setPagination] = useState({
    pageIndex: 0,
    pageSize: 25,
  });


  useEffect(() => {
    if (Object.keys(columnVisibility).length) {
      localStorage.setItem('columnVisibility', JSON.stringify(columnVisibility));
    }
  }, [columnVisibility]);

  // v1.3.0: Exit bulk mode & clear selections on filter/sort/page changes
  useEffect(() => {
    if (selectedKeys.size) {
      setSelectedKeys(new Set());
      setSelectAllOnPage(false);
      setBulkOpen(false);
    }
  }, [globalFilter, sorting, pagination.pageIndex, pagination.pageSize]);

  useEffect(() => {
    (async () => {
    
        const API_BASE = import.meta.env.DEV ? '' : 'https://admin.gr8r.com';
        const res = await fetch(`${API_BASE}/db1/videos`, {
            credentials: import.meta.env.DEV ? 'same-origin' : 'include',
        });

        const records = await res.json();
      if (records.length) {
        setData(records);
        const sample = records[0];
        const allKeys = Object.keys(sample);
        const savedVisibility = JSON.parse(localStorage.getItem('columnVisibility') || 'null');

        let visibility = Object.fromEntries(
          allKeys.map(k => [k, savedVisibility?.[k] ?? defaultVisible.includes(k)])
        );
        // Ensure the default-sorted column is visible if present
        if (allKeys.includes(DEFAULT_SORT_COL) && visibility[DEFAULT_SORT_COL] === false) {
          visibility = { ...visibility, [DEFAULT_SORT_COL]: true };
        }
        setColumnVisibility(visibility);

        setColumns(
        (() => {
          const baseCols = allKeys.map((key) => ({
            accessorKey: key,
            enableSorting: true,
            header: ({ column }) => {
          const dir = column.getIsSorted();
          // v1.3.0: compute page row keys (later used by header checkbox)
          const pageRowKeys = () => table?.getRowModel().rows.map(r => getRowKey(r.original)) ?? [];

              return (
                <button
                  type="button"
                  onClick={column.getToggleSortingHandler()}
                  className="flex items-center gap-1 cursor-pointer select-none hover:underline"
                  aria-sort={dir === 'asc' ? 'ascending' : dir === 'desc' ? 'descending' : 'none'}
                  title="Click to sort"
                >
                  {key}
                  <span className="inline-block w-3 text-xs">
                    {dir === 'asc' ? '▲' : dir === 'desc' ? '▼' : ''}
                  </span>
                </button>
              );
            },

            cell: (info) => {
              const val = info.getValue();
              const cellId = `${info.row.id}-${key}`;
               // v1.0.9: special renderers
              if (key === "status" && typeof val === "string" && (STATUS_OPTIONS as readonly string[]).includes(val)) {
                const cls = statusPillClasses[val as StatusType];
                return <Pill className={cls}>{val}</Pill>;
              }
              if (key === "video_type" && typeof val === "string" && (VIDEO_TYPE_OPTIONS as readonly string[]).includes(val)) {
                const cls = videoTypePillClasses[val as VideoType];
                return <Pill className={cls}>{val}</Pill>;
              }
              if (key === "scheduled_at") {
                return (
                  <div className="truncate max-w-[360px]">
                    {formatScheduledCompact(val as string | null, tz)}
                  </div>
                );
              }
              const display = val?.toString() || '-';
              const isCopied = copiedCellIdRef.current === cellId;

              return (
                <Tooltip.Root delayDuration={200} open={isCopied || undefined}>
                  <Tooltip.Trigger asChild>
                    <div
                      className={`truncate max-w-[200px] cursor-pointer px-1 ${
                        isCopied ? 'bg-orange-100 transition duration-300' : ''
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
                        isCopied ? 'bg-orange-600 border-orange-600' : 'bg-white border-2 border-[#003E24]'
                      }`}
                    >
                      {isCopied ? (
                        <span className="text-green-200 font-semibold">Copied!</span>
                      ) : (
                        <>
                          <div className="text-gray-400 text-[10px] pb-1">Clicking will copy contents to clipboard</div>
                          <div className="text-[#003E24]">{display}</div>
                        </>
                      )}
                      <Tooltip.Arrow className={isCopied ? 'fill-orange-600' : 'fill-[#003E24]'} />
                    </Tooltip.Content>
                  </Tooltip.Portal>
                </Tooltip.Root>
              );
            },
          }));

          // v1.0.8 ADD: kebab actions column (sticky right)
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
                aria-label={`Edit ${rec.title ?? ''}`}
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
// v1.3.0: selection checkbox column (sticky left)
const selectCol: ColumnDef<RecordType> = {
  id: "_select",
  header: () => {
  // Compute dynamically so it reflects current selection
  const pageKeys = table.getRowModel().rows.map(r => getRowKey(r.original));
  const allSelectedThisPage =
    pageKeys.length > 0 && pageKeys.every(k => selectedKeysRef.current.has(k));

  return (
    <input
      type="checkbox"
      aria-label="Select all on page"
      checked={allSelectedThisPage}
      onChange={(e) => {
        const checked = e.target.checked;
        setSelectedKeys((prev) => {
          const next = new Set(prev);
          if (checked) {
            pageKeys.forEach(k => next.add(k));
          } else {
            pageKeys.forEach(k => next.delete(k));
          }
          // open/close bulk bar
          setBulkOpen(next.size > 0);
          // keep the convenience flag in sync (not strictly necessary now)
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
            if (isChecked) {
              next.add(key);
            } else {
              next.delete(key);
            }
            setBulkOpen(next.size > 0);

            // keep the "select all on page" header in sync with current page
            const pageKeys = table.getRowModel().rows.map(r => getRowKey(r.original));
            const allSelectedThisPage =
              pageKeys.length > 0 && pageKeys.every(k => next.has(k));
            setSelectAllOnPage(allSelectedThisPage);

            return next;
          });
        }}       
      />
    );
  },
};

// Left to right: kebab on far left, then checkbox, then the rest
return [kebabCol, selectCol, ...baseCols];

        })()
      );

       // Apply default only if initial sorting is empty
        if (!sorting.length && allKeys.includes(DEFAULT_SORT_COL)) {
          setSorting(DEFAULT_SORT);
        }
      }
    })();
  }, []);
        useEffect(() => {
          const id = sorting[0]?.id as string | undefined;
          if (!id || !columns.length) return;

          const hasCol = columns.some((c) => (c as any).accessorKey === id || (c as any).id === id);
          if (!hasCol) return;

          setColumnVisibility((prev) => {
            if (prev && prev[id] === false) {
              return { ...prev, [id]: true };
            }
            return prev;
          });
        }, [sorting, columns]);
        useEffect(() => {
          setPagination((prev) => ({ ...prev, pageIndex: 0 }));
        }, [globalFilter]);
        useEffect(() => {
          setPagination((prev) => ({ ...prev, pageIndex: 0 }));
        }, [sorting]);
        useEffect(() => localStorage.setItem('sorting', JSON.stringify(sorting)), [sorting]);
        // ADDED: cleanup any pending copy timeout on unmount
        useEffect(() => {
            return () => {
              if (copyTimerRef.current) {
                window.clearTimeout(copyTimerRef.current);
              }
            };
          }, []);

          // v1.0.8 ADD
        function visibleEditableFieldsFor(record: RecordType) {
          // If visibility hasn't been initialized yet, fall back to defaultVisible
          const hasVisibility = Object.keys(columnVisibility).length > 0;

          const visibleKeys = hasVisibility
            ? Object.keys(columnVisibility).filter((k) => columnVisibility[k])
            : Object.keys(record).filter((k) => defaultVisible.includes(k) || isEditable(k));

          return visibleKeys.filter((k) => isEditable(k) && k in record);
        }

        function openEditSheet(record: RecordType) {
          let fields = visibleEditableFieldsFor(record);
          if (!fields.length) {
            // Fallback to all editable keys present on the record
            fields = Object.keys(record).filter((k) => isEditable(k));
          }

          const start: Record<string, any> = {};
          fields.forEach((k) => {
            if (k === "scheduled_at") {
              start[k] = toLocalDatetimeInputValue(record[k]);
            } else {
              const v = record[k];
              start[k] = (v === null || v === undefined) ? "" : String(v);
            }
          });

          setEditRecord(record);
          setEditLocal(start);
          setEditClears(new Set());
          setEditOpen(true);
        }

        async function savePartial(body: Record<string, any>) {
          const API_BASE = import.meta.env.DEV ? "" : "https://admin.gr8r.com";
          const res = await fetch(`${API_BASE}/db1/videos`, {
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

  // Build a map for quick row lookup
  const byKey = new Map<string, RecordType>();
  data.forEach(r => byKey.set(getRowKey(r), r));

  // Prepare optimistic copy
  const originalData = data.slice();
  const nowISO = new Date().toISOString();

  // Compute per-record payloads
  const jobs: Promise<BulkResult>[] = Array.from(selectedKeys).map(async (selKey: string) => {
    const rec = byKey.get(selKey);
    if (!rec) return { selKey, ok: false as const, err: "Missing record in view" };

    const payload: Record<string, any> = {};
    // identify row
    if (rec.id != null) payload.id = rec.id;
    else payload.title = rec.title;

    // status (no clear)
    if (bulkStatus) payload.status = bulkStatus;
    // video_type (no clear)
    if (bulkVideoType) payload.video_type = bulkVideoType;

    // scheduled_at (clear or set)
    if (bulkClearScheduled) {
      payload.clears = [...(payload.clears ?? []), "scheduled_at"];
    } else if (bulkScheduledLocal) {
      const iso = fromLocalDatetimeInputValue(bulkScheduledLocal);
      if (iso) payload.scheduled_at = iso;
    }

    
    // hashtags (no-change guard; else clear or transform)
    if (!bulkHashtagsNoChange) {
      if (bulkClearHashtags) {
        payload.clears = [...(payload.clears ?? []), "hashtags"];
      } else if (bulkHashtags.trim()) {
        const nextTags = applyHashtagMode(rec.hashtags ?? "", bulkHashtags, bulkHashtagsMode);
        payload.hashtags = nextTags;
      }
    }

    // nothing to change? (skip this row)
    const keysToCheck = Object.keys(payload).filter(k => k !== "id" && k !== "title");
    const isNoop = keysToCheck.length === 0;
    if (isNoop) return { selKey, ok: true as const }; // nothing to do

    // optimistic UI
    setData(prev => prev.map(r => {
      if (getRowKey(r) !== selKey) return r;
      const next = { ...r };
      if (payload.status) next.status = payload.status;
      if (payload.video_type) next.video_type = payload.video_type;
      if (payload.scheduled_at) next.scheduled_at = payload.scheduled_at;
      if (payload.hashtags != null) next.hashtags = payload.hashtags;
      if (payload.clears?.includes("scheduled_at")) next.scheduled_at = null;
      if (payload.clears?.includes("hashtags")) next.hashtags = null;
      next.record_modified = nowISO;
      return next;
    }));

    const res = await savePartial(payload);
    if (!res.ok) {
      // rollback this one
      setData(prev => prev.map(r => (getRowKey(r) === selKey ? byKey.get(selKey)! : r)));
      return { selKey, ok: false as const, err: res.err };
    }
    return { selKey, ok: true as const };
  });

  const results = await Promise.all(jobs);

  // Narrow to the failure shape explicitly
  function isBulkFailure(r: BulkResult): r is { selKey: string; ok: false; err: string } {
    return r.ok === false;
  }
  const failures = results.filter(isBulkFailure);

  if (failures.length) {
    alert(
      `Some rows failed to save (${failures.length}/${count}).\n` +
      failures.slice(0, 5).map(f => `• ${f.selKey}: ${f.err}`).join("\n") +
      (failures.length > 5 ? `\n…and ${failures.length - 5} more` : "")
    );
  }

  // Done: clear selection + bar
  setSelectedKeys(new Set());
  setSelectAllOnPage(false);
  setBulkOpen(false);
}

async function bulkDeleteSelected() {
  const count = selectedKeys.size;
  if (!count) return;

  const keys = Array.from(selectedKeys);
  const toSend = keys.map(k => {
    const rec = data.find(r => getRowKey(r) === k);
    if (!rec) return null;
    if (rec.id != null) return { id: rec.id };
    return { title: rec.title };
  }).filter(Boolean) as Array<{id?: string|number; title?: string}>;

  const API_BASE = import.meta.env.DEV ? "" : "https://admin.gr8r.com";
  const res = await fetch(`${API_BASE}/db1/videos`, {
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

  // optimistic remove
  setData(prev => prev.filter(r => !selectedKeys.has(getRowKey(r))));
  setSelectedKeys(new Set());
  setSelectAllOnPage(false);
  setBulkOpen(false);
}

  const handleResetColumns = () => {
    const reset = Object.fromEntries(
      columns.map(c => [c.accessorKey as string, defaultVisible.includes(c.accessorKey as string)])
    );
    const activeSortId = (sorting[0]?.id as string) ?? DEFAULT_SORT_COL;
    if (activeSortId in reset) {
      reset[activeSortId] = true; // ensure the active sorted column is shown
    }
    setColumnVisibility(reset);
    localStorage.setItem('columnVisibility', JSON.stringify(reset));
  };
  const table = useReactTable<RecordType>({
    data,
    columns,
    state: {
      columnVisibility,
      globalFilter,
      pagination,
      sorting,
    },
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

  // ADDED (Batch4): derived display values for totals/page counts
  const totalRows = table.getFilteredRowModel().rows.length;
  const pageIndex = table.getState().pagination.pageIndex;
  const pageSize = table.getState().pagination.pageSize;
  const pageCount = table.getPageCount();
  const firstRow = totalRows === 0 ? 0 : pageIndex * pageSize + 1;
  const lastRow = totalRows === 0 ? 0 : Math.min(totalRows, (pageIndex + 1) * pageSize);
  const currentSort = table.getState().sorting?.[0];
  const sortedByLabel = currentSort?.id ?? DEFAULT_SORT_COL;
  const sortedDirArrow = currentSort ? (currentSort.desc ? '↓' : '↑') : '↓';

  return (
    <Tooltip.Provider>
      <div className="overflow-x-auto">
        <ColumnSelectorModal
          isOpen={showColumnModal}
          onClose={() => setShowColumnModal(false)}
          onReset={handleResetColumns}
          columns={table.getAllLeafColumns().filter(c => c.id !== "_select" && c.id !== "_actions")}
          title="Edit Visible Columns"
        />

      <div className="mb-2 flex flex-wrap items-center gap-4">
        <input
          type="text"
          className="border px-2 py-1 rounded"
          placeholder="Search..."
          value={globalFilter ?? ''}
          onChange={(e) => setGlobalFilter(e.target.value)}
        />

      {/* ADDED (Batch4): page size dropdown */}
      <div className="flex items-center gap-2">
        <label htmlFor="pageSize" className="text-sm">Rows per page</label>
        <select
          id="pageSize"
          className="border px-2 py-1 rounded text-sm"
          value={pageSize}
          onChange={(e) => table.setPageSize(Number(e.target.value))}
        >
          {[10, 25, 50, 100].map(size => (
            <option key={size} value={size}>{size}</option>
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

      <button
        className="border px-2 py-1 rounded text-sm"
        onClick={() => setShowColumnModal(true)}
      >
        Change Viewable Fields
      </button>

      {/* v1.0.9 ADD: Timezone selector */}
      <div className="flex items-center gap-2">
        <label htmlFor="tz" className="text-sm">Time zone</label>
        <select
          id="tz"
          className="border px-2 py-1 rounded text-sm max-w-[420px]"
          value={tz}
          onChange={(e) => setTz(e.target.value)}
        >
          {effectiveChoices.map(z => (
            <option key={z.id} value={z.id}>{tzMenuLabelFriendly(z)}</option>
          ))}
        </select>
      </div>

      {/* ADDED (Batch4): totals (post-filter) */}
      <div className="text-sm">
        {totalRows === 0 ? '0 results' : `Showing ${firstRow}–${lastRow} of ${totalRows}`}
      </div>
    </div>
    {/* v1.3.0: Bulk Edit bar (sticky top, outside scroller) */}
    {bulkOpen && selectedKeys.size > 0 && (
      <div className="sticky top-0 z-40 border-b bg-white">
        <div className="flex flex-wrap items-end gap-3 p-2">
          <div className="text-sm mr-3">
            <strong>{selectedKeys.size}</strong> selected
          </div>

          {/* Status */}
          <div>
            <label className="block text-xs">Status</label>
            <div className="flex gap-1 flex-wrap">
              {STATUS_OPTIONS.map(opt => (
                <button
                  key={opt}
                  type="button"
                  className={`rounded-full border px-2 py-0.5 text-xs ${bulkStatus === opt ? 'ring-2 ring-offset-1' : ''} ${statusPillClasses[opt]}`}
                  onClick={() => setBulkStatus(prev => prev === opt ? "" : opt)}
                  title={opt}
                >
                  {opt}
                </button>
              ))}
            </div>
          </div>

          {/* Video Type */}
          <div>
            <label className="block text-xs">Video type</label>
            <div className="flex gap-1 flex-wrap">
              {VIDEO_TYPE_OPTIONS.map(opt => (
                <button
                  key={opt}
                  type="button"
                  className={`rounded-full border px-2 py-0.5 text-xs ${bulkVideoType === opt ? 'ring-2 ring-offset-1' : ''} ${videoTypePillClasses[opt]}`}
                  onClick={() => setBulkVideoType(prev => prev === opt ? "" : opt)}
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

          {/* Hashtags with mode */}
          <div className="min-w-[280px]">
            <label className="block text-xs">Hashtags</label>

            <div className="flex items-center gap-3 text-xs mb-1">
              <label className="inline-flex items-center gap-1">
                <input
                  type="radio"
                  name="hashtagsMode"
                  checked={bulkHashtagsNoChange}
                  onChange={() => setBulkHashtagsNoChange(true)}
                />
                No change
              </label>
              <label className="inline-flex items-center gap-1">
                <input
                  type="radio"
                  name="hashtagsMode"
                  checked={!bulkHashtagsNoChange && bulkHashtagsMode === "replace"}
                  onChange={() => { setBulkHashtagsNoChange(false); setBulkHashtagsMode("replace"); }}
                />
                Replace All
              </label>
              <label className="inline-flex items-center gap-1">
                <input
                  type="radio"
                  name="hashtagsMode"
                  checked={!bulkHashtagsNoChange && bulkHashtagsMode === "append"}
                  onChange={() => { setBulkHashtagsNoChange(false); setBulkHashtagsMode("append"); }}
                />
                Append
              </label>
              <label className="inline-flex items-center gap-1">
                <input
                  type="radio"
                  name="hashtagsMode"
                  checked={!bulkHashtagsNoChange && bulkHashtagsMode === "remove"}
                  onChange={() => { setBulkHashtagsNoChange(false); setBulkHashtagsMode("remove"); }}
                />
                Remove
              </label>
            </div>

            <textarea
              className="border rounded px-2 py-1 text-sm w-full min-h-10 disabled:bg-gray-50"
              placeholder="#growth #mindset"        // ← space separated
              value={bulkHashtags}
              disabled={bulkHashtagsNoChange}
              onChange={(e) => {
                setBulkHashtags(e.target.value);
                setBulkClearHashtags(false);
              }}
            />

            <div className="flex items-center gap-2 mt-1">
              <label className="text-xs inline-flex items-center gap-1">
                <input
                  type="checkbox"
                  checked={bulkClearHashtags}
                  disabled={bulkHashtagsNoChange}
                  onChange={(e) => {
                    setBulkClearHashtags(e.target.checked);
                    if (e.target.checked) setBulkHashtags("");
                  }}
                />
                Clear
              </label>
            </div>
          </div>

          {/* Actions */}
          <div className="ml-auto flex items-center gap-2">
            <button
              className="px-3 py-1 border rounded"
              onClick={() => {
                setSelectedKeys(new Set());
                setSelectAllOnPage(false);
                setBulkOpen(false);
              }}
            >
              Cancel
            </button>
            <button
              className="px-3 py-1 border rounded bg-[#003E24] text-white"
              onClick={() => setConfirmBulkSaveOpen(true)}
                disabled={
                  !bulkStatus &&
                  !bulkVideoType &&
                  !bulkScheduledLocal &&
                  !bulkClearScheduled &&
                  // hashtags only count as a change if NOT "No change" and either clear or text provided
                  (bulkHashtagsNoChange || (!bulkClearHashtags && !bulkHashtags.trim()))
                }
              title="Apply to selected"
            >
              Save to {selectedKeys.size} rows
            </button>
            <button
              className="px-3 py-1 border rounded bg-red-600 text-white"
              onClick={() => setConfirmBulkDeleteOpen(true)}
            >
              Delete {selectedKeys.size}
            </button>
            <button
              className="px-3 py-1 border rounded"
              onClick={resetBulkForm}
              title="Reset all bulk edit inputs to safe defaults"
            >
              Reset form
            </button>
          </div>
        </div>
      </div>
    )}

        <div
          className="overflow-auto max-h-[calc(100vh-200px)]"
          style={bulkOpen && selectedKeys.size > 0 ? { paddingTop: 56 } : undefined} // ~bar height
        >

          <table className="min-w-[1000px] divide-y divide-gray-300 text-sm table-fixed">
          <thead className="bg-gray-50">
            {table.getHeaderGroups().map(headerGroup => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map(header => (
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
            {table.getRowModel().rows.map(row => (
              <tr
                key={row.id}
                onContextMenu={(e) => {
                  e.preventDefault();
                  openEditSheet(row.original as RecordType);
                }}
              >
                {row.getVisibleCells().map(cell => (
                  <td
                    key={cell.id}
                    className={
                      "px-2 py-1 whitespace-nowrap " +
                      (cell.column.id === "scheduled_at"
                        ? "w-[360px] min-w-[360px] max-w-[360px]"     // wider only for scheduled_at
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

        <div className="mt-4 flex flex-wrap items-center gap-4">
          <div className="text-sm">
            Page {pageCount === 0 ? 0 : pageIndex + 1} of {pageCount}
          </div>

          <div className="flex items-center gap-2">
            <button
              className="px-2 py-1 border rounded"
              onClick={() => table.setPageIndex(0)}
              disabled={!table.getCanPreviousPage()}
            >
              ⏮ First
            </button>
            <button
              className="px-2 py-1 border rounded"
              onClick={() => table.previousPage()}
              disabled={!table.getCanPreviousPage()}
            >
              Previous
            </button>
            <button
              className="px-2 py-1 border rounded"
              onClick={() => table.nextPage()}
              disabled={!table.getCanNextPage()}
            >
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
      {/* v1.1.0 ADD: Edit bottom sheet */}
      {editOpen && editRecord && (
        <div className="fixed inset-0 z-50 bg-black/40 flex items-end" onClick={() => setEditOpen(false)}>
          <div className="w-full bg-white rounded-t-2xl p-4 max-h-[85vh] overflow-auto" onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center justify-between">
              <div className="font-semibold text-lg">Edit: {editRecord.title}</div>
              <button className="text-2xl leading-none" onClick={() => setEditOpen(false)}>×</button>
            </div>

            {/* fields */}
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
                            className={`rounded-full border px-3 py-1 text-xs font-medium ${cls} ${active ? 'ring-2 ring-offset-1' : 'opacity-80 hover:opacity-100'}`}
                            onClick={() => {
                              setEditLocal((s) => ({ ...s, [field]: opt }));
                            }}
                          >
                            {opt}
                          </button>
                        );
                      })}
                    </div>
                  ) : field === "video_type" ? (
                    <div className="flex flex-wrap gap-2">
                      {VIDEO_TYPE_OPTIONS.map((opt) => {
                        const active = (editLocal[field] ?? "") === opt;
                        const cls = videoTypePillClasses[opt];
                        return (
                          <button
                            key={opt}
                            type="button"
                            className={`rounded-full border px-3 py-1 text-xs font-medium ${cls} ${active ? 'ring-2 ring-offset-1' : 'opacity-80 hover:opacity-100'}`}
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
                    {canClear && (
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
                    )}
                    {!canClear && (
                      <span className="text-xs text-gray-500">Clearing disabled</span>
                    )}
                  </div>
                </div>
              );
            })}

            <div className="flex justify-end gap-2 mt-6">
              <button className="px-3 py-1 border rounded" onClick={() => setEditOpen(false)}>Cancel</button>
              <button
                className="px-3 py-1 border rounded bg-[#003E24] text-white"
                onClick={async () => {
                  if (!editRecord) return;

                  // Build diff payload
                  const payload: Record<string, any> = { title: editRecord.title };
                  const clears: string[] = [];

                  for (const field of visibleEditableFieldsFor(editRecord)) {
                    const original = editRecord[field];
                    if (EDITABLE_WITH_CLEAR.has(field) && editClears.has(field)) {
                      clears.push(field);
                      continue;
                    }
                    const nextVal = editLocal[field];

                    if (field === "scheduled_at") {
                      // Convert datetime-local -> ISO for comparison/save
                      const nextISO = fromLocalDatetimeInputValue(nextVal);
                      const origISO = original ?? null;
                      if (nextISO !== origISO) {
                        // If user blanked it without pressing Clear, treat as "no change"
                        if (nextISO) payload[field] = nextISO;
                      }
                    } else {
                      const normalizedNext = (nextVal ?? "").toString();
                      const normalizedOrig = (original ?? "").toString();
                      if (normalizedNext !== normalizedOrig) {
                        // For no-clear fields, empty string means empty string (allowed)
                        payload[field] = normalizedNext;
                      }
                    }
                  }

                  if (clears.length) payload.clears = clears;

                  // nothing changed?
                  if (Object.keys(payload).length === 1 && !payload.clears) {
                    setEditOpen(false);
                    return;
                  }

                  // optimistic UI
                  const before = editRecord;
                  setData((prev) =>
                    prev.map((r) => {
                      if (r.title !== editRecord.title) return r;
                      const next = { ...r, ...payload };
                      // Apply clears optimistically
                      if (clears.length) {
                        clears.forEach((f) => { next[f] = null; });
                      }
                      next.record_modified = new Date().toISOString();
                      return next;
                    })
                  );

                  const res = await savePartial(payload);
                  if (!res.ok) {
                    // rollback
                    setData((prev) => prev.map((r) => (r.title === before.title ? before : r)));
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

      {/* v1.0.8 ADD: Confirm-clear modal (typed YES) */}
      {confirmClearOpen && (
        <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center" onClick={() => setConfirmClearOpen(false)}>
          <div className="bg-white rounded-xl shadow-xl p-4 w-full max-w-md" onClick={(e) => e.stopPropagation()}>
            <div className="font-semibold text-lg mb-2">Confirm Clear</div>
            <p className="text-sm mb-2">
              This will permanently clear: <strong>{confirmClearFields.join(", ")}</strong>.
              This is not reversible.
            </p>
            <p className="text-sm mb-3">Type <strong>YES</strong> to confirm.</p>
            <input
              className="border rounded w-full px-2 py-1"
              value={confirmInput}
              onChange={(e) => setConfirmInput(e.target.value)}
              placeholder="YES"
            />
            <div className="flex justify-end gap-2 mt-4">
              <button className="px-3 py-1 border rounded" onClick={() => setConfirmClearOpen(false)}>Cancel</button>
              <button
                className="px-3 py-1 border rounded bg-red-600 text-white"
                disabled={confirmInput !== "YES"}
                onClick={() => {
                  // Mark fields as cleared and blank their UI value
                  setEditClears((prev) => {
                    const next = new Set(prev);
                    confirmClearFields.forEach((f) => next.add(f));
                    return next;
                  });
                  setEditLocal((prev) => {
                    const next = { ...prev };
                    confirmClearFields.forEach((f) => { next[f] = ""; });
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
      {/* v1.3.0: Confirm bulk save */}
      {confirmBulkSaveOpen && (
        <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center" onClick={() => setConfirmBulkSaveOpen(false)}>
          <div className="bg-white rounded-xl shadow-xl p-4 w-full max-w-md" onClick={(e) => e.stopPropagation()}>
            <div className="font-semibold text-lg mb-2">Confirm Bulk Save</div>
            <p className="text-sm mb-3">
              You are about to modify <strong>{selectedKeys.size}</strong> records. Are you sure you wish to proceed?
            </p>
            <div className="flex justify-end gap-2">
              <button className="px-3 py-1 border rounded" onClick={() => setConfirmBulkSaveOpen(false)}>No</button>
              <button
                className="px-3 py-1 border rounded bg-[#003E24] text-white"
                onClick={async () => {
                  setConfirmBulkSaveOpen(false);
                  await bulkSaveSelected();
                }}
              >
                Yes, modify
              </button>
            </div>
          </div>
        </div>
      )}

      {/* v1.3.0: Confirm bulk delete (typed YES) */}
      {confirmBulkDeleteOpen && (
        <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center" onClick={() => setConfirmBulkDeleteOpen(false)}>
          <div className="bg-white rounded-xl shadow-xl p-4 w-full max-w-md" onClick={(e) => e.stopPropagation()}>
            <div className="font-semibold text-lg mb-2">Confirm Delete</div>
            <p className="text-sm mb-1">
              You are about to delete <strong>{selectedKeys.size}</strong> records. This is not reversible.
            </p>
            <p className="text-sm mb-3">Type <strong>YES</strong> to continue.</p>
            <input
              className="border rounded w-full px-2 py-1"
              value={confirmDeleteInput}
              onChange={(e) => setConfirmDeleteInput(e.target.value)}
              placeholder="YES"
            />
            <div className="flex justify-end gap-2 mt-3">
              <button className="px-3 py-1 border rounded" onClick={() => setConfirmBulkDeleteOpen(false)}>Cancel</button>
              <button
                className="px-3 py-1 border rounded bg-red-600 text-white"
                disabled={confirmDeleteInput !== "YES"}
                onClick={async () => {
                  setConfirmBulkDeleteOpen(false);
                  setConfirmDeleteInput("");
                  await bulkDeleteSelected();
                }}
              >
                Confirm Delete
              </button>
            </div>
          </div>
        </div>
      )}

    </Tooltip.Provider>
  );
}