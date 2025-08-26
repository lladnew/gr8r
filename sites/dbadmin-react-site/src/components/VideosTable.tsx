//dbadmin-react-site/VideosTable.tsx v1.0.8 ADDED editing (UPSERT) capabilities to page
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
            // ⬇️ keep your existing left-click copy cell body unchanged:
            cell: (info) => {
              const val = info.getValue();
              const cellId = `${info.row.id}-${key}`;
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
        header: () => <div className="w-[56px]" aria-hidden />,
        enableSorting: false,
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

          return [...baseCols, kebabCol];
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
          columns={table.getAllLeafColumns()}
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

      {/* ADDED (Batch4): totals (post-filter) */}
      <div className="text-sm">
        {totalRows === 0 ? '0 results' : `Showing ${firstRow}–${lastRow} of ${totalRows}`}
      </div>
    </div>

        <div className="overflow-auto max-h-[calc(100vh-200px)]">
          <table className="min-w-[1000px] divide-y divide-gray-300 text-sm table-fixed">
          <thead className="bg-gray-50">
            {table.getHeaderGroups().map(headerGroup => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map(header => (
                  <th
                    key={header.id}
                    className={
                      "px-2 py-1 text-left whitespace-nowrap sticky top-0 z-10 bg-white " +
                      (header.column.id === "_actions"
                        ? "right-0 z-20 shadow-[inset_8px_0_8px_-8px_rgba(0,0,0,0.08)]"
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
                      "px-2 py-1 whitespace-nowrap overflow-hidden text-ellipsis max-w-[200px] " +
                      (cell.column.id === "_actions"
                        ? "sticky right-0 z-10 bg-white w-[56px] min-w-[56px] max-w-[56px] shadow-[inset_8px_0_8px_-8px_rgba(0,0,0,0.08)]"
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
                        // editing cancels any pending clear
                        setEditClears((prev) => {
                          const next = new Set(prev);
                          next.delete(field);
                          return next;
                        });
                      }}
                    />
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

      {/* v1.1.0 ADD: Confirm-clear modal (typed YES) */}
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
    </Tooltip.Provider>
  );
}
