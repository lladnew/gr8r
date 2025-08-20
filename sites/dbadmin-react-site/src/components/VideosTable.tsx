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
          allKeys.map((key) => ({
            accessorKey: key,
            enableSorting: true,
            header: ({ column }) => {
              const dir = column.getIsSorted(); // 'asc' | 'desc' | false
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
                        isCopied
                          ? 'bg-orange-600 border-orange-600'
                          : 'bg-white border-2 border-[#003E24]'
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
          }))
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
                    className="px-2 py-1 text-left whitespace-nowrap sticky top-0 z-10 bg-white"
                  >
                    {flexRender(header.column.columnDef.header, header.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {table.getRowModel().rows.map(row => (
              <tr key={row.id}>
                {row.getVisibleCells().map(cell => (
                  <td
                    key={cell.id}
                    className="px-2 py-1 whitespace-nowrap overflow-hidden text-ellipsis max-w-[200px]"
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
    </Tooltip.Provider>
  );
}
