//dbadmin-react-site/VideosTable.tsx v1.0.6 CHANGES (Batch4): 
// - Reset to page 0 when globalFilter changes
// - Added derived totals/page counts
// - Added page size dropdown (10/25/50/100)
// - Show "Page X of Y" with First/Last buttons
// - Show "Showing A–B of N" total matching rows//dbadmin-react-site/src/components/VideosTable.tsx v1.0.5 CHANGES: revised to use static dev validation when running in local dev mode; adjusted use dependency for copied cells to not re-fetch the whole table
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
} from '@tanstack/react-table';
import * as Tooltip from '@radix-ui/react-tooltip';
import ColumnSelectorModal from './ColumnSelectorModal';

type RecordType = { [key: string]: any };
const defaultVisible = ['title', 'status', 'scheduled_at'];

export default function VideosTable() {
  const [data, setData] = useState<RecordType[]>([]);
  const [columns, setColumns] = useState<ColumnDef<RecordType>[]>([]);
  const [columnVisibility, setColumnVisibility] = useState<Record<string, boolean>>({});
  const [globalFilter, setGlobalFilter] = useState('');
  const [showColumnModal, setShowColumnModal] = useState(false);
  const [copiedCellId, setCopiedCellId] = useState<string | null>(null);
  const copiedCellIdRef = useRef<string | null>(null);
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

        const visibility = Object.fromEntries(
          allKeys.map(k => [k, savedVisibility?.[k] ?? defaultVisible.includes(k)])
        );
        setColumnVisibility(visibility);

        setColumns(
          allKeys.map((key) => ({
            accessorKey: key,
            header: key,
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
                        console.log('📋 Copied cellId:', cellId);
                        navigator.clipboard.writeText(display);
                        console.log("🔥 Setting copiedCellId to:", cellId);
                        // Update ref immediately so this render sees the new value
                        copiedCellIdRef.current = cellId;
                        setCopiedCellId(cellId);
                          // ADDED: clear any prior timer and start a fresh one
                        if (copyTimerRef.current) {
                          window.clearTimeout(copyTimerRef.current);
                        }
                        copyTimerRef.current = window.setTimeout(() => {
                          console.log("⌛ Resetting copiedCellId");
                          setCopiedCellId(null);
                          copiedCellIdRef.current = null;
                          copyTimerRef.current = null;
                        }, 1000); // ⏱ Match test case timing
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
      }
    })();
  }, []);

        // ADDED (Batch4): reset to page 0 whenever the global filter changes
        useEffect(() => {
          setPagination((prev) => ({ ...prev, pageIndex: 0 }));
        }, [globalFilter]);
        +  // ADDED: cleanup any pending copy timeout on unmount
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
    },
    onColumnVisibilityChange: setColumnVisibility,
    onGlobalFilterChange: setGlobalFilter,
    onPaginationChange: setPagination,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    manualPagination: false,
  });

  // ADDED (Batch4): derived display values for totals/page counts
  const totalRows = table.getFilteredRowModel().rows.length;
  const pageIndex = table.getState().pagination.pageIndex;
  const pageSize = table.getState().pagination.pageSize;
  const pageCount = table.getPageCount();
  const firstRow = totalRows === 0 ? 0 : pageIndex * pageSize + 1;
  const lastRow = totalRows === 0 ? 0 : Math.min(totalRows, (pageIndex + 1) * pageSize);

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

      <button
        className="border px-2 py-1 rounded text-sm"
        onClick={() => setShowColumnModal(true)}
      >
        Change Viewable Fields
      </button>

      {/* ADDED (Batch4): totals (post-filter) */}
      <div className="ml-auto text-sm">
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
