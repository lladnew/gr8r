import {
  getCoreRowModel,
  getPaginationRowModel,
  getFilteredRowModel,
  getResizingRowModel,
  ColumnDef,
} from '@tanstack/table-core';

import {
  useReactTable,
  flexRender,
} from '@tanstack/react-table';

console.log('✅ All table-core and react-table imports resolved.');
console.log('🧩 Sample:', {
  core: typeof getCoreRowModel,
  resize: typeof getResizingRowModel,
  useReactTable: typeof useReactTable,
});


