import React from 'react';
import { Column } from '@tanstack/react-table';

interface ColumnSelectorModalProps {
  isOpen: boolean;
  onClose: () => void;
  onReset: () => void;
  columns: Column<any, unknown>[];
  title?: string;
}

export default function ColumnSelectorModal({
  isOpen,
  onClose,
  onReset,
  columns,
  title = 'Select Columns'
}: ColumnSelectorModalProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center">
      <div className="bg-white p-6 rounded shadow max-h-[80vh] w-[300px] overflow-y-auto">
        <h2 className="text-lg font-semibold mb-4">{title}</h2>

        <div className="space-y-2">
          {columns.map(col => (
            <label key={col.id} className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={col.getIsVisible()}
                onChange={col.getToggleVisibilityHandler()}
              />
              {col.id}
            </label>
          ))}
        </div>

        <div className="mt-4 flex justify-between">
          <button
            onClick={onReset}
            className="text-sm text-red-600 hover:underline"
          >
            Reset to Defaults
          </button>
          <button
            onClick={onClose}
            className="bg-blue-600 text-white px-3 py-1 rounded text-sm"
          >
            Done
          </button>
        </div>
      </div>
    </div>
  );
}
