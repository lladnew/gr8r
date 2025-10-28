import type { ReactNode } from 'react';

type EnumPill = {
  field: string;                 // e.g., 'status'
  options: readonly string[];    // e.g., STATUS_OPTIONS
  classes: Record<string,string> // option -> badge classes
};

type BulkField =
  | { type: 'enum'; field: string; options: readonly string[] }
  | { type: 'datetime'; field: string; clearable?: boolean }
  | { type: 'text'; field: string; clearable?: boolean };

type Editable = {
  noClear: Set<string>;
  withClear: Set<string>;
};

type SpecialRender = (params: {
  key: string;
  value: any;
  row: Record<string, any>;
  tz: string;
}) => ReactNode;

export type TableConfig = {
  title: string;
  apiPath: string;                              // '/db1/videos' | '/db1/publishing'
  getRowKey: (row: Record<string,any>) => string;
  defaultVisible: string[];
  defaultSortCol: string;
  enumPills?: EnumPill[];                       // e.g., status/video_type vs just status
  editable: Editable;
  bulkFields: BulkField[];                      // what shows in the bulk bar
  specialRenderers?: Record<string, SpecialRender>; // e.g., scheduled_at compact
  readOnlyFields?: Set<string>;                 // disable editing UI for these
  deleteKeyBuilder: (row: Record<string,any>) =>
    | Record<string, any> | null;               // which keys to send to DELETE
};
