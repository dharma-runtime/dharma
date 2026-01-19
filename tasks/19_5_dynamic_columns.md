# Task 19.5: Dynamic Column Projection (DHARMA-Q)

## Goal
Enable DHARMA-Q to query fields defined in DHL contracts (`amount`, `status`, `assignee`), not just system metadata.

## Problem
Currently, DHARMA-Q only indexes `seq`, `typ`, `subject`, and `text`. It ignores the JSON body of the assertion.

## Specification

### 1. Storage Layout (Sparse Columns)
For every field found in the DHL Schema:
- Create `cols/<field_name>.bin` (Values).
- Create `cols/<field_name>.valid` (Validity Bitmap).

**Type Mapping:**
- `Int`, `Duration` -> `i64` (8 bytes).
- `Timestamp` -> `i64` (Microseconds).
- `Bool` -> `u8` (1 byte).
- `Enum`, `Currency` -> `u32` (Symbol ID).
- `Identity` -> `[u8; 32]`.
- `GeoPoint` -> Two columns: `_lat.bin` (i32), `_lon.bin` (i32).

### 2. Ingest Logic (`dharma-core/src/dharmaq/mod.rs`)
Inside `append_row`:
1.  **Load Schema:** Fetch `CqrsSchema` for the assertion's `typ`.
2.  **Extract Fields:**
    - If field exists in assertion body:
        - Write value to `.bin` (and `_lat`/`_lon` for Geo).
        - Set bit 1 in `.valid`.
    - If field missing (or different type of assertion):
        - Write zero/padding to `.bin`.
        - Set bit 0 in `.valid`.
3.  **Synchronization:** All columns must have equal length (row count).

### 3. Query Logic (`execute_plan`)
When filter is `where amount > 100`:
1.  Check if `cols/amount.bin` exists.
2.  Load `.valid` mask.
3.  Apply `filter_gt_i64` to `.bin`.
4.  Result = `valid_mask AND gt_mask`.

When filter is `near(loc, 14.5, 121.0, 500m)`:
1.  Load `cols/loc_lat.bin` and `cols/loc_lon.bin`.
2.  Apply `filter_geo_box` (Optimization).
3.  Apply `filter_geo_radius` (Exact).

## Implementation Steps
1.  **Refactor Ingest:** Modify `append_row` to accept the `CqrsSchema`.
2.  **Column Writers:** Implement typed appenders (`append_i64_col`, `append_bool_col`, `append_geo_col`).
3.  **Planner:** Update `QueryPlan` resolution to detect if a field is a "System Column" or "Dynamic Column".
4.  **DHL Update:** Ensure `GeoPoint` is in the `TypeSpec`.
