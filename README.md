# DynamoDB PITR Recovery Assistant

A React + Node tool to:

1. Read your local AWS credentials/profile (standard AWS SDK chain).
2. List DynamoDB tables from your configured region.
3. Select a restore point in time.
4. Create an **exact diff preview** either by:
   - restoring to a temporary table from PITR, or
   - comparing directly against an existing backup/restored table you already have.
5. Run a final PITR restore into a new target table.
6. Optionally replace the current table in-place (delete + restore back to same name), with optional archival snapshot first.

## Quick start

```bash
npm install
npm run dev
```

- UI: http://localhost:5173
- API: http://localhost:8787

## Required IAM permissions

- `dynamodb:ListTables`
- `dynamodb:DescribeTable`
- `dynamodb:DescribeContinuousBackups`
- `dynamodb:RestoreTableToPointInTime`
- `dynamodb:Scan`
- `dynamodb:DeleteTable` (for preview cleanup)
- `dynamodb:DeleteTable` (for in-place replacement)

## Notes

- The preview step creates a **real temporary table** and scans both tables to compute a precise diff.
- This may take time and cost money for larger datasets.
- DynamoDB PITR restores to a new table name by design.
- The “replace current table” flow is destructive: it deletes the current table name, then restores PITR into that same name.
- Long-running restore operations are asynchronous:
  - start with `/api/pitr/preview/start`, `/api/pitr/recover/start`, or `/api/pitr/replace-current/start`
  - poll `/api/operations/:operationId` until `status` becomes `succeeded` or `failed`.
- Preview payload options:
  - PITR preview: `tableName + restoreIsoTime + tempTableName`
  - Existing table preview: `tableName + comparisonTableName`
- You can tune backend waiting behavior with:
  - `PITR_POLL_INTERVAL_MS` (default `5000`)
  - `PITR_MAX_POLL_ATTEMPTS` (default `720`, ~1 hour at 5s interval)
