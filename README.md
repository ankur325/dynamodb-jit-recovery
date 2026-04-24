# DynamoDB PITR Recovery Assistant

A React + Node tool to:

1. Read your local AWS credentials/profile (standard AWS SDK chain).
2. List DynamoDB tables from your configured region.
3. Select a restore point in time.
4. Create an **exact diff preview** by restoring to a temporary table and comparing item-level changes.
5. Run a final PITR restore into a new target table.

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

## Notes

- The preview step creates a **real temporary table** and scans both tables to compute a precise diff.
- This may take time and cost money for larger datasets.
- DynamoDB PITR restores to a new table name by design.
