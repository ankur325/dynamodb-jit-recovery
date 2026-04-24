import express from 'express';
import cors from 'cors';
import {
  DeleteTableCommand,
  DescribeContinuousBackupsCommand,
  DescribeTableCommand,
  DynamoDBClient,
  ListTablesCommand,
  RestoreTableToPointInTimeCommand,
  ScanCommand,
  waitUntilTableExists,
  waitUntilTableNotExists
} from '@aws-sdk/client-dynamodb';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { unmarshall } from '@aws-sdk/util-dynamodb';

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1';
const client = new DynamoDBClient({
  region,
  credentials: defaultProvider()
});

const MAX_SAMPLE_DIFFS = 40;

const buildKeyString = (item, keySchema = []) => {
  const keyOnly = keySchema.reduce((acc, key) => {
    acc[key.AttributeName] = item[key.AttributeName];
    return acc;
  }, {});

  return JSON.stringify(keyOnly);
};

async function scanAllRows(tableName) {
  const items = [];
  let ExclusiveStartKey;

  do {
    const response = await client.send(
      new ScanCommand({
        TableName: tableName,
        ExclusiveStartKey
      })
    );

    const unmarshalled = (response.Items ?? []).map((row) => unmarshall(row));
    items.push(...unmarshalled);
    ExclusiveStartKey = response.LastEvaluatedKey;
  } while (ExclusiveStartKey);

  return items;
}

function diffItems({ sourceRows, restoredRows, keySchema }) {
  const sourceMap = new Map(sourceRows.map((row) => [buildKeyString(row, keySchema), row]));
  const restoredMap = new Map(restoredRows.map((row) => [buildKeyString(row, keySchema), row]));

  const onlyInCurrent = [];
  const onlyInRecovered = [];
  const changed = [];

  for (const [key, currentItem] of sourceMap.entries()) {
    if (!restoredMap.has(key)) {
      onlyInCurrent.push({ key: JSON.parse(key), item: currentItem });
      continue;
    }

    const recoveredItem = restoredMap.get(key);
    if (JSON.stringify(currentItem) !== JSON.stringify(recoveredItem)) {
      changed.push({
        key: JSON.parse(key),
        current: currentItem,
        recovered: recoveredItem
      });
    }
  }

  for (const [key, recoveredItem] of restoredMap.entries()) {
    if (!sourceMap.has(key)) {
      onlyInRecovered.push({ key: JSON.parse(key), item: recoveredItem });
    }
  }

  return {
    currentItemCount: sourceRows.length,
    recoveredItemCount: restoredRows.length,
    onlyInCurrentCount: onlyInCurrent.length,
    onlyInRecoveredCount: onlyInRecovered.length,
    changedCount: changed.length,
    samples: {
      onlyInCurrent: onlyInCurrent.slice(0, MAX_SAMPLE_DIFFS),
      onlyInRecovered: onlyInRecovered.slice(0, MAX_SAMPLE_DIFFS),
      changed: changed.slice(0, MAX_SAMPLE_DIFFS)
    }
  };
}

app.get('/api/health', (_req, res) => {
  res.json({ status: 'ok', region });
});

app.get('/api/tables', async (_req, res) => {
  try {
    const data = await client.send(new ListTablesCommand({}));
    res.json({ region, tables: data.TableNames ?? [] });
  } catch (error) {
    res.status(500).json({ message: error.message, details: error });
  }
});

app.get('/api/table/:name', async (req, res) => {
  try {
    const data = await client.send(new DescribeTableCommand({ TableName: req.params.name }));
    const table = data.Table;
    res.json({
      tableName: table?.TableName,
      itemCount: table?.ItemCount,
      keySchema: table?.KeySchema ?? [],
      billingMode: table?.BillingModeSummary?.BillingMode ?? 'PROVISIONED',
      pitrEnabled: false
    });
  } catch (error) {
    res.status(500).json({ message: error.message, details: error });
  }
});

app.post('/api/pitr/preview', async (req, res) => {
  const { tableName, restoreIsoTime, tempTableName } = req.body;

  if (!tableName || !restoreIsoTime || !tempTableName) {
    return res.status(400).json({ message: 'tableName, restoreIsoTime and tempTableName are required.' });
  }

  try {
    const backupStatus = await client.send(
      new DescribeContinuousBackupsCommand({ TableName: tableName })
    );

    const pitrStatus = backupStatus.ContinuousBackupsDescription?.PointInTimeRecoveryDescription?.PointInTimeRecoveryStatus;
    if (pitrStatus !== 'ENABLED') {
      return res.status(400).json({ message: `PITR is not enabled for ${tableName}.` });
    }

    await client.send(
      new RestoreTableToPointInTimeCommand({
        SourceTableName: tableName,
        TargetTableName: tempTableName,
        RestoreDateTime: new Date(restoreIsoTime),
        UseLatestRestorableTime: false
      })
    );

    await waitUntilTableExists(
      {
        client,
        maxWaitTime: 300
      },
      { TableName: tempTableName }
    );

    const [sourceMeta, sourceRows, restoredRows] = await Promise.all([
      client.send(new DescribeTableCommand({ TableName: tableName })),
      scanAllRows(tableName),
      scanAllRows(tempTableName)
    ]);

    const keySchema = sourceMeta.Table?.KeySchema ?? [];
    const diff = diffItems({ sourceRows, restoredRows, keySchema });

    return res.json({
      tableName,
      tempTableName,
      restoreIsoTime,
      keySchema,
      ...diff,
      cleanupHint: `Call DELETE /api/table/${tempTableName} to remove the temporary table after review.`
    });
  } catch (error) {
    return res.status(500).json({ message: error.message, details: error });
  }
});

app.post('/api/pitr/recover', async (req, res) => {
  const { tableName, restoreIsoTime, targetTableName } = req.body;

  if (!tableName || !restoreIsoTime || !targetTableName) {
    return res.status(400).json({ message: 'tableName, restoreIsoTime and targetTableName are required.' });
  }

  try {
    await client.send(
      new RestoreTableToPointInTimeCommand({
        SourceTableName: tableName,
        TargetTableName: targetTableName,
        RestoreDateTime: new Date(restoreIsoTime),
        UseLatestRestorableTime: false
      })
    );

    await waitUntilTableExists(
      {
        client,
        maxWaitTime: 300
      },
      { TableName: targetTableName }
    );

    res.json({
      message: `Recovery started and table ${targetTableName} is now available.`,
      sourceTableName: tableName,
      targetTableName,
      restoreIsoTime
    });
  } catch (error) {
    res.status(500).json({ message: error.message, details: error });
  }
});

app.post('/api/pitr/replace-current', async (req, res) => {
  const { tableName, restoreIsoTime, archiveCurrentTable = true, archiveTableName } = req.body;

  if (!tableName || !restoreIsoTime) {
    return res.status(400).json({ message: 'tableName and restoreIsoTime are required.' });
  }

  if (archiveCurrentTable && !archiveTableName) {
    return res.status(400).json({ message: 'archiveTableName is required when archiveCurrentTable is true.' });
  }

  try {
    const backupStatus = await client.send(
      new DescribeContinuousBackupsCommand({ TableName: tableName })
    );

    const pitrStatus = backupStatus.ContinuousBackupsDescription?.PointInTimeRecoveryDescription?.PointInTimeRecoveryStatus;
    if (pitrStatus !== 'ENABLED') {
      return res.status(400).json({ message: `PITR is not enabled for ${tableName}.` });
    }

    if (archiveCurrentTable) {
      await client.send(
        new RestoreTableToPointInTimeCommand({
          SourceTableName: tableName,
          TargetTableName: archiveTableName,
          UseLatestRestorableTime: true
        })
      );

      await waitUntilTableExists(
        {
          client,
          maxWaitTime: 300
        },
        { TableName: archiveTableName }
      );
    }

    await client.send(new DeleteTableCommand({ TableName: tableName }));

    await waitUntilTableNotExists(
      {
        client,
        maxWaitTime: 300
      },
      { TableName: tableName }
    );

    await client.send(
      new RestoreTableToPointInTimeCommand({
        SourceTableName: tableName,
        TargetTableName: tableName,
        RestoreDateTime: new Date(restoreIsoTime),
        UseLatestRestorableTime: false
      })
    );

    await waitUntilTableExists(
      {
        client,
        maxWaitTime: 300
      },
      { TableName: tableName }
    );

    return res.json({
      message: `Replaced ${tableName} with PITR snapshot from ${restoreIsoTime}.`,
      tableName,
      restoreIsoTime,
      archiveCurrentTable,
      archiveTableName: archiveCurrentTable ? archiveTableName : null
    });
  } catch (error) {
    return res.status(500).json({ message: error.message, details: error });
  }
});

app.delete('/api/table/:name', async (req, res) => {
  try {
    await client.send(new DeleteTableCommand({ TableName: req.params.name }));
    res.json({ message: `Delete started for ${req.params.name}` });
  } catch (error) {
    res.status(500).json({ message: error.message, details: error });
  }
});

const PORT = process.env.PORT || 8787;
app.listen(PORT, () => {
  console.log(`API listening on http://localhost:${PORT}`);
});
