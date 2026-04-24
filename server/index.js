import express from 'express';
import cors from 'cors';
import {
  DeleteTableCommand,
  DescribeContinuousBackupsCommand,
  DescribeTableCommand,
  DynamoDBClient,
  ListTablesCommand,
  RestoreTableToPointInTimeCommand,
  ScanCommand
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
const POLL_INTERVAL_MS = Number(process.env.PITR_POLL_INTERVAL_MS || 5000);
const MAX_POLL_ATTEMPTS = Number(process.env.PITR_MAX_POLL_ATTEMPTS || 720);
const operations = new Map();

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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForTableStatus(tableName, expectedStatus) {
  for (let attempt = 1; attempt <= MAX_POLL_ATTEMPTS; attempt += 1) {
    try {
      const description = await client.send(new DescribeTableCommand({ TableName: tableName }));
      const currentStatus = description?.Table?.TableStatus;
      if (currentStatus === expectedStatus) {
        return;
      }
    } catch (error) {
      if (
        expectedStatus === 'NOT_EXISTS'
        && error?.name === 'ResourceNotFoundException'
      ) {
        return;
      }
      throw error;
    }
    await sleep(POLL_INTERVAL_MS);
  }

  throw new Error(
    `Timed out waiting for ${tableName} to reach ${expectedStatus} after ${MAX_POLL_ATTEMPTS} attempts.`
  );
}

function createAsyncOperation(type, payload, work) {
  const operationId = `${type}-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
  operations.set(operationId, {
    operationId,
    type,
    payload,
    status: 'running',
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    progressMessage: 'Started.'
  });

  (async () => {
    try {
      const result = await work((progressMessage) => {
        const current = operations.get(operationId);
        if (!current) return;
        operations.set(operationId, {
          ...current,
          progressMessage,
          updatedAt: new Date().toISOString()
        });
      });
      const current = operations.get(operationId);
      if (!current) return;
      operations.set(operationId, {
        ...current,
        status: 'succeeded',
        progressMessage: 'Completed.',
        result,
        updatedAt: new Date().toISOString(),
        completedAt: new Date().toISOString()
      });
    } catch (error) {
      const current = operations.get(operationId);
      if (!current) return;
      operations.set(operationId, {
        ...current,
        status: 'failed',
        error: {
          name: error?.name || 'Error',
          message: error?.message || 'Unknown error'
        },
        updatedAt: new Date().toISOString(),
        completedAt: new Date().toISOString()
      });
    }
  })();

  return operationId;
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

app.post('/api/pitr/preview/start', async (req, res) => {
  const { tableName, restoreIsoTime, tempTableName, comparisonTableName } = req.body;

  if (!tableName) {
    return res.status(400).json({ message: 'tableName is required.' });
  }

  if (comparisonTableName && (restoreIsoTime || tempTableName)) {
    return res.status(400).json({
      message: 'Use either comparisonTableName OR restoreIsoTime/tempTableName for preview.'
    });
  }

  if (!comparisonTableName && (!restoreIsoTime || !tempTableName)) {
    return res.status(400).json({
      message: 'For PITR preview, restoreIsoTime and tempTableName are required.'
    });
  }

  const operationId = createAsyncOperation(
    'preview',
    { tableName, restoreIsoTime, tempTableName, comparisonTableName },
    async (progress) => {
      let restoredTableName = tempTableName;
      if (comparisonTableName) {
        progress(`Using existing comparison table ${comparisonTableName}...`);
        await client.send(new DescribeTableCommand({ TableName: comparisonTableName }));
        restoredTableName = comparisonTableName;
      } else {
        progress('Validating PITR status...');
        const backupStatus = await client.send(
          new DescribeContinuousBackupsCommand({ TableName: tableName })
        );

        const pitrStatus = backupStatus.ContinuousBackupsDescription?.PointInTimeRecoveryDescription?.PointInTimeRecoveryStatus;
        if (pitrStatus !== 'ENABLED') {
          throw new Error(`PITR is not enabled for ${tableName}.`);
        }

        progress(`Starting temporary restore table ${tempTableName}...`);
        await client.send(
          new RestoreTableToPointInTimeCommand({
            SourceTableName: tableName,
            TargetTableName: tempTableName,
            RestoreDateTime: new Date(restoreIsoTime),
            UseLatestRestorableTime: false
          })
        );

        progress(`Waiting for ${tempTableName} to become ACTIVE...`);
        await waitForTableStatus(tempTableName, 'ACTIVE');
      }

      progress('Scanning current and restored tables...');
      const [sourceMeta, sourceRows, restoredRows] = await Promise.all([
        client.send(new DescribeTableCommand({ TableName: tableName })),
        scanAllRows(tableName),
        scanAllRows(restoredTableName)
      ]);

      const keySchema = sourceMeta.Table?.KeySchema ?? [];
      const diff = diffItems({ sourceRows, restoredRows, keySchema });

      return {
        tableName,
        tempTableName: comparisonTableName ? null : tempTableName,
        comparisonTableName: comparisonTableName ?? null,
        restoreIsoTime: restoreIsoTime ?? null,
        keySchema,
        ...diff,
        cleanupHint: comparisonTableName
          ? null
          : `Call DELETE /api/table/${tempTableName} to remove the temporary table after review.`
      };
    }
  );

  return res.status(202).json({
    operationId,
    status: 'running',
    pollUrl: `/api/operations/${operationId}`
  });
});

app.post('/api/pitr/recover/start', async (req, res) => {
  const { tableName, restoreIsoTime, targetTableName } = req.body;

  if (!tableName || !restoreIsoTime || !targetTableName) {
    return res.status(400).json({ message: 'tableName, restoreIsoTime and targetTableName are required.' });
  }

  const operationId = createAsyncOperation(
    'recover',
    { tableName, restoreIsoTime, targetTableName },
    async (progress) => {
      progress(`Starting restore into ${targetTableName}...`);
      await client.send(
        new RestoreTableToPointInTimeCommand({
          SourceTableName: tableName,
          TargetTableName: targetTableName,
          RestoreDateTime: new Date(restoreIsoTime),
          UseLatestRestorableTime: false
        })
      );

      progress(`Waiting for ${targetTableName} to become ACTIVE...`);
      await waitForTableStatus(targetTableName, 'ACTIVE');

      return {
        message: `Recovery started and table ${targetTableName} is now available.`,
        sourceTableName: tableName,
        targetTableName,
        restoreIsoTime
      };
    }
  );

  return res.status(202).json({
    operationId,
    status: 'running',
    pollUrl: `/api/operations/${operationId}`
  });
});

app.post('/api/pitr/replace-current/start', async (req, res) => {
  const { tableName, restoreIsoTime, archiveCurrentTable = true, archiveTableName } = req.body;

  if (!tableName || !restoreIsoTime) {
    return res.status(400).json({ message: 'tableName and restoreIsoTime are required.' });
  }

  if (archiveCurrentTable && !archiveTableName) {
    return res.status(400).json({ message: 'archiveTableName is required when archiveCurrentTable is true.' });
  }

  const operationId = createAsyncOperation(
    'replace-current',
    { tableName, restoreIsoTime, archiveCurrentTable, archiveTableName },
    async (progress) => {
      progress('Validating PITR status...');
      const backupStatus = await client.send(
        new DescribeContinuousBackupsCommand({ TableName: tableName })
      );

      const pitrStatus = backupStatus.ContinuousBackupsDescription?.PointInTimeRecoveryDescription?.PointInTimeRecoveryStatus;
      if (pitrStatus !== 'ENABLED') {
        throw new Error(`PITR is not enabled for ${tableName}.`);
      }

      if (archiveCurrentTable) {
        progress(`Archiving current table into ${archiveTableName}...`);
        await client.send(
          new RestoreTableToPointInTimeCommand({
            SourceTableName: tableName,
            TargetTableName: archiveTableName,
            UseLatestRestorableTime: true
          })
        );
        progress(`Waiting for archive table ${archiveTableName} to become ACTIVE...`);
        await waitForTableStatus(archiveTableName, 'ACTIVE');
      }

      progress(`Deleting ${tableName} before in-place restore...`);
      await client.send(new DeleteTableCommand({ TableName: tableName }));
      await waitForTableStatus(tableName, 'NOT_EXISTS');

      progress(`Restoring ${tableName} to ${restoreIsoTime}...`);
      await client.send(
        new RestoreTableToPointInTimeCommand({
          SourceTableName: tableName,
          TargetTableName: tableName,
          RestoreDateTime: new Date(restoreIsoTime),
          UseLatestRestorableTime: false
        })
      );

      progress(`Waiting for ${tableName} to become ACTIVE...`);
      await waitForTableStatus(tableName, 'ACTIVE');

      return {
        message: `Replaced ${tableName} with PITR snapshot from ${restoreIsoTime}.`,
        tableName,
        restoreIsoTime,
        archiveCurrentTable,
        archiveTableName: archiveCurrentTable ? archiveTableName : null
      };
    }
  );

  return res.status(202).json({
    operationId,
    status: 'running',
    pollUrl: `/api/operations/${operationId}`
  });
});

app.get('/api/operations/:operationId', (req, res) => {
  const operation = operations.get(req.params.operationId);
  if (!operation) {
    return res.status(404).json({ message: 'Operation not found.' });
  }

  return res.json(operation);
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
