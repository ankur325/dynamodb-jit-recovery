import { useMemo, useState } from 'react';

const api = async (path, options) => {
  const response = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...options
  });

  if (!response.ok) {
    const errorBody = await response.json().catch(() => ({}));
    throw new Error(errorBody.message || `Request failed: ${response.status}`);
  }

  return response.json();
};

const isoDateTimeInputValue = () => {
  const now = new Date();
  now.setMinutes(now.getMinutes() - now.getTimezoneOffset());
  return now.toISOString().slice(0, 16);
};

const Section = ({ title, children }) => (
  <section className="section">
    <h2>{title}</h2>
    {children}
  </section>
);

export default function App() {
  const [tables, setTables] = useState([]);
  const [region, setRegion] = useState('');
  const [selectedTable, setSelectedTable] = useState('');
  const [restoreTime, setRestoreTime] = useState(isoDateTimeInputValue());
  const [preview, setPreview] = useState(null);
  const [status, setStatus] = useState('Press "Load tables" to begin.');
  const [loading, setLoading] = useState(false);
  const [targetTableName, setTargetTableName] = useState('');
  const [archiveTableName, setArchiveTableName] = useState('');
  const [archiveCurrentTable, setArchiveCurrentTable] = useState(true);

  const tempTableName = useMemo(() => {
    if (!selectedTable) return '';
    return `${selectedTable}-preview-${Date.now()}`;
  }, [selectedTable]);

  const pollOperationUntilDone = async (operationId) => {
    // Keep polling for long-running DynamoDB restore operations.
    while (true) {
      const operation = await api(`/api/operations/${operationId}`);
      if (operation.status === 'succeeded') {
        return operation.result;
      }
      if (operation.status === 'failed') {
        throw new Error(operation?.error?.message || 'Long-running operation failed.');
      }
      setStatus(operation.progressMessage || 'Still running...');
      await new Promise((resolve) => setTimeout(resolve, 4000));
    }
  };

  const loadTables = async () => {
    setLoading(true);
    setStatus('Loading DynamoDB tables using local AWS credentials...');
    try {
      const data = await api('/api/tables');
      setTables(data.tables);
      setRegion(data.region);
      setStatus(`Loaded ${data.tables.length} tables from ${data.region}.`);
    } catch (error) {
      setStatus(error.message);
    } finally {
      setLoading(false);
    }
  };

  const previewRecovery = async () => {
    if (!selectedTable) return;
    setLoading(true);
    setStatus('Creating temporary restored table and diffing item-level changes...');
    setPreview(null);
    try {
      const restoreIsoTime = new Date(restoreTime).toISOString();
      const start = await api('/api/pitr/preview/start', {
        method: 'POST',
        body: JSON.stringify({
          tableName: selectedTable,
          restoreIsoTime,
          tempTableName
        })
      });
      const data = await pollOperationUntilDone(start.operationId);
      setPreview(data);
      setStatus('Preview complete. Inspect differences below, then recover or cleanup.');
    } catch (error) {
      setStatus(error.message);
    } finally {
      setLoading(false);
    }
  };

  const recover = async () => {
    if (!selectedTable || !targetTableName) return;
    setLoading(true);
    setStatus(`Starting recovery into ${targetTableName}...`);
    try {
      const restoreIsoTime = new Date(restoreTime).toISOString();
      const start = await api('/api/pitr/recover/start', {
        method: 'POST',
        body: JSON.stringify({
          tableName: selectedTable,
          restoreIsoTime,
          targetTableName
        })
      });
      const data = await pollOperationUntilDone(start.operationId);

      setStatus(data.message);
    } catch (error) {
      setStatus(error.message);
    } finally {
      setLoading(false);
    }
  };

  const replaceCurrentTable = async () => {
    if (!selectedTable) return;

    if (archiveCurrentTable && !archiveTableName) {
      setStatus('Provide an archive table name to keep the current state before replacement.');
      return;
    }

    const confirmed = window.confirm(
      `This will DELETE and replace ${selectedTable}. Continue?`
    );

    if (!confirmed) return;

    setLoading(true);
    setStatus(`Replacing ${selectedTable} with PITR snapshot...`);
    try {
      const restoreIsoTime = new Date(restoreTime).toISOString();
      const start = await api('/api/pitr/replace-current/start', {
        method: 'POST',
        body: JSON.stringify({
          tableName: selectedTable,
          restoreIsoTime,
          archiveCurrentTable,
          archiveTableName: archiveCurrentTable ? archiveTableName : undefined
        })
      });
      const data = await pollOperationUntilDone(start.operationId);
      setStatus(data.message);
    } catch (error) {
      setStatus(error.message);
    } finally {
      setLoading(false);
    }
  };

  const cleanupPreviewTable = async () => {
    if (!preview?.tempTableName) return;
    setLoading(true);
    setStatus(`Deleting temporary table ${preview.tempTableName}...`);
    try {
      const data = await api(`/api/table/${preview.tempTableName}`, { method: 'DELETE' });
      setStatus(`${data.message}. You can now run another preview safely.`);
    } catch (error) {
      setStatus(error.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <main>
      <h1>DynamoDB PITR Recovery Assistant</h1>
      <p className="subtle">
        Interactive preview tool: compare the current table against a point-in-time restore, then recover with confidence.
      </p>

      <Section title="1) Select table and restore time">
        <div className="row">
          <button onClick={loadTables} disabled={loading}>Load tables</button>
          <span>Region: <strong>{region || 'Unknown'}</strong></span>
        </div>

        <label>
          Table
          <select
            value={selectedTable}
            onChange={(event) => {
              setSelectedTable(event.target.value);
              setPreview(null);
            }}
          >
            <option value="">-- choose table --</option>
            {tables.map((tableName) => (
              <option value={tableName} key={tableName}>{tableName}</option>
            ))}
          </select>
        </label>

        <label>
          Restore timestamp
          <input
            type="datetime-local"
            value={restoreTime}
            onChange={(event) => setRestoreTime(event.target.value)}
          />
        </label>

        <button className="primary" onClick={previewRecovery} disabled={loading || !selectedTable}>
          Preview exact differences (creates temporary table)
        </button>
      </Section>

      {preview && (
        <Section title="2) Review exact differences">
          <div className="stats">
            <div><span>Current items</span><strong>{preview.currentItemCount}</strong></div>
            <div><span>Recovered items</span><strong>{preview.recoveredItemCount}</strong></div>
            <div><span>Only in current</span><strong>{preview.onlyInCurrentCount}</strong></div>
            <div><span>Only in recovered</span><strong>{preview.onlyInRecoveredCount}</strong></div>
            <div><span>Changed</span><strong>{preview.changedCount}</strong></div>
          </div>

          <details open>
            <summary>Sample: rows present only now</summary>
            <pre>{JSON.stringify(preview.samples.onlyInCurrent, null, 2)}</pre>
          </details>
          <details>
            <summary>Sample: rows that would come back after restore</summary>
            <pre>{JSON.stringify(preview.samples.onlyInRecovered, null, 2)}</pre>
          </details>
          <details>
            <summary>Sample: rows with changed values</summary>
            <pre>{JSON.stringify(preview.samples.changed, null, 2)}</pre>
          </details>

          <div className="row">
            <button onClick={cleanupPreviewTable} disabled={loading}>Cleanup temporary preview table</button>
            <span>{preview.tempTableName}</span>
          </div>
        </Section>
      )}

      <Section title="3) Recover into a new table">
        <label>
          Target restored table name
          <input
            type="text"
            placeholder="my-table-restore-2026-04-24"
            value={targetTableName}
            onChange={(event) => setTargetTableName(event.target.value)}
          />
        </label>
        <button className="danger" onClick={recover} disabled={loading || !selectedTable || !targetTableName}>
          Restore now
        </button>
      </Section>

      <Section title="4) Replace current table (destructive)">
        <p className="subtle">
          DynamoDB cannot restore in-place directly. This action deletes the current table, then restores the PITR
          snapshot back into the same table name.
        </p>
        <label className="checkbox-label">
          <input
            type="checkbox"
            checked={archiveCurrentTable}
            onChange={(event) => setArchiveCurrentTable(event.target.checked)}
          />
          Archive current state first (recommended)
        </label>
        {archiveCurrentTable && (
          <label>
            Archive table name
            <input
              type="text"
              placeholder={`${selectedTable || 'table'}-pre-replace-${new Date().toISOString().slice(0, 10)}`}
              value={archiveTableName}
              onChange={(event) => setArchiveTableName(event.target.value)}
            />
          </label>
        )}
        <button className="danger" onClick={replaceCurrentTable} disabled={loading || !selectedTable}>
          Replace current table with selected PITR time
        </button>
      </Section>

      <Section title="Status">
        <p>{status}</p>
      </Section>

      <Section title="Notes">
        <ul>
          <li>Preview runs an actual point-in-time restore to a temporary table, so it may take minutes and incur DynamoDB costs.</li>
          <li>DynamoDB PITR can only restore into a new table; this app follows that model for safety.</li>
          <li>Use IAM permissions for List/Describe/Scan/Restore/Delete table operations.</li>
        </ul>
      </Section>
    </main>
  );
}
