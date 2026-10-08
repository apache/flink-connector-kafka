/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Execute the actual inline workflow scripts, with recorded GitHub/filesystem calls.
// Run with: node --test .github/scripts/weekly-matrix.test.js
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');

const workflow = fs.readFileSync(path.join(__dirname, '../workflows/weekly.yml'), 'utf8');
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
const day = 24 * 60 * 60 * 1000;

function section(source, name, indent) {
    const lines = source.split('\n');
    const start = lines.indexOf(`${' '.repeat(indent)}${name}:`);
    assert.notEqual(start, -1, `Missing YAML section ${name}`);
    let end = start + 1;
    while (end < lines.length && (!lines[end].trim()
        || lines[end].startsWith(' '.repeat(indent + 1)) || lines[end].trim().startsWith('#'))) end++;
    return lines.slice(start + 1, end).join('\n');
}

function value(source, key, indent) {
    const matches = source.split('\n').filter(line => line.startsWith(`${' '.repeat(indent)}${key}: `));
    assert.equal(matches.length, 1, `Expected one YAML value ${key}`);
    return matches[0].slice(indent + key.length + 2).trim();
}

const steps = workflow.split(/^      - /m).slice(1);
function step(id) {
    const matches = steps.filter(source => source.includes(`\n        id: ${id}\n`));
    assert.equal(matches.length, 1, `Expected one workflow step ${id}`);
    return matches[0];
}
function actionStep(action) {
    const matches = steps.filter(source => source.includes(`uses: ${action}@`));
    assert.equal(matches.length, 1, `Expected one ${action} step`);
    return matches[0];
}
function block(source, header) {
    const lines = source.split('\n');
    const start = lines.indexOf(header);
    assert.notEqual(start, -1, `Missing YAML block ${header.trim()}`);
    const result = [];
    for (const line of lines.slice(start + 1)) {
        if (line.trim() && !line.startsWith('            ')) break;
        result.push(line.slice(12));
    }
    return result.join('\n');
}
function script(id) {
    return new AsyncFunction('github', 'context', 'core', 'require', 'process', 'Date',
        block(step(id), '          script: |'));
}

const baselineScript = script('baseline_run');
const matrixScript = script('matrix');
const configuredRows = JSON.parse(block(step('matrix'), '          FLINK_BRANCHES: >-'));
const isSnapshot = row => row.flink.endsWith('-SNAPSHOT');
const releasedRows = configuredRows.filter(row => !isSnapshot(row));
const snapshotRows = configuredRows.filter(isSnapshot);
const branches = [...new Set(configuredRows.map(row => row.branch))];
const callers = ['compile_and_test', 'python_test'];
const jobs = Object.fromEntries(callers.map(name => [name, section(workflow, name, 2)]));
const download = section(actionStep('actions/download-artifact'), 'with', 8);
const upload = section(actionStep('actions/upload-artifact'), 'with', 8);
const artifactName = value(upload, 'name', 10);
const statePath = value(upload, 'path', 10);
const sharedUses = callers.map(name => value(jobs[name], 'uses', 4));
const key = row => JSON.stringify(row);
const rowLabel = row => `${row.flink} / ${row.branch} / ${row.jdk || 'all'}`;

function callerName(name, row) {
    return value(jobs[name], 'name', 4).replace(/\$\{\{\s*matrix\.flink_branches\.(\w+)(?:\s*\|\|\s*'([^']*)')?\s*\}\}/g,
        (_, field, fallback) => row[field] || fallback || '');
}
function successfulJobs(rows) {
    return rows.flatMap(row => callers.flatMap(name => ['build', 'test'].map(child => ({
        name: `${callerName(name, row)} / ${child}`, conclusion: 'success',
    }))));
}
function ciRevision(scenario) {
    return scenario.currentRun.referenced_workflows.slice()
        .sort((left, right) => left.path.localeCompare(right.path)).map(reference => reference.sha).join(',');
}
function resolved(rows, scenario) {
    return rows.map(row => ({ ...row, ...(scenario.branchShas[row.branch]
        ? { sha: scenario.branchShas[row.branch] } : {}) }));
}
function selectedFor(scenario, predicate) {
    return resolved(scenario.rows.filter(row => isSnapshot(row) || predicate(row)), scenario);
}
function greenState(scenario) {
    return { rows: Object.fromEntries(releasedRows.map(row => [key(row), {
        ...scenario.baselineState.rows[key(row)],
        greenAt: scenario.baselineRun.created_at,
        greenRunId: scenario.baselineRun.id,
    }])) };
}

function fixture() {
    const repository = { owner: 'apache', repo: 'flink-connector-kafka' };
    const provenance = {
        event: 'schedule', head_branch: 'main', head_repository: { full_name: 'apache/flink-connector-kafka' },
    };
    const scenario = {
        rows: configuredRows,
        now: Date.parse('2026-09-30T00:00:00Z'),
        branchShas: Object.fromEntries(branches.map(branch => [branch, `${branch}-sha`])),
        context: { eventName: 'schedule', repo: repository, ref: 'refs/heads/main',
            runNumber: 20, runId: 200020, sha: 'current-head' },
        baselineRun: { ...provenance, id: 100019, run_number: 19, status: 'completed',
            conclusion: 'failure', head_sha: 'older-head', created_at: '2026-09-23T00:00:00Z' },
        currentRun: { ...provenance, id: 200020, run_number: 20, status: 'in_progress',
            referenced_workflows: sharedUses.map((workflowPath, index) => ({ path: workflowPath, sha: `shared-ci-${index}` })) },
        workflowFile: { type: 'file', sha: 'weekly-workflow-blob' },
        artifacts: [{ name: artifactName, expired: false }],
        jobs: successfulJobs(configuredRows),
        requests: [], filesystemCalls: [], warnings: [], notices: [],
        summary: { headings: [], tables: [], raw: [], written: false },
        expectBaselineRead: true, expectJobs: true,
    };
    scenario.baselineRunId = String(scenario.baselineRun.id);
    scenario.expectedArtifactRunId = scenario.baselineRun.id;
    scenario.runs = [{ ...scenario.currentRun }, { ...scenario.baselineRun }];
    scenario.baselineState = { rows: Object.fromEntries(releasedRows.map(row => [key(row), {
        sha: scenario.branchShas[row.branch], ci: ciRevision(scenario), workflow: scenario.workflowFile.sha,
        greenAt: null, greenRunId: null,
    }])) };
    const record = (method, params) => scenario.requests.push({ method, params: { ...params } });
    const actions = {
        async listWorkflowRuns(params) {
            record('listWorkflowRuns', params);
            if (scenario.historyError) throw scenario.historyError;
            return { data: { workflow_runs: scenario.runs } };
        },
        async getWorkflowRun(params) {
            record('getWorkflowRun', params);
            if (params.run_id === scenario.context.runId) {
                if (scenario.currentRunError) throw scenario.currentRunError;
                return { data: scenario.currentRun };
            }
            if (scenario.baselineRunError) throw scenario.baselineRunError;
            return { data: scenario.baselineRun };
        },
        async listJobsForWorkflowRun(params) {
            record('unpaginatedJobs', params);
            return { data: { jobs: scenario.jobs } };
        },
        async listWorkflowRunArtifacts(params) {
            record('unpaginatedArtifacts', params);
            return { data: { artifacts: scenario.artifacts } };
        },
    };
    scenario.github = { rest: { actions, repos: {
        async getBranch(params) {
            record('getBranch', params);
            if (!scenario.branchShas[params.branch]) throw new Error('Branch lookup failed');
            return { data: { commit: { sha: scenario.branchShas[params.branch] } } };
        },
        async getContent(params) {
            record('getContent', params);
            if (scenario.workflowError) throw scenario.workflowError;
            return { data: scenario.workflowFile };
        },
    } }, async paginate(method, params) {
        const isJobs = method === actions.listJobsForWorkflowRun;
        record(isJobs ? 'paginateJobs' : method === actions.listWorkflowRunArtifacts ? 'paginateArtifacts' : 'unknownPagination', params);
        if (isJobs && scenario.jobsError) throw scenario.jobsError;
        if (!isJobs && scenario.artifactError) throw scenario.artifactError;
        return isJobs ? scenario.jobs : scenario.artifacts;
    } };
    return scenario;
}

function assertMessages(actual, expected, kind) {
    assert.equal(actual.length, expected.length, `Unexpected ${kind}: ${JSON.stringify(actual)}`);
    for (const fragment of expected) assert.ok(actual.some(message => message.includes(fragment)),
        `Missing ${kind} containing ${JSON.stringify(fragment)}: ${JSON.stringify(actual)}`);
}

async function execute(which, scenario, { warnings = [], notices = [] } = {}) {
    const outputs = {};
    const requestsBefore = scenario.requests.length;
    const filesystemBefore = scenario.filesystemCalls.length;
    const warningsBefore = scenario.warnings.length;
    const noticesBefore = scenario.notices.length;
    const summary = {
        addHeading(text) { scenario.summary.headings.push(text); return summary; },
        addTable(rows) { scenario.summary.tables.push(rows); return summary; },
        addRaw(text) { scenario.summary.raw.push(text); return summary; },
        write() { return new Promise((resolve, reject) => setImmediate(() => {
            scenario.summary.written = true;
            if (scenario.summaryError) reject(scenario.summaryError);
            else resolve(summary);
        })); },
    };
    const filesystem = {
        readFileSync(...args) {
            scenario.filesystemCalls.push({ method: 'readFileSync', args });
            if (scenario.readError) throw scenario.readError;
            if (scenario.artifactText !== undefined) return scenario.artifactText;
            if (scenario.baselineState === undefined) throw new Error('Artifact missing');
            return JSON.stringify(scenario.baselineState);
        },
        mkdirSync(...args) { scenario.filesystemCalls.push({ method: 'mkdirSync', args }); },
        writeFileSync(...args) {
            scenario.filesystemCalls.push({ method: 'writeFileSync', args });
            scenario.writtenText = args[1];
        },
    };
    class FixedDate extends Date {
        constructor(...args) { super(...(args.length ? args : [scenario.now])); }
        static now() { return scenario.now; }
    }
    let scriptError;
    try {
        await (which === 'baseline' ? baselineScript : matrixScript)(scenario.github, scenario.context, {
            info() {}, warning(message) { scenario.warnings.push(message); },
            notice(message) { scenario.notices.push(message); },
            setOutput(name, result) { outputs[name] = result; }, summary,
        }, name => {
            scenario.filesystemCalls.push({ method: 'require', args: [name] });
            return filesystem;
        }, { env: { FLINK_BRANCHES: JSON.stringify(scenario.rows), BASELINE_RUN_ID: scenario.baselineRunId } }, FixedDate);
    } catch (error) { scriptError = error; }

    const repository = scenario.context.repo;
    const expected = which === 'baseline' ? [
        { method: 'listWorkflowRuns', params: { ...repository, workflow_id: 'weekly.yml',
            branch: scenario.context.ref.replace('refs/heads/', ''), per_page: 100 } },
        ...(!scenario.historyError && scenario.expectedArtifactRunId ? [{ method: 'paginateArtifacts',
            params: { ...repository, run_id: scenario.expectedArtifactRunId, per_page: 100 } }] : []),
    ] : [
        ...[...new Set(scenario.rows.map(row => row.branch))].map(branch => ({ method: 'getBranch', params: { ...repository, branch } })),
        { method: 'getContent', params: { ...repository, path: '.github/workflows/weekly.yml', ref: scenario.context.sha } },
        { method: 'getWorkflowRun', params: { ...repository, run_id: scenario.context.runId } },
        ...(scenario.expectBaselineRead ? [{ method: 'getWorkflowRun', params: { ...repository, run_id: Number(scenario.baselineRunId) } }] : []),
        ...(scenario.expectJobs ? [{ method: 'paginateJobs', params: { ...repository, run_id: Number(scenario.baselineRunId), filter: 'latest', per_page: 100 } }] : []),
    ];
    const sortRequests = requests => requests.slice().sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
    assert.deepEqual(sortRequests(scenario.requests.slice(requestsBefore)), sortRequests(expected), 'Unexpected GitHub request');
    assert.deepEqual(scenario.filesystemCalls.slice(filesystemBefore), which === 'baseline' ? [] : [
        { method: 'require', args: ['fs'] },
        { method: 'readFileSync', args: [statePath, 'utf8'] },
        { method: 'mkdirSync', args: [path.dirname(statePath), { recursive: true }] },
        { method: 'writeFileSync', args: [statePath, scenario.writtenText] },
    ], 'Unexpected filesystem operation');
    assertMessages(scenario.warnings.slice(warningsBefore), warnings, 'warnings');
    assertMessages(scenario.notices.slice(noticesBefore), notices, 'notices');
    if (scriptError) throw scriptError;
    return outputs;
}

async function findBaseline(scenario, messages) {
    const outputs = await execute('baseline', scenario, messages);
    assert.deepEqual(Object.keys(outputs), ['run_id']);
    assert.equal(typeof outputs.run_id, 'string');
    return outputs.run_id;
}
async function selectMatrix(scenario, messages) {
    const outputs = await execute('matrix', scenario, messages);
    assert.deepEqual(Object.keys(outputs), ['matrix']);
    assert.equal(typeof scenario.writtenText, 'string');
    assert.equal(scenario.summary.written, true, 'The script must await its summary write');
    assert.equal(scenario.summary.headings.at(-1), 'Weekly matrix');
    const table = scenario.summary.tables.at(-1).map(row => row.map(cell => typeof cell === 'object' ? cell.data : cell));
    assert.deepEqual(table[0], ['Row', 'Decision', 'Reason', 'Connector SHA', 'Last green run', 'Age']);
    assert.deepEqual(table.slice(1).map(row => row[0]), scenario.rows.map(rowLabel));
    const selected = JSON.parse(outputs.matrix);
    for (const [index, row] of scenario.rows.entries()) {
        assert.equal(table[index + 1][1], selected.some(candidate => key(candidate) === key({ ...row, sha: scenario.branchShas[row.branch] })) ? 'run' : 'skip');
    }
    return { matrix: selected, state: JSON.parse(scenario.writtenText), table: table.slice(1) };
}
function reason(result, row) { return result.table.find(cells => cells[0] === rowLabel(row))[2]; }

// This checks the connections between scripts and Actions, which mocked API tests cannot execute.
test('caller jobs, baseline download and artifact upload are wired to the actual script outputs', () => {
    assert.equal(value(download, 'name', 10), artifactName);
    assert.equal(path.join(value(download, 'path', 10), path.basename(statePath)), statePath);
    assert.equal(value(download, 'run-id', 10), '${{ steps.baseline_run.outputs.run_id }}');
    assert.equal(value(step('matrix'), 'BASELINE_RUN_ID', 10), '${{ steps.baseline_run.outputs.run_id }}');
    assert.equal(value(actionStep('actions/download-artifact'), 'if', 8), "steps.baseline_run.outputs.run_id != ''");
    assert.equal(value(section(section(workflow, 'prepare_matrix', 2), 'outputs', 4), 'matrix', 6), '${{ steps.matrix.outputs.matrix }}');
    for (const name of callers) {
        assert.equal(value(jobs[name], 'needs', 4), 'prepare_matrix');
        assert.equal(value(section(section(jobs[name], 'strategy', 4), 'matrix', 6), 'flink_branches', 8), '${{ fromJSON(needs.prepare_matrix.outputs.matrix) }}');
        assert.equal(value(section(jobs[name], 'with', 4), 'connector_branch', 6), '${{ matrix.flink_branches.sha || matrix.flink_branches.branch }}');
        // These guards are also valid JavaScript; GitHub-only functions such as contains() need a different evaluator.
        const condition = new Function('github', 'needs', `return (${value(jobs[name], 'if', 4)});`);
        for (const empty of ['', '[]']) assert.equal(condition({ repository_owner: 'apache' }, { prepare_matrix: { outputs: { matrix: empty } } }), false);
        assert.equal(condition({ repository_owner: 'apache' }, { prepare_matrix: { outputs: { matrix: '[{}]' } } }), true);
        assert.equal(condition({ repository_owner: 'fork' }, { prepare_matrix: { outputs: { matrix: '[{}]' } } }), false);
    }
});

test('a failed overall run can provide independently green released rows despite a changed head', async () => {
    const scenario = fixture();
    scenario.baselineRunId = await findBaseline(scenario);
    assert.equal(scenario.baselineRunId, String(scenario.baselineRun.id));
    scenario.context.sha = 'unrelated-new-head';
    scenario.branchShas[snapshotRows[0].branch] = 'new-main-tip';
    const result = await selectMatrix(scenario);
    assert.deepEqual(result.matrix, resolved(snapshotRows, scenario));
    assert.deepEqual(result.state, greenState(scenario));
    for (const row of releasedRows) {
        assert.equal(reason(result, row), 'Unchanged');
        const cells = result.table.find(cells => cells[0] === rowLabel(row));
        assert.ok(String(cells[4]).includes(String(scenario.baselineRun.id)));
        assert.ok(String(cells[5]).includes('7'));
    }
});

const invalidProvenance = [
    ['pull request event', { event: 'pull_request' }],
    ['wrong repository', { head_repository: { full_name: 'fork/flink-connector-kafka' } }],
    ['missing repository', { head_repository: null }],
    ['wrong branch', { head_branch: 'other' }],
    ['missing branch', { head_branch: undefined }],
    ['current run number', { run_number: 20 }],
    ['missing run number', { run_number: undefined }],
];
for (const [label, fields] of invalidProvenance) {
    test(`history selection and artifact revalidation reject ${label}`, async () => {
        const scenario = fixture();
        Object.assign(scenario.runs[1], fields);
        scenario.expectedArtifactRunId = null;
        assert.equal(await findBaseline(scenario, { notices: ['trusted weekly run'] }), '');
        Object.assign(scenario.baselineRun, fields);
        scenario.expectJobs = false;
        const result = await selectMatrix(scenario, { warnings: ['provenance'] });
        assert.deepEqual(result.matrix, resolved(configuredRows, scenario));
    });
}

test('history chooses the newest eligible run by number, including manual and incomplete runs', async () => {
    const scenario = fixture();
    scenario.runs[1].event = 'workflow_dispatch';
    scenario.runs[1].status = 'in_progress';
    scenario.runs[1].conclusion = null;
    scenario.runs.unshift({ ...scenario.runs[1], id: 999999, run_number: 18 });
    scenario.runs.push({ ...scenario.runs[1], id: 300021, run_number: 21 });
    scenario.runs.reverse();
    assert.equal(await findBaseline(scenario), String(scenario.baselineRun.id));
});

for (const artifacts of [[], [{ name: 'unrelated', expired: false }], [{ name: artifactName, expired: true }]]) {
    test(`an unavailable baseline artifact ${JSON.stringify(artifacts)} cannot fall back to an older run`, async () => {
        const scenario = fixture();
        scenario.artifacts = artifacts;
        scenario.runs.push({ ...scenario.runs[1], id: 100018, run_number: 18 });
        assert.equal(await findBaseline(scenario, { notices: ['artifact'] }), '');
    });
}

test('first run, unavailable history and unavailable artifact listing fail open', async () => {
    const scenario = fixture();
    scenario.runs = [];
    scenario.expectedArtifactRunId = null;
    assert.equal(await findBaseline(scenario, { notices: ['trusted weekly run'] }), '');
    scenario.historyError = new Error('History unavailable');
    assert.equal(await findBaseline(scenario, { warnings: ['History unavailable'] }), '');
    const artifactScenario = fixture();
    artifactScenario.artifactError = new Error('Artifacts unavailable');
    assert.equal(await findBaseline(artifactScenario, { warnings: ['Artifacts unavailable'] }), '');
});

test('a failing released job beyond the first page retries only its row; a failed snapshot does not affect others', async () => {
    const scenario = fixture();
    const failedRow = releasedRows[0];
    scenario.jobs.find(job => job.name.startsWith(callerName(callers[0], snapshotRows[0]))).conclusion = 'failure';
    scenario.jobs.push(...Array.from({ length: 100 }, (_, index) => ({ name: `unrelated ${index}`, conclusion: 'success' })));
    scenario.jobs.push({ name: `${callerName(callers[1], failedRow)} / late failure`, conclusion: 'failure' });
    const result = await selectMatrix(scenario);
    assert.deepEqual(result.matrix, selectedFor(scenario, row => row === failedRow));
    const expected = greenState(scenario);
    expected.rows[key(failedRow)] = { ...scenario.baselineState.rows[key(failedRow)], greenAt: null, greenRunId: null };
    assert.deepEqual(result.state, expected);
    assert.equal(reason(result, failedRow), 'No successful baseline');
});

for (const group of callers) {
    test(`both successful caller groups are required: missing ${group} retries the row`, async () => {
        const scenario = fixture();
        scenario.jobs = scenario.jobs.filter(job => !job.name.startsWith(callerName(group, releasedRows[0])));
        assert.deepEqual((await selectMatrix(scenario)).matrix, selectedFor(scenario, row => row === releasedRows[0]));
    });
}

test('pending or skipped jobs cannot bless a row, and a successful retry records its run ID', async () => {
    const scenario = fixture();
    const row = releasedRows[0];
    for (const conclusion of [null, 'skipped']) {
        scenario.jobs = successfulJobs(configuredRows);
        scenario.jobs.find(job => job.name.startsWith(callerName(callers[1], row))).conclusion = conclusion;
        assert.deepEqual((await selectMatrix(scenario)).matrix, selectedFor(scenario, candidate => candidate === row));
    }
    const retry = await selectMatrix(scenario);
    scenario.baselineState = retry.state;
    scenario.jobs = successfulJobs([...snapshotRows, row]);
    scenario.baselineRun.id++;
    scenario.baselineRunId = String(scenario.baselineRun.id);
    scenario.baselineRun.created_at = new Date(scenario.now).toISOString();
    scenario.now += 7 * day;
    const next = await selectMatrix(scenario);
    assert.deepEqual(next.matrix, resolved(snapshotRows, scenario));
    assert.equal(next.state.rows[key(row)].greenRunId, scenario.baselineRun.id);
});

test('an incomplete run carries earlier green rows but never blesses pending rows', async () => {
    const scenario = fixture();
    scenario.baselineState = greenState(scenario);
    const pending = releasedRows[0];
    scenario.baselineState.rows[key(pending)].greenAt = null;
    scenario.baselineState.rows[key(pending)].greenRunId = null;
    scenario.baselineRun.status = 'in_progress';
    scenario.expectJobs = false;
    const result = await selectMatrix(scenario, { notices: ['progress'] });
    assert.deepEqual(result.matrix, selectedFor(scenario, row => row === pending));
    assert.deepEqual(result.state, scenario.baselineState);
});

test('skipped weeks preserve green time and run ID; 27 days ensures the next weekly run rebuilds', async () => {
    const scenario = fixture();
    const initial = await selectMatrix(scenario);
    const greenAt = scenario.baselineRun.created_at;
    scenario.baselineState = initial.state;
    scenario.jobs = successfulJobs(snapshotRows);
    for (const age of [14, 21, 27 - 1 / day]) {
        scenario.now = Date.parse(greenAt) + age * day;
        scenario.baselineRun.created_at = new Date(scenario.now - 7 * day).toISOString();
        const result = await selectMatrix(scenario);
        assert.deepEqual(result.matrix, resolved(snapshotRows, scenario));
        assert.deepEqual(result.state, initial.state);
    }
    scenario.now = Date.parse(greenAt) + 27 * day;
    const expired = await selectMatrix(scenario);
    assert.deepEqual(expired.matrix, resolved(configuredRows, scenario));
    assert.ok(Object.values(expired.state.rows).every(row => row.greenAt === null && row.greenRunId === null));
    assert.ok(releasedRows.every(row => reason(expired, row) === 'Baseline expired'));
});

for (const greenAt of [null, 'invalid', '2026-10-01T00:00:00Z']) {
    test(`untrusted green timestamp ${JSON.stringify(greenAt)} requires rebuilding`, async () => {
        const scenario = fixture();
        scenario.baselineState = greenState(scenario);
        scenario.jobs = [];
        scenario.baselineState.rows[key(releasedRows[0])].greenAt = greenAt;
        assert.deepEqual((await selectMatrix(scenario)).matrix, selectedFor(scenario, row => row === releasedRows[0]));
    });
}

for (const branch of branches) {
    test(`a new commit on ${branch} only rebuilds that branch and snapshots`, async () => {
        const scenario = fixture();
        scenario.branchShas[branch] = 'new-sha';
        const result = await selectMatrix(scenario);
        assert.deepEqual(result.matrix, selectedFor(scenario, row => row.branch === branch));
        assert.ok(releasedRows.filter(row => row.branch === branch).every(row => reason(result, row) === 'Connector changed'));
        assert.ok(Object.keys(result.state.rows).every(row => !isSnapshot(JSON.parse(row))));
    });
}

for (const index of sharedUses.keys()) {
    test(`changed shared CI revision ${index} rebuilds every released row`, async () => {
        const scenario = fixture();
        scenario.currentRun.referenced_workflows[index].sha = 'new-ci-sha';
        const result = await selectMatrix(scenario);
        assert.deepEqual(result.matrix, resolved(configuredRows, scenario));
        assert.ok(Object.values(result.state.rows).every(row => row.ci === ciRevision(scenario) && row.greenAt === null));
        assert.ok(releasedRows.every(row => reason(result, row) === 'Shared CI changed'));
    });
}

test('reference ordering and ref names do not alter identity, but additional referenced workflows do', async () => {
    const scenario = fixture();
    scenario.currentRun.referenced_workflows.reverse();
    scenario.currentRun.referenced_workflows.forEach(reference => { reference.path = reference.path.replace(/@.*$/, '@other'); });
    assert.deepEqual((await selectMatrix(scenario)).matrix, resolved(snapshotRows, scenario));
    scenario.currentRun.referenced_workflows.push({ path: 'other/repo/.github/workflows/ci.yml@main', sha: 'extra-sha' });
    const changed = await selectMatrix(scenario);
    assert.deepEqual(changed.matrix, resolved(configuredRows, scenario));
    assert.ok(Object.values(changed.state.rows).every(row => row.ci === ciRevision(scenario)));
});

for (const missing of [0, 1]) {
    test(`missing referenced ${callers[missing]} workflow disables caching with a visible diagnostic`, async () => {
        const scenario = fixture();
        scenario.currentRun.referenced_workflows.splice(missing, 1);
        const result = await selectMatrix(scenario, { warnings: ['referenced CI'] });
        assert.deepEqual(result.matrix, resolved(configuredRows, scenario));
        assert.deepEqual(result.state, { rows: {} });
        assert.ok(scenario.summary.raw.join('').includes('Cache disabled:'));
        assert.ok(releasedRows.every(row => reason(result, row) === 'CI identity unavailable'));
    });
}

for (const failure of ['missing SHA', 'currentRunError', 'workflowError']) {
    test(`unresolved identity (${failure}) cannot persist reusable state`, async () => {
        const scenario = fixture();
        if (failure === 'missing SHA') delete scenario.currentRun.referenced_workflows[0].sha;
        else scenario[failure] = new Error('Identity unavailable');
        const result = await selectMatrix(scenario, { warnings: [failure === 'workflowError' ? 'weekly workflow' : 'referenced CI'] });
        assert.deepEqual(result.matrix, resolved(configuredRows, scenario));
        assert.deepEqual(result.state, { rows: {} });
        assert.ok(scenario.summary.raw.join('').includes('Cache disabled:'));
    });
}

for (const file of [{ type: 'dir', sha: 'directory' }, { type: 'file', sha: '' }, { type: 'file', sha: 42 }]) {
    test(`invalid workflow identity ${JSON.stringify(file)} fails open`, async () => {
        const scenario = fixture();
        scenario.workflowFile = file;
        const result = await selectMatrix(scenario, { warnings: ['weekly workflow'] });
        assert.deepEqual(result.matrix, resolved(configuredRows, scenario));
        assert.deepEqual(result.state, { rows: {} });
    });
}

test('changed workflow content and legacy rows without its identity require a fresh green build', async () => {
    const scenario = fixture();
    scenario.workflowFile.sha = 'new-workflow';
    const changed = await selectMatrix(scenario);
    assert.deepEqual(changed.matrix, resolved(configuredRows, scenario));
    assert.ok(releasedRows.every(row => reason(changed, row) === 'Workflow changed'));
    scenario.baselineState = changed.state;
    Object.values(scenario.baselineState.rows).forEach(row => { delete row.workflow; });
    const migrated = await selectMatrix(scenario);
    assert.deepEqual(migrated.matrix, resolved(configuredRows, scenario));
    assert.ok(Object.values(migrated.state.rows).every(row => row.workflow === scenario.workflowFile.sha && row.greenAt === null));
    scenario.baselineState = migrated.state;
    assert.deepEqual((await selectMatrix(scenario)).matrix, resolved(snapshotRows, scenario));
});

test('manual dispatch runs all rows, and its successful jobs bless the next scheduled run', async () => {
    const scenario = fixture();
    scenario.context.eventName = 'workflow_dispatch';
    const manual = await selectMatrix(scenario);
    assert.deepEqual(manual.matrix, resolved(configuredRows, scenario));
    assert.ok(releasedRows.every(row => reason(manual, row) === 'Manual dispatch'));
    assert.ok(Object.values(manual.state.rows).every(row => row.greenAt === null && row.greenRunId === null));
    scenario.baselineState = manual.state;
    scenario.baselineRun.event = 'workflow_dispatch';
    scenario.context.eventName = 'schedule';
    assert.deepEqual((await selectMatrix(scenario)).matrix, resolved(snapshotRows, scenario));
});

for (const field of ['flink', 'jdk']) {
    test(`changing ${field} creates a new row identity`, async () => {
        const scenario = fixture();
        const original = releasedRows[0];
        const replacement = { ...original, [field]: field === 'flink' ? `${original.flink}-changed` : original.jdk === '21' ? '17' : '21' };
        scenario.rows = configuredRows.map(row => row === original ? replacement : row);
        const result = await selectMatrix(scenario);
        assert.deepEqual(result.matrix, selectedFor(scenario, row => row === replacement));
        assert.equal(result.state.rows[key(replacement)].greenAt, null);
        assert.ok(!Object.hasOwn(result.state.rows, key(original)));
    });
}

test('caller matching includes the JDK and full branch name', async () => {
    const scenario = fixture();
    scenario.baselineState = greenState(scenario);
    const row = releasedRows[0];
    const otherJdk = row.jdk === '21' ? '17' : '21';
    scenario.jobs = [
        { name: `${callerName(callers[0], { ...row, jdk: otherJdk })} / test`, conclusion: 'failure' },
        { name: `${callerName(callers[1], { ...row, branch: `${row.branch}-other` })} / test`, conclusion: 'failure' },
    ];
    assert.deepEqual((await selectMatrix(scenario)).matrix, resolved(snapshotRows, scenario));
});

for (const state of [undefined, { matrix: [] }, { rows: [] }]) {
    test(`missing or incompatible artifact ${JSON.stringify(state)} runs everything`, async () => {
        const scenario = fixture();
        scenario.baselineState = state;
        scenario.expectBaselineRead = false;
        scenario.expectJobs = false;
        assert.deepEqual((await selectMatrix(scenario)).matrix, resolved(configuredRows, scenario));
    });
}

test('unreadable or malformed artifacts and a missing baseline run cannot be trusted', async () => {
    for (const setup of [scenario => { scenario.artifactText = '{invalid JSON'; },
        scenario => { scenario.readError = new Error('Unreadable artifact'); },
        scenario => { scenario.baselineRunId = ''; }]) {
        const scenario = fixture();
        setup(scenario);
        scenario.expectBaselineRead = false;
        scenario.expectJobs = false;
        assert.deepEqual((await selectMatrix(scenario)).matrix, resolved(configuredRows, scenario));
    }
});

for (const failure of ['baselineRunError', 'jobsError']) {
    test(`${failure} invalidates cached rows instead of silently trusting them`, async () => {
        const scenario = fixture();
        scenario.baselineState = greenState(scenario);
        scenario[failure] = new Error('Baseline results unavailable');
        if (failure === 'baselineRunError') scenario.expectJobs = false;
        const result = await selectMatrix(scenario, { warnings: ['Baseline results unavailable'] });
        assert.deepEqual(result.matrix, resolved(configuredRows, scenario));
    });
}

test('a failed branch lookup only reruns affected rows and does not cache them', async () => {
    const scenario = fixture();
    const branch = releasedRows[0].branch;
    delete scenario.branchShas[branch];
    const result = await selectMatrix(scenario, { warnings: [branch] });
    assert.deepEqual(result.matrix, selectedFor(scenario, row => row.branch === branch));
    assert.ok(releasedRows.filter(row => row.branch === branch).every(row =>
        !Object.hasOwn(result.state.rows, key(row)) && reason(result, row) === 'Connector revision unavailable'));
});

test('summary publication failure preserves the selected matrix and baseline', async () => {
    const scenario = fixture();
    scenario.summaryError = new Error('Summary unavailable');
    const result = await selectMatrix(scenario, { warnings: ['Cannot write weekly matrix summary'] });
    assert.deepEqual(result.matrix, resolved(snapshotRows, scenario));
    assert.deepEqual(result.state, greenState(scenario));
});
