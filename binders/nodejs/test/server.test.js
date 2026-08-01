'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { DEFAULT_BINARY, serverBinaryName } = require('../lib/server');

test('serverBinaryName uses the executable suffix Go can spawn on Windows', () => {
    assert.equal(serverBinaryName('win32'), 'cheetah-server.exe');
    assert.equal(serverBinaryName('linux'), 'cheetah-server');
    assert.equal(serverBinaryName('darwin'), 'cheetah-server');
});

test('DEFAULT_BINARY follows the current host platform', () => {
    assert.equal(path.basename(DEFAULT_BINARY), serverBinaryName(process.platform));
});
