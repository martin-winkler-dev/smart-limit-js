// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'bun:test';
import { createLogger } from '../../src/logger.js';
import type { LogEntry } from '../../src/types.js';

function recordingLogger() {
    const entries: LogEntry[] = [];
    const logger = createLogger((entry) => {
        entries.push(entry);
    });
    return { entries, logger };
}

describe('createLogger', () => {
    it('does nothing without a log function', () => {
        const logger = createLogger(undefined);
        expect(() => {
            logger.log('a', 1);
            logger.warn('b');
            logger.error('FATAL', 'c', { x: 1 });
        }).not.toThrow();
    });

    it('does nothing when log is not a function', () => {
        const logger = createLogger('console' as never);
        expect(() => {
            logger.log('a');
            logger.warn('b');
            logger.error('TIMEOUT', 'c');
        }).not.toThrow();
    });

    it('writes log entries with only level and message when no data is given', () => {
        const { entries, logger } = recordingLogger();
        logger.log('hello');
        expect(entries).toEqual([{ level: 'log', message: 'hello' }]);
        expect(Object.keys(entries[0]!)).toEqual(['level', 'message']);
    });

    it('adds data to log and warn entries', () => {
        const { entries, logger } = recordingLogger();
        logger.log('hello', { a: 1 });
        logger.warn('careful', 0);
        expect(entries).toEqual([
            { level: 'log', message: 'hello', data: { a: 1 } },
            { level: 'warn', message: 'careful', data: 0 },
        ]);
        expect(Object.keys(entries[1]!)).toEqual(['level', 'message', 'data']);
    });

    it('writes warn entries without code', () => {
        const { entries, logger } = recordingLogger();
        logger.warn('careful');
        expect(entries).toEqual([{ level: 'warn', message: 'careful' }]);
        expect(Object.keys(entries[0]!)).toEqual(['level', 'message']);
    });

    it('adds the code to error entries', () => {
        const { entries, logger } = recordingLogger();
        logger.error('HOOK_FAILED', 'hook threw');
        logger.error('BUCKET_FAILED', 'bucket threw', { index: 2 });
        expect(entries).toEqual([
            { level: 'error', message: 'hook threw', code: 'HOOK_FAILED' },
            { level: 'error', message: 'bucket threw', code: 'BUCKET_FAILED', data: { index: 2 } },
        ]);
        expect(Object.keys(entries[0]!)).toEqual(['level', 'message', 'code']);
        expect(Object.keys(entries[1]!)).toEqual(['level', 'message', 'code', 'data']);
    });

    it('keeps null data', () => {
        const { entries, logger } = recordingLogger();
        logger.log('x', null);
        expect(entries).toEqual([{ level: 'log', message: 'x', data: null }]);
    });

    it('swallows errors thrown by the log function', () => {
        let calls = 0;
        const logger = createLogger(() => {
            calls++;
            throw new Error('log broke');
        });
        expect(() => {
            logger.log('a');
            logger.warn('b');
            logger.error('FATAL', 'c');
        }).not.toThrow();
        expect(calls).toBe(3);
    });
});
