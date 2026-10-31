// Copyright 2026 Martin Winkler
// SPDX-License-Identifier: Apache-2.0

import type { LimiterErrorCode, LogEntry, LogLevel } from './types.js';
import { callSafely } from './util.js';

// -------------------------------------------------------------------
// --- 1. Types
// -------------------------------------------------------------------

/** Logger with structured levels and error codes. */
export type Logger = {
    log(message: string, data?: unknown): void;
    warn(message: string, data?: unknown): void;
    error(code: LimiterErrorCode, message: string, data?: unknown): void;
};

// -------------------------------------------------------------------
// --- 2. Exported Functions
// -------------------------------------------------------------------

/** Wraps user log callback into safe logger; swallows subscriber errors. */
export function createLogger(onLog: ((entry: LogEntry) => void) | undefined): Logger {
    function write(level: LogLevel, message: string, code: LimiterErrorCode | undefined, data: unknown): void {
        if (typeof onLog !== 'function') {
            return;
        }
        const entry: LogEntry = { level, message };
        if (code !== undefined) {
            entry.code = code;
        }
        if (data !== undefined) {
            entry.data = data;
        }
        callSafely(() => onLog(entry));
    }

    return {
        log: (message, data) => write('log', message, undefined, data),
        warn: (message, data) => write('warn', message, undefined, data),
        error: (code, message, data) => write('error', message, code, data),
    };
}
