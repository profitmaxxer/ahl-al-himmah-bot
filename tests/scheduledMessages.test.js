import { test, describe } from 'node:test';
import assert from 'node:assert/strict';

import {
    isValidIanaTimezone,
    normalizeScheduleRow,
    buildDiscordPayloadFromSchedule,
    pickPoolItem,
} from '../../src/services/scheduledMessagesService.js';

// ─── isValidIanaTimezone ─────────────────────────────────────────────────────

describe('isValidIanaTimezone', () => {
    test('returns true for Africa/Casablanca', () => {
        assert.equal(isValidIanaTimezone('Africa/Casablanca'), true);
    });

    test('returns true for UTC', () => {
        assert.equal(isValidIanaTimezone('UTC'), true);
    });

    test('returns true for America/New_York', () => {
        assert.equal(isValidIanaTimezone('America/New_York'), true);
    });

    test('returns false for an invalid timezone', () => {
        assert.equal(isValidIanaTimezone('Not/ATimezone'), false);
    });

    test('returns false for empty string', () => {
        assert.equal(isValidIanaTimezone(''), false);
    });

    test('returns false for null', () => {
        assert.equal(isValidIanaTimezone(null), false);
    });
});

// ─── normalizeScheduleRow ────────────────────────────────────────────────────

describe('normalizeScheduleRow', () => {
    const defaults = { defaultTimezone: 'Africa/Casablanca' };

    test('passes through valid cron row', () => {
        const row = {
            id: 1,
            schedule_mode: 'cron',
            schedule_value: '0 9 * * 5',
            timezone: 'UTC',
            enabled: true,
        };
        const normalized = normalizeScheduleRow(row, defaults);
        assert.equal(normalized.timezone, 'UTC');
        assert.equal(normalized.enabled, true);
    });

    test('falls back to default timezone when invalid', () => {
        const row = {
            id: 2,
            schedule_mode: 'cron',
            schedule_value: '0 9 * * 5',
            timezone: 'Invalid/Zone',
            enabled: true,
        };
        const normalized = normalizeScheduleRow(row, defaults);
        assert.equal(normalized.timezone, 'Africa/Casablanca');
    });

    test('coerces enabled from string "true"', () => {
        const row = {
            id: 3,
            schedule_mode: 'cron',
            schedule_value: '* * * * *',
            timezone: 'UTC',
            enabled: 'true',
        };
        const normalized = normalizeScheduleRow(row, defaults);
        assert.equal(normalized.enabled, true);
    });

    test('throws on unsupported schedule_mode', () => {
        const row = {
            id: 4,
            schedule_mode: 'interval',
            schedule_value: '60',
            timezone: 'UTC',
            enabled: true,
        };
        assert.throws(() => normalizeScheduleRow(row, defaults), /Unsupported schedule_mode/);
    });

    test('throws on invalid cron expression', () => {
        const row = {
            id: 5,
            schedule_mode: 'cron',
            schedule_value: 'not-a-cron',
            timezone: 'UTC',
            enabled: true,
        };
        assert.throws(() => normalizeScheduleRow(row, defaults), /Invalid cron expression/);
    });
});

// ─── buildDiscordPayloadFromSchedule ─────────────────────────────────────────

describe('buildDiscordPayloadFromSchedule', () => {
    test('returns content from row for single mode', () => {
        const row = {
            text_content: 'hello',
            embed_payload: null,
            mention_everyone: false,
            mention_here: false,
            mention_role_ids: [],
        };
        const payload = buildDiscordPayloadFromSchedule(row, null);
        assert.equal(payload.content, 'hello');
        assert.deepEqual(payload.embeds, []);
        assert.deepEqual(payload.allowedMentions, { parse: [] });
    });

    test('returns embed from row', () => {
        const row = {
            text_content: null,
            embed_payload: { title: 'Test' },
            mention_everyone: false,
            mention_here: false,
            mention_role_ids: [],
        };
        const payload = buildDiscordPayloadFromSchedule(row, null);
        assert.equal(payload.content, null);
        assert.equal(payload.embeds.length, 1);
        assert.equal(payload.embeds[0].title, 'Test');
    });

    test('picks content from pool item over row', () => {
        const row = {
            text_content: 'row content',
            embed_payload: null,
            mention_everyone: false,
            mention_here: false,
            mention_role_ids: [],
        };
        const poolItem = { text_content: 'pool item content', embed_payload: null };
        const payload = buildDiscordPayloadFromSchedule(row, poolItem);
        assert.equal(payload.content, 'pool item content');
    });

    test('allowedMentions includes everyone when mention_everyone is true', () => {
        const row = {
            text_content: 'msg',
            embed_payload: null,
            mention_everyone: true,
            mention_here: false,
            mention_role_ids: [],
        };
        const payload = buildDiscordPayloadFromSchedule(row, null);
        assert.ok(payload.allowedMentions.parse.includes('everyone'));
    });

    test('allowedMentions includes role ids', () => {
        const row = {
            text_content: 'msg',
            embed_payload: null,
            mention_everyone: false,
            mention_here: false,
            mention_role_ids: ['111', '222'],
        };
        const payload = buildDiscordPayloadFromSchedule(row, null);
        assert.deepEqual(payload.allowedMentions.roles, ['111', '222']);
    });
});

// ─── pickPoolItem ─────────────────────────────────────────────────────────────

describe('pickPoolItem — random strategy', () => {
    const pool = [{ text_content: 'a' }, { text_content: 'b' }, { text_content: 'c' }];

    test('returns an item from the pool', () => {
        const { item } = pickPoolItem(pool, 'random', null, null);
        assert.ok(pool.includes(item));
    });

    test('anti-repeat: does not repeat last index when pool size > 1', () => {
        // Run many times to statistically verify anti-repeat
        const lastIndex = 0;
        let repeated = 0;
        for (let i = 0; i < 50; i++) {
            const { newIndex } = pickPoolItem(pool, 'random', null, lastIndex);
            if (newIndex === lastIndex) repeated++;
        }
        // Should not repeat 100% of the time with pool size 3
        assert.ok(repeated < 50, `Repeated last index too often: ${repeated}/50`);
    });

    test('throws on empty pool', () => {
        assert.throws(() => pickPoolItem([], 'random', null, null), /Pool is empty/);
    });

    test('weighted random: high-weight item is picked more often', () => {
        const weightedPool = [
            { text_content: 'rare', weight: 1 },
            { text_content: 'common', weight: 99 },
        ];
        let commonCount = 0;
        for (let i = 0; i < 100; i++) {
            const { item } = pickPoolItem(weightedPool, 'random', null, null);
            if (item.text_content === 'common') commonCount++;
        }
        // Common item has 99% weight — expect > 80 out of 100
        assert.ok(commonCount > 80, `Common item picked only ${commonCount}/100 times`);
    });
});

describe('pickPoolItem — shuffle strategy', () => {
    const pool = [{ text_content: 'x' }, { text_content: 'y' }, { text_content: 'z' }];

    test('cycles through all items before repeating', () => {
        const seen = new Set();
        let state = null;
        for (let i = 0; i < pool.length; i++) {
            const { item, newState } = pickPoolItem(pool, 'shuffle', state, null);
            seen.add(item.text_content);
            state = newState;
        }
        assert.equal(seen.size, pool.length);
    });

    test('resets and starts new cycle after completing one', () => {
        let state = null;
        for (let i = 0; i < pool.length; i++) {
            const { newState } = pickPoolItem(pool, 'shuffle', state, null);
            state = newState;
        }
        // After full cycle, cursor should be reset to 0
        assert.equal(state.cursor, 0);
    });
});
