import cron from 'node-cron';
import { pgConfig } from '../config/postgres.js';
import { logger } from '../utils/logger.js';

// ─── pure helpers (exported for tests) ──────────────────────────────────────

/**
 * Returns true if the string is a valid IANA timezone identifier.
 * @param {string} timezone
 * @returns {boolean}
 */
export function isValidIanaTimezone(timezone) {
    if (!timezone || typeof timezone !== 'string') return false;
    try {
        Intl.DateTimeFormat('en-US', { timeZone: timezone }).format(new Date());
        return true;
    } catch {
        return false;
    }
}

/**
 * Coerces a raw DB row into a normalized schedule object, applying defaults.
 * Throws if the row is fundamentally invalid (bad mode, invalid cron).
 *
 * @param {object} row - raw row from scheduled_messages table
 * @param {{ defaultTimezone: string }} defaults
 * @returns {object} normalized row
 */
export function normalizeScheduleRow(row, defaults) {
    const timezone = row.timezone && isValidIanaTimezone(row.timezone)
        ? row.timezone
        : defaults.defaultTimezone;

    const enabled = row.enabled === true || row.enabled === 'true' || row.enabled === 1;

    if (row.schedule_mode !== 'cron') {
        throw new Error(`Unsupported schedule_mode "${row.schedule_mode}" for schedule id=${row.id}`);
    }

    if (!cron.validate(row.schedule_value)) {
        throw new Error(`Invalid cron expression "${row.schedule_value}" for schedule id=${row.id}`);
    }

    return { ...row, timezone, enabled };
}

/**
 * Builds the Discord send payload from a schedule row and an optional pool item.
 *
 * @param {object} row - normalized schedule row
 * @param {object|null} poolItem - selected pool item (null for single mode)
 * @returns {{ content: string|null, embeds: object[], allowedMentions: object }}
 */
export function buildDiscordPayloadFromSchedule(row, poolItem = null) {
    const source = poolItem || row;

    const content = source.text_content || null;

    const embeds = [];
    const embedData = source.embed_payload || source.embed || null;
    if (embedData && typeof embedData === 'object') {
        embeds.push(embedData);
    }

    // Safe allowedMentions — never inferred from message text
    const allowedMentions = { parse: [] };

    if (row.mention_everyone || row.mention_here) {
        allowedMentions.parse.push('everyone');
    }

    const roleIds = Array.isArray(row.mention_role_ids)
        ? row.mention_role_ids.filter(Boolean)
        : [];
    if (roleIds.length > 0) {
        allowedMentions.roles = roleIds;
    }

    return { content, embeds, allowedMentions };
}

/**
 * Picks a pool item using the given strategy.
 * Updates and returns the new pool_state.
 *
 * @param {object[]} poolPayload - array of pool items
 * @param {'random'|'shuffle'} strategy
 * @param {object|null} currentState - current pool_state from DB
 * @param {number|null} lastIndex - last_sent_item_index from DB
 * @returns {{ item: object, newState: object, newIndex: number }}
 */
export function pickPoolItem(poolPayload, strategy, currentState, lastIndex) {
    if (!Array.isArray(poolPayload) || poolPayload.length === 0) {
        throw new Error('Pool is empty — cannot pick an item');
    }

    if (strategy === 'shuffle') {
        let state = currentState && Array.isArray(currentState.order) && currentState.order.length === poolPayload.length
            ? { ...currentState }
            : null;

        if (!state) {
            // Build a fresh Fisher-Yates shuffle order
            const order = Array.from({ length: poolPayload.length }, (_, i) => i);
            for (let i = order.length - 1; i > 0; i--) {
                const j = Math.floor(Math.random() * (i + 1));
                [order[i], order[j]] = [order[j], order[i]];
            }
            state = { order, cursor: 0 };
        }

        const cursor = state.cursor ?? 0;
        const pickedIndex = state.order[cursor % state.order.length];

        const nextCursor = (cursor + 1) % poolPayload.length;
        // Reset shuffle order at cycle boundary
        if (nextCursor === 0) {
            const order = Array.from({ length: poolPayload.length }, (_, i) => i);
            for (let i = order.length - 1; i > 0; i--) {
                const j = Math.floor(Math.random() * (i + 1));
                [order[i], order[j]] = [order[j], order[i]];
            }
            return { item: poolPayload[pickedIndex], newState: { order, cursor: 0 }, newIndex: pickedIndex };
        }

        return { item: poolPayload[pickedIndex], newState: { order: state.order, cursor: nextCursor }, newIndex: pickedIndex };
    }

    // Default: random (weighted if items have `weight`)
    const hasWeights = poolPayload.some(item => typeof item.weight === 'number' && item.weight > 0);

    let pickedIndex;
    if (hasWeights) {
        const weights = poolPayload.map(item => (typeof item.weight === 'number' && item.weight > 0 ? item.weight : 1));
        const total = weights.reduce((sum, w) => sum + w, 0);
        let roll = Math.random() * total;
        pickedIndex = 0;
        for (let i = 0; i < weights.length; i++) {
            roll -= weights[i];
            if (roll < 0) { pickedIndex = i; break; }
        }
    } else {
        pickedIndex = Math.floor(Math.random() * poolPayload.length);
        // Anti-repeat: if only one option would repeat, try to avoid it
        if (poolPayload.length > 1 && pickedIndex === lastIndex) {
            pickedIndex = (pickedIndex + 1) % poolPayload.length;
        }
    }

    return { item: poolPayload[pickedIndex], newState: currentState ?? null, newIndex: pickedIndex };
}

// ─── DB access ──────────────────────────────────────────────────────────────

function getPool(client) {
    return client?.db?.db?.pool ?? null;
}

/**
 * Returns all enabled schedule rows across all guilds.
 * @param {object} client
 * @returns {Promise<object[]>}
 */
export async function listEnabledSchedules(client) {
    const pool = getPool(client);
    if (!pool) return [];
    const result = await pool.query(
        `SELECT * FROM ${pgConfig.tables.scheduled_messages} WHERE enabled = true ORDER BY id ASC`
    );
    return result.rows;
}

/**
 * Returns all schedules for a given guild (enabled and disabled).
 * @param {object} client
 * @param {string} guildId
 * @returns {Promise<object[]>}
 */
export async function listSchedulesForGuild(client, guildId) {
    const pool = getPool(client);
    if (!pool) return [];
    const result = await pool.query(
        `SELECT * FROM ${pgConfig.tables.scheduled_messages} WHERE guild_id = $1 ORDER BY name ASC`,
        [guildId]
    );
    return result.rows;
}

/**
 * Returns a single schedule row by guild + name. Null if not found.
 * @param {object} client
 * @param {string} guildId
 * @param {string} name
 * @returns {Promise<object|null>}
 */
export async function getScheduleByName(client, guildId, name) {
    const pool = getPool(client);
    if (!pool) return null;
    const result = await pool.query(
        `SELECT * FROM ${pgConfig.tables.scheduled_messages} WHERE guild_id = $1 AND name = $2`,
        [guildId, name]
    );
    return result.rows[0] ?? null;
}

/**
 * Upsert a schedule row. Uses name+guild_id as the conflict key.
 * @param {object} client
 * @param {object} fields
 * @returns {Promise<object|null>} the upserted row
 */
export async function upsertSchedule(client, fields) {
    const pool = getPool(client);
    if (!pool) return null;

    const {
        guild_id,
        name,
        channel_id,
        message_mode = 'single',
        schedule_mode = 'cron',
        schedule_value,
        timezone = 'Africa/Casablanca',
        text_content = null,
        embed_payload = null,
        pool_payload = [],
        random_strategy = 'random',
        enabled = true,
        mention_everyone = false,
        mention_here = false,
        mention_role_ids = [],
        created_by = null,
        updated_by = null,
    } = fields;

    const result = await pool.query(
        `INSERT INTO ${pgConfig.tables.scheduled_messages}
            (guild_id, name, channel_id, message_mode, schedule_mode, schedule_value, timezone,
             text_content, embed_payload, pool_payload, random_strategy, enabled,
             mention_everyone, mention_here, mention_role_ids, created_by, updated_by)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
         ON CONFLICT (guild_id, name) DO UPDATE SET
            channel_id        = EXCLUDED.channel_id,
            message_mode      = EXCLUDED.message_mode,
            schedule_mode     = EXCLUDED.schedule_mode,
            schedule_value    = EXCLUDED.schedule_value,
            timezone          = EXCLUDED.timezone,
            text_content      = EXCLUDED.text_content,
            embed_payload     = EXCLUDED.embed_payload,
            pool_payload      = EXCLUDED.pool_payload,
            random_strategy   = EXCLUDED.random_strategy,
            enabled           = EXCLUDED.enabled,
            mention_everyone  = EXCLUDED.mention_everyone,
            mention_here      = EXCLUDED.mention_here,
            mention_role_ids  = EXCLUDED.mention_role_ids,
            updated_by        = EXCLUDED.updated_by,
            updated_at        = CURRENT_TIMESTAMP
         RETURNING *`,
        [
            guild_id, name, channel_id, message_mode, schedule_mode, schedule_value, timezone,
            text_content,
            embed_payload ? JSON.stringify(embed_payload) : null,
            JSON.stringify(pool_payload),
            random_strategy, enabled,
            mention_everyone, mention_here,
            mention_role_ids,
            created_by, updated_by,
        ]
    );

    return result.rows[0] ?? null;
}

/**
 * Deletes a schedule by guild + name.
 * @param {object} client
 * @param {string} guildId
 * @param {string} name
 * @returns {Promise<boolean>}
 */
export async function deleteSchedule(client, guildId, name) {
    const pool = getPool(client);
    if (!pool) return false;
    const result = await pool.query(
        `DELETE FROM ${pgConfig.tables.scheduled_messages} WHERE guild_id = $1 AND name = $2`,
        [guildId, name]
    );
    return (result.rowCount ?? 0) > 0;
}

/**
 * Patches runtime state columns after a fire: last_run_at, pool_state, last_sent_item_index.
 * @param {object} client
 * @param {number} id - schedule row id
 * @param {{ last_run_at?: Date, pool_state?: object|null, last_sent_item_index?: number|null, updated_by?: string|null }} patch
 */
export async function updateScheduleRuntimeState(client, id, patch) {
    const pool = getPool(client);
    if (!pool) return;

    const sets = [];
    const values = [];
    let idx = 1;

    if (patch.last_run_at !== undefined) {
        sets.push(`last_run_at = $${idx++}`);
        values.push(patch.last_run_at ?? new Date());
    }
    if (patch.pool_state !== undefined) {
        sets.push(`pool_state = $${idx++}`);
        values.push(patch.pool_state ? JSON.stringify(patch.pool_state) : null);
    }
    if (patch.last_sent_item_index !== undefined) {
        sets.push(`last_sent_item_index = $${idx++}`);
        values.push(patch.last_sent_item_index ?? null);
    }
    if (patch.updated_by !== undefined) {
        sets.push(`updated_by = $${idx++}`);
        values.push(patch.updated_by ?? null);
    }

    if (sets.length === 0) return;

    values.push(id);
    await pool.query(
        `UPDATE ${pgConfig.tables.scheduled_messages} SET ${sets.join(', ')} WHERE id = $${idx}`,
        values
    );
}

/**
 * Patches enabled state of a schedule.
 * @param {object} client
 * @param {string} guildId
 * @param {string} name
 * @param {boolean} enabled
 * @param {string|null} updatedBy
 * @returns {Promise<boolean>}
 */
export async function setScheduleEnabled(client, guildId, name, enabled, updatedBy = null) {
    const pool = getPool(client);
    if (!pool) return false;
    const result = await pool.query(
        `UPDATE ${pgConfig.tables.scheduled_messages}
         SET enabled = $1, updated_by = $2, updated_at = CURRENT_TIMESTAMP
         WHERE guild_id = $3 AND name = $4`,
        [enabled, updatedBy, guildId, name]
    );
    return (result.rowCount ?? 0) > 0;
}

/**
 * Appends one item to the pool_payload JSON array of a schedule.
 * @param {object} client
 * @param {string} guildId
 * @param {string} name
 * @param {object} item
 * @param {string|null} updatedBy
 * @returns {Promise<object[]|null>} the new pool_payload, or null on failure
 */
export async function appendPoolItem(client, guildId, name, item, updatedBy = null) {
    const pool = getPool(client);
    if (!pool) return null;
    const result = await pool.query(
        `UPDATE ${pgConfig.tables.scheduled_messages}
         SET pool_payload = pool_payload || $1::jsonb,
             updated_by = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE guild_id = $3 AND name = $4
         RETURNING pool_payload`,
        [JSON.stringify([item]), updatedBy, guildId, name]
    );
    return result.rows[0]?.pool_payload ?? null;
}

/**
 * Removes a pool item by index.
 * @param {object} client
 * @param {string} guildId
 * @param {string} name
 * @param {number} index - 0-based
 * @param {string|null} updatedBy
 * @returns {Promise<object[]|null>} the new pool_payload
 */
export async function removePoolItem(client, guildId, name, index, updatedBy = null) {
    const pool = getPool(client);
    if (!pool) return null;

    // Fetch current array, splice in JS, write back
    const row = await getScheduleByName(client, guildId, name);
    if (!row) return null;

    const arr = Array.isArray(row.pool_payload) ? [...row.pool_payload] : [];
    if (index < 0 || index >= arr.length) return null;
    arr.splice(index, 1);

    const result = await pool.query(
        `UPDATE ${pgConfig.tables.scheduled_messages}
         SET pool_payload = $1::jsonb,
             updated_by = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE guild_id = $3 AND name = $4
         RETURNING pool_payload`,
        [JSON.stringify(arr), updatedBy, guildId, name]
    );
    return result.rows[0]?.pool_payload ?? null;
}

// ─── advisory lock ───────────────────────────────────────────────────────────

const ADVISORY_LOCK_KEY = 2147483001;

/**
 * Tries to acquire the Postgres advisory lock.
 * Returns true if this instance is the scheduler leader.
 * @param {object} pool - pg.Pool
 * @returns {Promise<boolean>}
 */
async function tryAcquireAdvisoryLock(pool) {
    try {
        const result = await pool.query('SELECT pg_try_advisory_lock($1) AS acquired', [ADVISORY_LOCK_KEY]);
        return result.rows[0]?.acquired === true;
    } catch (error) {
        logger.warn('scheduler: advisory lock check failed', { error: error.message });
        return false;
    }
}

// ─── scheduler runtime ───────────────────────────────────────────────────────

/**
 * Fires a single schedule: picks content, sends to channel, updates state.
 * @param {object} client
 * @param {object} row - normalized schedule row
 */
async function fireSchedule(client, row) {
    try {
        const guild = client.guilds.cache.get(row.guild_id)
            ?? await client.guilds.fetch(row.guild_id).catch(() => null);

        if (!guild) {
            logger.warn('scheduler: guild not found for schedule', { scheduleId: row.id, guildId: row.guild_id });
            return;
        }

        const channel = guild.channels.cache.get(row.channel_id)
            ?? await guild.channels.fetch(row.channel_id).catch(() => null);

        if (!channel || !channel.isTextBased()) {
            logger.warn('scheduler: channel not found or not text-based', {
                scheduleId: row.id, channelId: row.channel_id, guildId: row.guild_id
            });
            return;
        }

        let payload;
        let poolState = row.pool_state;
        let lastIndex = row.last_sent_item_index ?? null;

        if (row.message_mode === 'pool') {
            const poolPayload = Array.isArray(row.pool_payload) ? row.pool_payload : [];
            if (poolPayload.length === 0) {
                logger.warn('scheduler: pool is empty, skipping fire', { scheduleId: row.id, name: row.name });
                return;
            }
            const { item, newState, newIndex } = pickPoolItem(
                poolPayload,
                row.random_strategy ?? 'random',
                poolState,
                lastIndex
            );
            payload = buildDiscordPayloadFromSchedule(row, item);
            poolState = newState;
            lastIndex = newIndex;
        } else {
            payload = buildDiscordPayloadFromSchedule(row, null);
        }

        if (!payload.content && payload.embeds.length === 0) {
            logger.warn('scheduler: no content to send for schedule', { scheduleId: row.id, name: row.name });
            return;
        }

        await channel.send({
            content: payload.content ?? undefined,
            embeds: payload.embeds.length > 0 ? payload.embeds : undefined,
            allowedMentions: payload.allowedMentions,
        });

        await updateScheduleRuntimeState(client, row.id, {
            last_run_at: new Date(),
            pool_state: poolState,
            last_sent_item_index: lastIndex,
        });

        logger.info('scheduler: fired schedule', {
            scheduleId: row.id, name: row.name, guildId: row.guild_id, channelId: row.channel_id
        });
    } catch (error) {
        logger.error('scheduler: error firing schedule', {
            error: error.message,
            scheduleId: row.id,
            name: row.name,
            guildId: row.guild_id,
            channelId: row.channel_id,
        });
    }
}

/**
 * Starts the scheduled messages runtime.
 * Returns a handle with a `.stop()` method for clean shutdown.
 *
 * @param {object} client - TitanBot client
 * @returns {{ stop: () => void }}
 */
export function startScheduledMessages(client) {
    const dbAvailable = client?.db?.isAvailable?.() ?? false;
    const featureEnabled = client?.config?.features?.scheduler ?? client?.config?.bot?.features?.scheduler ?? true;

    if (!dbAvailable) {
        logger.warn('scheduler: PostgreSQL unavailable — scheduler disabled');
        return { stop: () => {} };
    }

    if (!featureEnabled) {
        logger.info('scheduler: feature flag disabled — scheduler not started');
        return { stop: () => {} };
    }

    const pool = getPool(client);
    if (!pool) {
        logger.warn('scheduler: no pg pool — scheduler disabled');
        return { stop: () => {} };
    }

    const defaultTimezone = client?.config?.bot?.scheduler?.defaultTimezone ?? 'Africa/Casablanca';
    const refreshIntervalSeconds = client?.config?.bot?.scheduler?.refreshIntervalSeconds ?? 60;
    const maxPoolItems = client?.config?.bot?.scheduler?.maxPoolItems ?? 500;

    // Map: schedule id => { task: CronTask, updatedAt: string, enabled: boolean }
    const activeTasks = new Map();
    let refreshIntervalId = null;
    let isLeader = false;

    function stopTask(id) {
        const entry = activeTasks.get(id);
        if (entry) {
            try { entry.task.stop(); } catch {}
            activeTasks.delete(id);
        }
    }

    function scheduleRow(row) {
        // Stop existing task if already tracked
        stopTask(row.id);

        if (!row.enabled) return;

        let normalized;
        try {
            normalized = normalizeScheduleRow(row, { defaultTimezone });
        } catch (err) {
            logger.warn('scheduler: skipping invalid row', { scheduleId: row.id, error: err.message });
            return;
        }

        const task = cron.schedule(
            normalized.schedule_value,
            () => fireSchedule(client, normalized),
            { timezone: normalized.timezone }
        );

        activeTasks.set(row.id, {
            task,
            updatedAt: String(row.updated_at),
            enabled: row.enabled,
        });
    }

    async function loadAndDiff() {
        try {
            const rows = await listEnabledSchedules(client);

            // Remove tasks no longer in DB or now disabled
            const activeIds = new Set(rows.map(r => r.id));
            for (const [id] of activeTasks) {
                if (!activeIds.has(id)) stopTask(id);
            }

            // Add or refresh changed rows
            for (const row of rows) {
                const existing = activeTasks.get(row.id);
                const hasChanged = !existing
                    || String(row.updated_at) !== existing.updatedAt
                    || row.enabled !== existing.enabled;

                if (hasChanged) scheduleRow(row);
            }
        } catch (error) {
            logger.error('scheduler: error during schedule refresh', { error: error.message });
        }
    }

    async function init() {
        isLeader = await tryAcquireAdvisoryLock(pool);

        if (!isLeader) {
            logger.info('scheduler: advisory lock not acquired — standby mode (another instance is leader)');
            // Re-check periodically so we can take over after a restart
            refreshIntervalId = setInterval(async () => {
                isLeader = await tryAcquireAdvisoryLock(pool);
                if (isLeader) {
                    logger.info('scheduler: acquired advisory lock — becoming leader');
                    clearInterval(refreshIntervalId);
                    await loadAndDiff();
                    refreshIntervalId = setInterval(loadAndDiff, refreshIntervalSeconds * 1000);
                }
            }, 15000);
            return;
        }

        logger.info('scheduler: advisory lock acquired — starting as leader');
        await loadAndDiff();
        refreshIntervalId = setInterval(loadAndDiff, refreshIntervalSeconds * 1000);
    }

    init().catch(err => {
        logger.error('scheduler: init error', { error: err.message });
    });

    return {
        stop() {
            if (refreshIntervalId) {
                clearInterval(refreshIntervalId);
                refreshIntervalId = null;
            }
            for (const [id] of activeTasks) stopTask(id);
            logger.info('scheduler: stopped all tasks');
        }
    };
}
